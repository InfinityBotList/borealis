import discord
from discord.ext import commands, tasks
from pydantic import BaseModel
from ruamel.yaml import YAML
import logging
import asyncpg
import asyncio
from kittycat.perms import get_user_staff_perms
from kittycat.kittycat import has_perm
import secrets
import traceback
import sys
import os
import datetime
import io
import importlib
import uvicorn
import aiohttp

MAX_PER_CACHE_SERVER = 40

logging.basicConfig(level=logging.INFO)

class NeededBots(BaseModel):
    id: int
    name: str
    invite: str

class CacheServerMaker(BaseModel):
    client_id: int
    client_secret: str
    token: str

class Config(BaseModel):
    token: str
    postgres_url: str
    pinned_servers: list[int]
    needed_bots: list[NeededBots]
    notify_webhook: str
    base_url: str
    cache_server_maker: CacheServerMaker

yaml = YAML(typ="safe")
with open("config.yaml", "r") as f:
    config = Config(**yaml.load(f))

class BorealisBot(commands.AutoShardedBot):
    pool: asyncpg.pool.Pool

    def __init__(self, config: Config):
        super().__init__(command_prefix="#", intents=discord.Intents.all())
        self.config = config
        self.pool = None
        self.session = aiohttp.ClientSession()

    async def run(self):
        self.pool = await asyncpg.pool.create_pool(self.config.postgres_url)
        api = importlib.import_module("api")
        api.bot = bot
        api.config = config
        server = uvicorn.Server(config=uvicorn.Config(api.app, workers=3, loop=loop, port=2837))
        asyncio.create_task(server.serve())
        await super().start(self.config.token)

intents = discord.Intents.all()

bot = BorealisBot(config)
cache_server_bot = discord.Client(intents=discord.Intents.all())

# On ready handler
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name}#{bot.user.discriminator} ({bot.user.id})")
    validate_members.start()
    ensure_invites.start()
    ensure_cache_servers.start()
    nuke_not_approved.start()
    await cache_server_bot.start(config.cache_server_maker.token)
    await bot.tree.sync()

@cache_server_bot.event
async def on_ready():
    for guild in cache_server_bot.guilds:
        if guild.owner_id == cache_server_bot.user.id:
            await guild.delete()
        else:
            await guild.leave()

async def create_unprovisioned_cache_server():
    await cache_server_bot.wait_until_ready()

    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    if not oauth_md:
        raise Exception("Oauth metadata not setup, please run #cs_oauth_mdset to set owner id for new cache servers")
    
    oauth_creds = await bot.pool.fetch("SELECT user_id, access_token, refresh_token, expires_at from cache_server_oauths")

    for cred in oauth_creds:
        # Check if expired, if so, refresh access token and update db
        if cred["expires_at"] < datetime.datetime.now(tz=datetime.timezone.utc):
            data = {
                "grant_type": "refresh_token",
                "refresh_token": cred["refresh_token"],
                "client_id": config.cache_server_maker.client_id,
                "client_secret": config.cache_server_maker.client_secret,
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{bot.config.base_url}/api/v10/oauth2/token", data=data) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed to refresh token for {cred['user_id']}")
                    
                    data = await resp.json()
                    await bot.pool.execute("UPDATE cache_server_oauths SET access_token = $1, refresh_token = $2, expires_at = $3 WHERE user_id = $4", data["access_token"], data["refresh_token"], datetime.datetime.now() + datetime.timedelta(seconds=data["expires_in"]), cred["user_id"])

    guild: discord.Guild = await cache_server_bot.create_guild(name="IBLCS-" + secrets.token_hex(4))

    # Add owner first to transfer ownership
    owner_creds = None
    for cred in oauth_creds:
        if cred["user_id"] == oauth_md["owner_id"]:
            owner_creds = cred
            break
    
    if not owner_creds:
        raise Exception("Owner credentials not found")
    
    async with aiohttp.ClientSession() as session:
        # First add owner
        async with session.put(f"https://discord.com/api/v10/guilds/{guild.id}/members/{owner_creds['user_id']}", headers={"Authorization": f"Bot {config.cache_server_maker.token}"}, json={"access_token": owner_creds["access_token"]}) as resp:
            if not resp.ok:
                raise Exception(f"Failed to add owner to guild: {await resp.text()}")
        
        await asyncio.sleep(1)

        # Add all other users
        for cred in oauth_creds:
            if cred["user_id"] == owner_creds["user_id"]:
                continue

            async with session.put(f"https://discord.com/api/v10/guilds/{guild.id}/members/{cred['user_id']}", headers={"Authorization": f"Bot {config.cache_server_maker.token}"}, json={"access_token": cred["access_token"]}) as resp:
                if not resp.ok:
                    raise Exception(f"Failed to add user to guild: {await resp.text()}")

            await asyncio.sleep(1)
        
    # create oauthadmin role
    oauth_admin_role = await guild.create_role(name="Oauth Admin", permissions=discord.Permissions.all(), color=discord.Color.blurple(), hoist=True)

    # Give all users oauthadmin role
    async for member in guild.fetch_members():
        await member.add_roles(oauth_admin_role)

    # Send everyone ping to temp channel
    temp_chan = await guild.create_text_channel("temp")

    await temp_chan.send(
        f"@everyone"
    )

    msg = f"""
New unprovisioned cache server created!

Add the following bots to the server: 

        """

    for b in bot.config.needed_bots:
        invite = b.invite.replace("{id}", str(b.id)).replace("{perms}", "8")
        msg += f"\n- {b.name}: [{invite}]\n"
    
    msg += "\n3. Run the following command in the server: ``#make_cache_server true``"

    await temp_chan.send(msg)

    # Transfer ownership and leave
    await guild.edit(owner=discord.Object(int(owner_creds["user_id"])))
    await guild.leave()

async def handle_member(member: discord.Member, cache_server_info):
    """
    Handles a member, including adding them to any needed roles
    
    This is a seperate function to allow for better debugging using the WIP fastapi webserver
    """
    if not member.bot:
        try:
            usp = await get_user_staff_perms(bot.pool, member.id)
        except:
            usp = None
        
        if usp and cache_server_info:
            staff_role = member.guild.get_role(int(cache_server_info["staff_role"]))

            if not staff_role:
                # Send alert to logs channel
                logs_channel = member.guild.get_channel(int(cache_server_info["logs_channel"]))

                if logs_channel:
                    await logs_channel.send(f"Failed to find staff role for staff member {member.name} ({member.id}). The staff role currently configured is {cache_server_info['staff_role']}. Please verify this role exists <@&{cache_server_info['staff_role']}>")
                return

            if len(usp.user_positions) > 0:
                # Add staff role
                if staff_role not in member.roles:
                    await member.add_roles(staff_role)
            elif staff_role in member.roles:
                await member.remove_roles(staff_role) 

        return

    # If still not found...
    if not cache_server_info:
        # Ignore non-cache servers
        return 

    # Check if the bot is in the needed bots list
    if str(member.id) in [str(b.id) for b in bot.config.needed_bots]:
        # Give Needed Bots role and Bots role
        needed_bots_role = member.guild.get_role(int(cache_server_info["system_bots_role"]))
        bots_role = member.guild.get_role(int(cache_server_info["bots_role"]))

        if not needed_bots_role or not bots_role:
            # Send alert to logs channel
            logs_channel = member.guild.get_channel(int(cache_server_info["logs_channel"]))

            if logs_channel:
                await logs_channel.send(f"Failed to find needed roles for needed bot {member.name} ({member.id}). The Needed Bots role currently configured is {cache_server_info['system_bots_role']} and the Bots role is {cache_server_info['bots_role']}. Please verify these roles exist <@&{cache_server_info['staff_role']}>")
            return

        # Check if said bot has the needed roles
        if needed_bots_role not in member.roles or bots_role not in member.roles:
            # Add the roles
            return await member.add_roles(needed_bots_role, bots_role)

        return

    # Check if this bot has been selected for this cache server
    count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_bots WHERE guild_id = $1 AND bot_id = $2", str(member.guild.id), str(member.id))

    if not count:
        # Not white-listed, kick it
        return await member.kick(reason="Not white-listed for cache server")
    
    # Also, check that the bot is approved or certified
    bot_type = await bot.pool.fetchval("SELECT type from bots WHERE bot_id = $1", str(member.id))

    if bot_type and bot_type not in ["approved", "certified"]:
        # Not approved or certified, kick it
        await bot.pool.execute("DELETE FROM cache_server_bots WHERE guild_id = $1 AND bot_id = $2", str(member.guild.id), str(member.id))
        return await member.kick(reason="Not approved or certified")

    # Add the bot to the Bots role
    bots_role = member.guild.get_role(int(cache_server_info["bots_role"]))

    if not bots_role:
        # Send alert to logs channel
        logs_channel = member.guild.get_channel(int(cache_server_info["logs_channel"]))

        if logs_channel:
            await logs_channel.send(f"Failed to find Bots role for bot {member.name} ({member.id}). The Bots role currently configured is {cache_server_info['bots_role']}. Please verify this role exists <@&{cache_server_info['staff_role']}>")
        return

    if bots_role not in member.roles:
        await member.add_roles(bots_role)

@bot.event
async def on_member_join(member: discord.Member):
    cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role from cache_servers WHERE guild_id = $1", str(member.guild.id))
    await handle_member(member, cache_server_info=cache_server_info)

@tasks.loop(minutes=5)
async def nuke_not_approved():
    print (f"Starting nuke_not_approved task on {datetime.datetime.now()}")

    # Get all bots that are not approved or certified
    not_approved = await bot.pool.fetch("SELECT bot_id, guild_id from cache_server_bots WHERE bot_id NOT IN (SELECT bot_id from bots WHERE type = 'approved' OR type = 'certified')")

    for b in not_approved:
        # Delete it first
        await bot.pool.execute("DELETE FROM cache_server_bots WHERE bot_id = $1", b["bot_id"])

        guild = bot.get_guild(int(b["guild_id"]))

        if guild:
            member = guild.get_member(int(b["bot_id"]))

            if member:
                await member.kick(reason="Not approved or certified")

_ensure_cache_server = {} # delete if fail check 3 times
@tasks.loop(minutes=5)
async def ensure_cache_servers():
    print(f"Starting ensure_cache_servers task on {datetime.datetime.now()}")

    # First ensure only one row of cache_server_oauth_md exists
    count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_oauth_md")

    if count > 1:
        await bot.pool.execute("DELETE FROM cache_server_oauth_md WHERE id NOT IN (SELECT id from cache_server_oauth_md ORDER BY id DESC LIMIT 1)")

    # Check for guilds we are not in
    unknown_guilds = await bot.pool.fetch("SELECT guild_id from cache_servers WHERE guild_id != ALL($1)", [str(g.id) for g in bot.guilds])

    for guild in unknown_guilds:
        print(f"ALERT: Found unknown server {guild['guild_id']}")

        c = _ensure_cache_server.get(guild["guild_id"], 0)

        if c > 3:
            await bot.pool.execute("DELETE FROM cache_servers WHERE guild_id = $1", guild["guild_id"])

        _ensure_cache_server[guild["guild_id"]] = c + 1


@tasks.loop(minutes=15)
async def ensure_invites():
    """Task to ensure and correct guild invites for all servers"""
    print(f"Starting ensure_invites task on {datetime.datetime.now()}")
    for guild in bot.guilds:
        cache_server_info = await bot.pool.fetchrow("SELECT welcome_channel, logs_channel, invite_code from cache_servers WHERE guild_id = $1", str(guild.id))

        if not cache_server_info:
            continue

        print(f"Validating invites for {guild.name} ({guild.id})")
        invites = await guild.invites()

        have_invite = False
        for invite in invites:
            if invite.code == cache_server_info["invite_code"]:
                have_invite = True
            else:
                if not invite.expires_at:
                    await invite.delete(reason="Unlimited invites are not allowed on cache servers")

        if not have_invite:
            logs_channel = guild.get_channel(int(cache_server_info["logs_channel"]))

            if not logs_channel:
                print(f"Failed to find logs channel for {guild.name} ({guild.id})")
                continue

            welcome_channel = guild.get_channel(int(cache_server_info["welcome_channel"]))

            if not welcome_channel:
                print(f"Failed to find welcome channel for {guild.name} ({guild.id})")
                await logs_channel.send("Failed to find welcome channel, creating new one")
                welcome_channel = await guild.create_text_channel("welcome", reason="Welcome channel")
                await bot.pool.execute("UPDATE cache_servers SET welcome_channel = $1 WHERE guild_id = $2", str(welcome_channel.id), str(guild.id))

            await logs_channel.send("Cache server invite has expired, creating new one")
            invite = await welcome_channel.create_invite(reason="Cache server invite", unique=True, max_uses=0, max_age=0)
            await bot.pool.execute("UPDATE cache_servers SET invite_code = $1 WHERE guild_id = $2", invite.code, str(guild.id))

@tasks.loop(minutes=5) 
async def validate_members():
    """Task to validate all members every 5 minutes"""
    print(f"Starting validate_members task on {datetime.datetime.now()}")
    for guild in bot.guilds:
        cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role, name from cache_servers WHERE guild_id = $1", str(guild.id))

        if not cache_server_info:
            if guild.id in bot.config.pinned_servers:
                continue

            if os.environ.get("DELETE_GUILDS", "false").lower() == "true":
                print(f"ALERT: Found unknown server {guild.name} ({guild.id}), leaving/deleting")
                try:
                    if guild.owner_id == bot.user.id:
                        print(f"ALERT: Guild owner is bot, deleting guild")
                        await guild.delete()
                    else:
                        print(f"ALERT: Guild owner is not bot, leaving guild")
                        await guild.leave()
                except discord.HTTPException:
                    print(f"ALERT: Failed to leave/delete guild {guild.name} ({guild.id})")
            
            continue
        else:
            # Check name
            if not cache_server_info["name"]:
                await bot.pool.execute("UPDATE cache_servers SET name = $1 WHERE guild_id = $2", guild.name, str(guild.id))
            elif cache_server_info["name"] != guild.name:
                # Update server name
                await guild.edit(name=cache_server_info["name"])      

        print(f"Validating members for {guild.name} ({guild.id})")
        for member in guild.members:
            if member.id == bot.user.id:
                continue
    
            print(f"Validating {member.name} ({member.id}) [bot={member.bot}]")
            await handle_member(member, cache_server_info=cache_server_info)

# Error handler
@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return

    traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
    await ctx.send(f"Error: {error}")

@bot.hybrid_command()
async def db_test(ctx: commands.Context):
    await bot.pool.acquire()
    await ctx.send("Acquired connection")

@bot.hybrid_command()
async def kittycat(
    ctx: commands.Context, 
    only_show_resolved: bool | None = commands.parameter(default=True, description="Whether to only show resolved permissions or not")
):
    """Returns the resolved permissions of the user"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if only_show_resolved:
        await ctx.send(f"**Resolved**: ``{' | '.join(resolved)}``")
    else:
        await ctx.send(f"**Positions:** {[f'{usp.id} [{usp.index}]' for usp in usp.user_positions]} with overrides: {usp.perm_overrides}\n\n**Resolved**: ``{' | '.join(resolved)}``")

@bot.hybrid_command()
async def csreport(ctx: commands.Context, only_file: bool = False):
    """Create report on all cache servers"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.csreport"):
        return await ctx.send("You need ``borealis.csreport`` permission to use this command!")

    servers = await bot.pool.fetch("SELECT guild_id, bots_role, system_bots_role, logs_channel, staff_role, welcome_channel, invite_code from cache_servers")

    msg = "Cache Servers:\n"

    for s in servers:
        guild = bot.get_guild(int(s["guild_id"]))

        if not guild:
            continue

        opts = {
            "bots_role": guild.get_role(int(s["bots_role"])) or f"{s['bots_role']}, not found",
            "system_bots_role": guild.get_role(int(s["system_bots_role"])) or f"{s['system_bots_role']}, not found",
            "logs_channel": guild.get_channel(int(s["logs_channel"])) or f"{s['logs_channel']}, not found",
            "staff_role": guild.get_role(int(s["staff_role"])) or f"{s['staff_role']}, not found",
            "welcome_channel": guild.get_channel(int(s["welcome_channel"])) or f"{s['welcome_channel']}, not found",
            "invite_code": s["invite_code"]
        }

        opts_str = ""

        for k, v in opts.items():
            opts_str += f"\n- {k}: {v}"

        msg += f"\n- {guild.name} ({guild.id})\n{opts_str}"

        # Get all bots in cache server
        bots = await bot.pool.fetch("SELECT bot_id, guild_id, created_at, added from cache_server_bots")

        msg += "\n\n== Bots =="

        for b in bots:
            if b["guild_id"] == str(guild.id):
                name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
                msg += f"\n- {name} [{b['bot_id']}]: {b['created_at']} ({b['added']})"

    if len(msg) < 1500 and not only_file:
        await ctx.send(msg)
    else:
        # send as file
        file = discord.File(filename="cache_servers.txt", fp=io.BytesIO(msg.encode("utf-8")))
        await ctx.send(file=file)

async def get_selected_bots(guild_id: int | str):
    # Check currently selected too
    selected = await bot.pool.fetch("SELECT bot_id, created_at, added from cache_server_bots WHERE guild_id = $1 ORDER BY created_at DESC", str(guild_id))

    if len(selected) < MAX_PER_CACHE_SERVER:
        # Try selecting other bots and adding it to db
        not_yet_selected = await bot.pool.fetch("SELECT bot_id, created_at from bots WHERE (type = 'approved' OR type = 'certified') AND cache_server_uninvitable IS NULL AND bot_id NOT IN (SELECT bot_id from cache_server_bots) ORDER BY RANDOM() DESC LIMIT 50")

        for b in not_yet_selected:
            if len(selected) >= MAX_PER_CACHE_SERVER:
                break

            created_at = await bot.pool.fetchval("INSERT INTO cache_server_bots (guild_id, bot_id) VALUES ($1, $2) RETURNING created_at", str(guild_id), b["bot_id"])
            selected.append({"bot_id": b["bot_id"], "created_at": created_at, "added": 0})
    
    elif len(selected) > MAX_PER_CACHE_SERVER:
        remove_amount = len(selected) - MAX_PER_CACHE_SERVER
        to_remove = selected[:remove_amount]
        await bot.pool.execute("DELETE FROM cache_server_bots WHERE guild_id = $1 AND bot_id = ANY($2)", str(guild_id), [b["bot_id"] for b in to_remove])
        selected = selected[remove_amount:]
    
    return selected

@bot.hybrid_command()
async def csbots(ctx: commands.Context, only_show_not_on_server: bool = True):
    """Selects 50 bots for a cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.csbots"):
        return await ctx.send("You need ``borealis.csbots`` permission to use this command!")

    # Check if a cache server
    is_cache_server = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

    if not is_cache_server:
        return await ctx.send("This server is not a cache server")

    # Check currently selected too
    selected = await get_selected_bots(ctx.guild.id)

    msg = "Selected bots:\n"

    showing = 0
    for b in selected:
        if only_show_not_on_server:
            # Check if in server
            in_server = ctx.guild.get_member(int(b["bot_id"]))
            if in_server:
                continue
        
        showing += 1

        name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
        client_id = await bot.pool.fetchval("SELECT client_id from bots WHERE bot_id = $1", b["bot_id"])
        msg += f"\n- {name} [{b['bot_id']}]: https://discord.com/api/oauth2/authorize?client_id={client_id or b['bot_id']}&guild_id={ctx.guild.id}&scope=bot ({b['added']}, {b['created_at']})"

        if len(msg) >= 1500:
            await ctx.send(msg)
            msg = ""

    if msg:
        await ctx.send(msg)
    
    await ctx.send(f"Total: {len(selected)} bots\nShowing: {showing} bots")

@bot.hybrid_command()
async def cslist(
    ctx: commands.Context,
):
    """Lists all cache servers, their names, their invites and when they were made"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.cslist"):
        return await ctx.send("You need ``borealis.cslist`` permission to use this command!")

    servers = await bot.pool.fetch("SELECT guild_id, invite_code, name, created_at from cache_servers")

    msg = "Cache Servers:\n"

    for s in servers:            
        bot_count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_bots WHERE guild_id = $1", s["guild_id"])

        msg += f"\n- {s['guild_id']} ({s['name']}) ({bot_count} bots): [{s['invite_code']}, https://discord.gg/{s['invite_code']}] ({s['created_at']})"

        if len(msg) >= 1500:
            await ctx.send(msg, suppress_embeds=True)
            msg = ""

    if msg:
        await ctx.send(msg, suppress_embeds=True)

    await ctx.send(f"Total: {len(servers)} servers")

@bot.hybrid_command()
async def cs_mark_uninvitable(
    ctx: commands.Context,
    bot_id: int,
    reason: str
):
    """Marks a bot as uninvitable in the cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.csbots"):
        return await ctx.send("You need ``borealis.csbots`` permission to use this command!")

    # Check if a cache server
    is_cache_server = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

    if not is_cache_server:
        return await ctx.send("This server is not a cache server")

    # Check if bot is in cache_server_bots
    count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_bots WHERE guild_id = $1 AND bot_id = $2", str(ctx.guild.id), str(bot_id))

    if not count:
        return await ctx.send("Bot is not in cache server")

    await bot.pool.execute("UPDATE bots SET cache_server_uninvitable = $1 WHERE bot_id = $2", reason, str(bot_id))
    await bot.pool.execute("DELETE FROM cache_server_bots WHERE bot_id = $1", str(bot_id))
    await ctx.send("Bot marked as uninvitable")

@bot.hybrid_command()
async def cs_unmark_uninvitable(
    ctx: commands.Context,
    bot_id: int
):
    """Unmarks a bot as uninvitable"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.csbots"):
        return await ctx.send("You need ``borealis.csbots`` permission to use this command!")

    # Check if a cache server
    is_cache_server = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

    if not is_cache_server:
        return await ctx.send("This server is not a cache server")

    await bot.pool.execute("UPDATE bots SET cache_server_uninvitable = NULL WHERE bot_id = $1", str(bot_id))
    await ctx.send("Bot unmarked as uninvitable")

@bot.hybrid_command()
async def cs_list_uninvitable(
    ctx: commands.Context,
    only_show_for_guild: bool = False
):
    """Shows a list of all bots marked as uninvitable with their reason"""
    if only_show_for_guild:
        is_cache_server = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

        if not is_cache_server:
            return await ctx.send("This server is not a cache server")

        bots = await bot.pool.fetch("SELECT bot_id, cache_server_uninvitable from bots WHERE guild_id = $1 AND cache_server_uninvitable IS NOT NULL", str(ctx.guild.id))
        message = "Uninvitable bots for this server:\n"

        for b in bots:
            name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
            cache_server = await bot.pool.fetchrow("SELECT guild_id, invite_code, name from cache_servers WHERE guild_id = $1", str(ctx.guild.id))
            message += f"\n- {name} [{b['bot_id']}]: {b['cache_server_uninvitable']} [{cache_server['guild_id']}, {cache_server['name']}, {cache_server['invite_code']}]"

            if len(message) >= 1500:
                await ctx.send(message)
                message = ""
        
        if message:
            await ctx.send(message)
        
        return

    # Show all
    bots = await bot.pool.fetch("SELECT bot_id, cache_server_uninvitable from bots WHERE cache_server_uninvitable IS NOT NULL")

    message = "Uninvitable bots:\n"

    for b in bots:
        name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
        message += f"\n- {name} [{b['bot_id']}]: {b['cache_server_uninvitable']}"

        if len(message) >= 1500:
            await ctx.send(message)
            message = ""
        
    if message:
        await ctx.send(message)

@bot.hybrid_command()
async def make_cache_server(
    ctx: commands.Context, 
    is_cache_server: bool | None = commands.parameter(default=False, description="Whether the server should be setup as a cache server or not")
):
    """Creates a cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.make_cache_servers"):
        return await ctx.send("You need ``borealis.make_cache_servers`` permission to use this command!")

    existing = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

    if existing:
        return await ctx.send("This server is already a cache server")

    msg = ""

    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    if not is_cache_server:
        # Ensure user is in oauth flow
        count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_oauths WHERE user_id = $1", str(ctx.author.id))

        if not count:
            return await ctx.send("You need to be in the oauth flow to create a cache server")

        try:
            await create_unprovisioned_cache_server()
        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
            return await ctx.send(f"Failed to create unprovisioned cache server: {e}")
        await ctx.send("Provisioned new server")
    else:
        if ctx.guild.id in bot.config.pinned_servers:
            return await ctx.send("This server is a pinned server and cannot be converted to a cache server")

        if str(ctx.guild.owner_id) != oauth_md["owner_id"]:
            return await ctx.send("Cache server owner id and expected owner id do not match")

        done_bots = []
        needed_bots_members: list[discord.Member] = []
        for b in bot.config.needed_bots:
            member = ctx.guild.get_member(b.id)

            if member:
                done_bots.append(b.name)
                needed_bots_members.append(member)
                continue
            
        not_yet_added = list(filter(lambda b: b.name not in done_bots, bot.config.needed_bots))
        if not not_yet_added:
            if not ctx.me.guild_permissions.administrator:
                return await ctx.send("Please give Borealis administrator in order to continue")

            # Create the 'Needed Bots' role
            needed_bots_role = await ctx.guild.create_role(name="System Bots", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)
            hs_role = await ctx.guild.create_role(name="Holding Staff", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)
            bots_role = await ctx.guild.create_role(name="Bots", permissions=discord.Permissions(view_audit_log=True, create_expressions=True, manage_expressions=True, external_emojis=True, external_stickers=True), color=discord.Color.blurple(), hoist=True)

            await ctx.me.add_roles(needed_bots_role, bots_role)
            await ctx.author.add_roles(hs_role)

            for m in needed_bots_members:
                await m.add_roles(needed_bots_role, bots_role)

            welcome_category = await ctx.guild.create_category("Welcome")
            welcome_channel = await welcome_category.create_text_channel("welcome")
            invite = await welcome_channel.create_invite(reason="Cache server invite", unique=True, max_uses=0, max_age=0)
            logs_category = await ctx.guild.create_category('Logging')
            logs_channel = await logs_category.create_text_channel('system-logs')
            
            await bot.pool.execute("INSERT INTO cache_servers (guild_id, bots_role, system_bots_role, logs_channel, staff_role, welcome_channel, invite_code, name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", str(ctx.guild.id), str(bots_role.id), str(needed_bots_role.id), str(logs_channel.id), str(hs_role.id), str(welcome_channel.id), invite.code, ctx.guild.name)
            await ctx.send("Cache server added to database")
            async with aiohttp.ClientSession() as session:
                hook = discord.Webhook.from_url(bot.config.notify_webhook, session=session)
                await hook.send(content=f"@Bot Reviewers\n\nCache server added: {ctx.guild.name} ({ctx.guild.id}) {invite.url}")
        else:
            msg = "The following bots have not been added to the server yet:\n"
            
            for b in not_yet_added:
                invite = b.invite.replace("{id}", str(b.id)).replace("{perms}", "8")
                msg += f"\n- {b.name}: [{invite}]\n"
            
            msg += "\nPlease add these bots to the server and run the command again"
            return await ctx.send(msg)

@bot.hybrid_command()
async def cs_delete(
    ctx: commands.Context,
    guild_id: int = None
):
    """Deletes a cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.cs_delete"):
        return await ctx.send("You need ``borealis.cs_delete`` permission to use this command!")

    cs_data = await bot.pool.fetchrow("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", guild_id or str(ctx.guild.id))

    if not cs_data:
        return await ctx.send("Specified server is not a cache server")

    await bot.pool.execute("DELETE FROM cache_servers WHERE guild_id = $1", guild_id or str(ctx.guild.id))
    await ctx.send("Cache server deleted")
    
    if guild_id:
        guild = bot.get_guild(guild_id)

        if guild:
            await guild.leave()
    else:
        await ctx.guild.leave()

@bot.hybrid_command()
async def cs_oauth_list(
    ctx: commands.Context
):
    """Lists all oauth2s configured for cache servers"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.cs_oauth_list"):
        return await ctx.send("You need ``borealis.cs_oauth_list`` permission to use this command!")

    oauths = await bot.pool.fetch("SELECT user_id from cache_server_oauths")   
    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    msg = "OAuths:\n"

    for o in oauths:
        try:
            usp = await get_user_staff_perms(bot.pool, o["user_id"])
            resolved = usp.resolve()
        except:
            resolved = []

        service_account = has_perm(resolved, "service_account.marker")

        msg += f"\n- {o['user_id']} <@{o['user_id']}> (service account={service_account})"

        if len(msg) >= 1500:
            await ctx.send(msg)
            msg = ""

    if msg:
        await ctx.send(msg)

    if oauth_md:
        msg = f"Metadata:\n- Currently selected cache server owner: {oauth_md['owner_id']} (<@{oauth_md['owner_id']}>)"
        await ctx.send(msg)

@bot.hybrid_command()
async def cs_oauth_add(
    ctx: commands.Context,
    bypass_checks: bool = False,
):
    """Adds an oauth2 to the cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.cs_oauth_add"):
        return await ctx.send("You need ``borealis.cs_oauth_add`` permission to use this command!")

    if not bypass_checks:
        count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_oauths WHERE user_id = $1", str(ctx.author.id))

        if count:
            return await ctx.send("User is already an oauth2")

    await ctx.send(f"Visit {config.base_url}/oauth2 to continue")    

@bot.hybrid_command()
async def cs_oauth_mdset(
    ctx: commands.Context,
    owner_id: str
):
    """Sets a user as a service account or not"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.cs_oauth_mdset"):
        return await ctx.send("You need ``borealis.cs_oauth_mdset`` permission to use this command!")

    oauth_v = await bot.pool.fetchval("SELECT user_id from cache_server_oauths WHERE user_id = $1", owner_id)

    if not oauth_v:
        return await ctx.send("User is not an oauth2 user")
    
    md_count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_oauth_md")

    if md_count:
        await bot.pool.execute("UPDATE cache_server_oauth_md SET owner_id = $1", owner_id)
    else:
        await bot.pool.execute("INSERT INTO cache_server_oauth_md (owner_id) VALUES ($1)", owner_id)

    await ctx.send("Cache Server OAuth Metadata updated")

@bot.hybrid_command()
async def nuke_from_main_server(
    ctx: commands.Context,
    guild_id: int
):
    """Nukes a server from the main server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.nuke_from_main_server"):
        return await ctx.send("You need ``borealis.nuke_from_main_server`` permission to use this command!")

    if guild_id not in config.pinned_servers:
        return await ctx.send("Guild is not a pinned server. Must be temporarily pinned to nuke")

    guild = bot.get_guild(guild_id)

    if not guild:
        return await ctx.send("Guild not found")
    
    bots_to_nuke = await bot.pool.fetch("SELECT bot_id, type, premium from bots WHERE type != 'certified' AND premium = false")
    
    class KickAskView(discord.ui.View):
        def __init__(self, member: discord.Member):
            super().__init__(timeout=1000)    
            self.done = False
            self.member = member
        
        @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger, custom_id="kick")
        async def kick(self, interaction: discord.Interaction, button: discord.ui.Button,):
            await interaction.response.send_message(f"Kicking {self.member} ({self.member.id})", ephemeral=False)
            await guild.kick(self.member)
            self.done = True
            self.stop()
        
        @discord.ui.button(label="Ignore", style=discord.ButtonStyle.secondary, custom_id="nothing")
        async def nothing(self, interaction: discord.Interaction, button: discord.ui.Button):
            await interaction.response.send_message(f"Ignoring {self.member} ({self.member.id})", ephemeral=False)
            self.done = True
            self.stop()

    for b in bots_to_nuke:
        bot_obj = guild.get_member(int(b["bot_id"]))

        if not bot_obj:
            continue

        if bot_obj.id in [815553000470478850, ctx.me.id]:
            await ctx.send(f"Skipping {bot_obj.name} ({bot_obj.id})")
            continue # Ignore

        # Ask with a component
        view = KickAskView(bot_obj)

        random_emoji = secrets.choice(guild.emojis)

        await ctx.send(f"Should I kick {bot_obj.name} ({bot_obj.id}) [type={b['type']}, premium={b['premium']}]? {random_emoji}", view=view)
        await view.wait()

        if not view.done:
            return await ctx.send("Timed out")
    
    await ctx.send("Done from db")

    for bot_obj in guild.members:
        if not bot_obj:
            continue

        if bot_obj.id in [815553000470478850, ctx.me.id]:
            continue

        if not bot_obj.bot:
            continue

        # Ensure its not premium or certified
        typ = await bot.pool.fetchrow("SELECT type, premium from bots WHERE bot_id = $1", str(bot_obj.id))

        if typ and (typ["type"] in ["certified"] or typ["premium"]):
            continue

        # Ask with a component
        view = KickAskView(bot_obj)

        random_emoji = secrets.choice(guild.emojis)

        await ctx.send(f"Should I kick {bot_obj.name} ({bot_obj.id}) [not on db]? {random_emoji}", view=view)
        await view.wait()

        if not view.done:
            return await ctx.send("Timed out")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bot.run())
