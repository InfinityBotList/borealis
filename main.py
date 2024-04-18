import discord
from discord.ext import commands, tasks
from pydantic import BaseModel, Field
from ruamel.yaml import YAML
import logging
import asyncpg
import asyncio
from kittycat.perms import get_user_staff_perms
from kittycat.kittycat import StaffPermissions, has_perm
import secrets
import traceback
import sys
import os
import datetime
import io
import importlib
import uvicorn
import aiohttp
from typing import Callable
from PIL import Image, ImageDraw, ImageFont
from cfg_autogen import gen_config
from migrations import MIGRATION_LIST, Migration
from constants import BOTS_ROLE_PERMS

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
    postgres_url: str = Field(default="postgresql:///infinity")
    pinned_servers: list[int] = Field(default=[870950609291972618, 758641373074423808])
    main_server: int = Field(default=758641373074423808)
    needed_bots: list[NeededBots] = Field(
        default=[
            NeededBots(
                name = "Borealis",
                id = 1200677946789212242,
                invite = "https://discord.com/api/oauth2/authorize?client_id={id}&permissions=8&scope=bot%20applications.commands"
            ),
            NeededBots(
                name = "Arcadia",
                id = 870728078228324382,
                invite = "https://discord.com/api/oauth2/authorize?client_id={id}&scope=bot%20applications.commands"
            ),
            NeededBots(
                name = "Popplio",
                id = 815553000470478850,
                invite = "https://discord.com/api/oauth2/authorize?client_id={id}&scope=bot%20applications.commands"
            )
        ]
    )
    notify_webhook: str
    base_url: str
    cache_server_maker: CacheServerMaker
    borealis_client_id: int
    borealis_client_secret: str

gen_config(Config, 'config.yaml.sample')

yaml = YAML(typ="safe")
with open("config.yaml", "r") as f:
    config = Config(**yaml.load(f))

with open("guild_logo.png", "rb") as f:
    guild_logo = f.read()

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
    ensure_guild_image.start()
    main_server_kicker.start()
    await cache_server_bot.start(config.cache_server_maker.token)
    await bot.tree.sync()

@cache_server_bot.event
async def on_ready():
    for guild in cache_server_bot.guilds:
        if guild.owner_id == cache_server_bot.user.id:
            await guild.delete()
        else:
            await guild.leave()

async def create_cache_server(guild: discord.Guild):
    # Check that we're not already in a cache server
    count = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(guild.id))

    if count:
        return False
    
    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    if guild.id in bot.config.pinned_servers:
        return False

    if str(guild.owner_id) != oauth_md["owner_id"]:
        return False

    done_bots = []
    needed_bots_members: list[discord.Member] = []
    for b in bot.config.needed_bots:
        member = guild.get_member(b.id)

        if member:
            done_bots.append(b.name)
            needed_bots_members.append(member)
            continue
        
    not_yet_added = list(filter(lambda b: b.name not in done_bots, bot.config.needed_bots))
    if not not_yet_added:
        if not guild.me.guild_permissions.administrator:
            raise Exception("Please give Borealis administrator in order to continue")

        # Create the 'Needed Bots' role
        needed_bots_role = await guild.create_role(name="System Bots", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)
        hs_role = await guild.create_role(name="Holding Staff", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)
        webmod_role = await guild.create_role(name="Web Moderator", permissions=discord.Permissions(manage_guild=True, kick_members=True, ban_members=True, moderate_members=True), color=discord.Color.green(), hoist=True)
        bots_role = await guild.create_role(name="Bots", permissions=BOTS_ROLE_PERMS, color=discord.Color.brand_red(), hoist=True)

        await guild.me.add_roles(needed_bots_role, bots_role)

        for m in needed_bots_members:
            await m.add_roles(needed_bots_role, bots_role)

        welcome_category = await guild.create_category("Welcome")
        welcome_channel = await welcome_category.create_text_channel("welcome")
        invite = await welcome_channel.create_invite(reason="Cache server invite", unique=True, max_uses=0, max_age=0)
        logs_category = await guild.create_category('Logging')
        logs_channel = await logs_category.create_text_channel('system-logs')
        
        await bot.pool.execute("INSERT INTO cache_servers (guild_id, bots_role, web_moderator_role, system_bots_role, logs_channel, staff_role, welcome_channel, invite_code, name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", str(guild.id), str(bots_role.id), str(webmod_role.id), str(needed_bots_role.id), str(logs_channel.id), str(hs_role.id), str(welcome_channel.id), invite.code, guild.name)
        async with aiohttp.ClientSession() as session:
            hook = discord.Webhook.from_url(bot.config.notify_webhook, session=session)
            await hook.send(content=f"@Bot Reviewers\n\nCache server added: {guild.name} ({guild.id}) {invite.url}")

        for member in guild.members:
            await handle_member(member, cache_server_info={"bots_role": bots_role.id, "web_moderator_role": str(webmod_role.id), "system_bots_role": needed_bots_role.id, "logs_channel": logs_channel.id, "staff_role": hs_role.id})

        return True
    else:
        raise Exception(f"The following bots have not been added to the server yet: {', '.join([f'{b.id} ({b.name})' for b in not_yet_added])}")

async def resolve_guilds_from_str(guilds: str, check: Callable[[discord.Guild], bool]):
    """
    If guilds is 'all', return all guilds that the bot is in and has kick_members permission
    """
    resolved_guilds: list[discord.Guild] = []
    if guilds == "all":
        resolved_guilds = [g for g in bot.guilds if check(g) and g.id not in bot.config.pinned_servers]
    elif guilds == "cs":
        db_ids = await bot.pool.fetch("SELECT guild_id from cache_servers")
        for g in db_ids:
            guild = bot.get_guild(int(g["guild_id"]))

            if guild and check(guild):
                resolved_guilds.append(guild)
    else:
        for id in guilds.split(","):
            id = id.strip()

            guild = None
            for g in bot.guilds:
                if id == str(g.id) or id == g.name:
                    guild = g
                    break
                    
            if not guild:
                continue

            if guild and check(guild):
                resolved_guilds.append(guild)
    
    return resolved_guilds


async def refresh_oauth(cred: dict):
    # Check if expired, if so, refresh access token and update db
    if cred["expires_at"] < datetime.datetime.now(tz=datetime.timezone.utc):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": cred["refresh_token"],
            "client_id": config.borealis_client_id if cred["bot"] == "borealis" else config.cache_server_maker.client_id,
            "client_secret": config.borealis_client_secret if cred["bot"] == "borealis" else config.cache_server_maker.client_secret,
        }
        print(data)
        async with bot.session.post(f"https://discord.com/api/v10/oauth2/token", data=data, headers=headers) as resp:
            if resp.status != 200:
                err = await resp.text()
                raise Exception(f"Failed to refresh token for {cred['user_id']} {resp.status} {err}")
            
            data = await resp.json()
            await bot.pool.execute("UPDATE cache_server_oauths SET access_token = $1, refresh_token = $2, expires_at = $3 WHERE user_id = $4", data["access_token"], data["refresh_token"], datetime.datetime.now() + datetime.timedelta(seconds=data["expires_in"]), cred["user_id"])

        return {
            "user_id": cred["user_id"],
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
        }
    
    return {
        "user_id": cred["user_id"], 
        "access_token": cred["access_token"],
        "refresh_token": cred["refresh_token"],
    }

async def create_unprovisioned_cache_server():
    await cache_server_bot.wait_until_ready()

    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    if not oauth_md:
        raise Exception("Oauth metadata not setup, please run #cs_oauth_mdset to set owner id for new cache servers")
    
    _oauth_creds = await bot.pool.fetch("SELECT user_id, access_token, refresh_token, expires_at, bot from cache_server_oauths WHERE bot = 'doxycycline'")

    oauth_creds = []
    for cred in _oauth_creds:
        usp = await get_user_staff_perms(bot.pool, int(cred["user_id"]))
        resolved = usp.resolve()

        if not has_perm(resolved, "borealis.make_cache_servers"):
            continue # Don't add this user

        oauth_creds.append(await refresh_oauth(cred))
        
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
            webmod_role = member.guild.get_role(int(cache_server_info["web_moderator_role"]))
            staff_role = member.guild.get_role(int(cache_server_info["staff_role"]))
            
            if not webmod_role:
                async with aiohttp.ClientSession() as session:
                    hook = discord.Webhook.from_url(bot.config.notify_webhook, session=session)
                    await hook.send(content=f"Failed to find web moderator role for staff member {member.name} ({member.id}). The web moderator role currently configured is {cache_server_info['web_moderator_role']}. Please verify this role exists <@&{cache_server_info['staff_role']}>")
                return

            if not staff_role:
                async with aiohttp.ClientSession() as session:
                    hook = discord.Webhook.from_url(bot.config.notify_webhook, session=session)
                    await hook.send(content=f"Failed to find staff role for staff member {member.name} ({member.id}). The staff role currently configured is {cache_server_info['staff_role']}. Please verify this role exists <@&{cache_server_info['staff_role']}>")
                return

            if len(usp.user_positions) == 0:
                if staff_role in member.roles:
                    await member.remove_roles(staff_role)
                if webmod_role in member.roles:
                    await member.remove_roles(webmod_role)
            else:
                # Add webmod role
                if webmod_role not in member.roles:
                    await member.add_roles(webmod_role)
                
                resolved_perms = usp.resolve()

                if has_perm(resolved_perms, "borealis.can_have_staff_role"):
                    if staff_role not in member.roles:
                        await member.add_roles(staff_role)
                else:
                    if staff_role in member.roles:
                        await member.remove_roles(staff_role)

    # If still not found...
    if not cache_server_info:
        # Ignore non-cache servers
        return 

    if member.bot:
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

async def remove_if_tresspassing(member: discord.Member):
    """Removes a bot from the main server if it is not premium, certified or explicitly whitelisted"""
    if member.guild.id != bot.config.main_server:
        raise Exception("Not main server")
   
    if not member.bot:
        raise Exception("Not a bot")

    whitelist_entry = await bot.pool.fetchval("SELECT COUNT(*) from bot_whitelist WHERE bot_id = $1", str(member.id))

    if whitelist_entry:
        return
    
    bot_entry = await bot.pool.fetchval("SELECT COUNT(*) from bots WHERE bot_id = $1 AND (premium = true OR type = 'certified')", str(member.id))

    if bot_entry:
        return
    
    if member.top_role > member.guild.me.top_role:
        print("Cant kick", member.name, member.top_role, member.guild.me.top_role)
    
    await member.kick(reason="Not premium, certified or whitelisted")

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == bot.config.main_server and member.bot:
        await remove_if_tresspassing(member)
        return

    cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role, web_moderator_role from cache_servers WHERE guild_id = $1", str(member.guild.id))
    
    if not cache_server_info:
        try:
            res = await create_cache_server(member.guild)

            if not res:
                print(f"Not making cache server for {member.guild.name} ({member.guild.id}) [failed checks]")
        except Exception as exc:
            print(f"Not making cache server for {member.guild.name} ({member.guild.id}) due to reason: {exc}")
    else:
        await handle_member(member, cache_server_info=cache_server_info)

@tasks.loop(minutes=5)
async def main_server_kicker():
    print(f"Starting main_server_kicker task on {datetime.datetime.now()}")

    main_server = bot.get_guild(bot.config.main_server)

    if not main_server:
        return
    
    for member in main_server.members:
        if member.bot:
            await remove_if_tresspassing(member)

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

@tasks.loop(minutes=120)
async def ensure_guild_image():
    print(f"Starting ensure_guild_image task on {datetime.datetime.now()}")

    for guild in bot.guilds:
        cache_server_info = await bot.pool.fetchrow("SELECT guild_id from cache_servers WHERE guild_id = $1", str(guild.id))

        if not cache_server_info:
            continue

        name = guild.name.split("-")[-1]

        try:
            print("Editing guild logo for", name)

            # Draw name on guild_logo
            img = Image.open(io.BytesIO(guild_logo))
            draw = ImageDraw.Draw(img)
            font = ImageFont.truetype("Roboto-MediumItalic.ttf", 66)
            draw.text((img.width/7, (3.75/6)*img.height), name, (10, 10, 10), font=font, stroke_width=1)
            bio = io.BytesIO()
            img.save(bio, format="PNG")

            bio.seek(0, 0)

            # Try writing file to disk for validation
            with open(f"guild_logo_{guild.name}.png", "wb") as f:
                f.write(bio.read())
            
            bio.seek(0, 0)

            await guild.edit(icon=bio.read())
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Failed to edit guild logo for {name}: {e}")

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
        cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role, web_moderator_role, name from cache_servers WHERE guild_id = $1", str(guild.id))

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
    only_show_resolved: bool | None = commands.parameter(default=True, description="Whether to only show resolved permissions or not"),
    user_id: int | None = commands.parameter(default=None, description="Whether to only show resolved permissions or not")
):
    """Returns the resolved permissions of the user"""
    usp = await get_user_staff_perms(bot.pool, user_id or ctx.author.id)
    resolved = usp.resolve()

    if only_show_resolved:
        await ctx.send(f"**Resolved**: ``{' | '.join(resolved)}``")
    else:
        await ctx.send(f"**Positions:** {[f'{usp.id} [{usp.index}]' for usp in usp.user_positions]} with overrides: {usp.perm_overrides}\n\n**Resolved**: ``{' | '.join(resolved)}``")

@bot.hybrid_command()
async def cs_createreport(ctx: commands.Context, only_file: bool = False):
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
async def cs_allbots(
    ctx: commands.Context, 
    only_show_not_on_server: bool = True,
    only_send_servers_with_needed_action: bool = True
):
    """Partitions all bots and sends"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.csallbots"):
        return await ctx.send("You need ``borealis.csallbots`` permission to use this command!")

    for guild in bot.guilds:
        # Check if a cache server
        is_cache_server = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(guild.id))

        if not is_cache_server:
            continue
            
        msg = f"**Cache server: {guild.name} ({guild.id})**"

        # Check currently selected too
        selected = await get_selected_bots(guild.id)

        msg += "\nSelected bots:\n"

        showing = 0
        for b in selected:
            if only_show_not_on_server:
                # Check if in server
                in_server = guild.get_member(int(b["bot_id"]))
                if in_server:
                    continue
            
            showing += 1

            name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
            client_id = await bot.pool.fetchval("SELECT client_id from bots WHERE bot_id = $1", b["bot_id"])
            msg += f"\n- {name} [{b['bot_id']}]: https://discord.com/api/oauth2/authorize?client_id={client_id or b['bot_id']}&guild_id={guild.id}&scope=bot ({b['added']}, {b['created_at']})"

            if len(msg) >= 1500:
                await ctx.send(msg)
                msg = ""

        if showing or not only_send_servers_with_needed_action:
            msg += f"\nTotal: {len(selected)} bots\nShowing: {showing} bots"
            await ctx.send(msg)

@bot.hybrid_command()
async def cs_bots(ctx: commands.Context, only_show_not_on_server: bool = True):
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
async def cs_list(
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

        # Check that we're not already in a cache server
        count = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(ctx.guild.id))

        if count:
            return await ctx.send("You are already in a cache server. Did you mean ``#make_cache_server true``?")

        if ctx.guild.name.startswith("IBLCS"):
            return await ctx.send("This server is already an unprovisioned cache server. Did you mean ``#make_cache_server true``?")

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

            return await create_cache_server(ctx.guild)
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
async def cs_leave(
    ctx: commands.Context,
    guilds: str,
    reason: str,
    user: discord.User | None = commands.parameter(default=None, description="Who to use for leaving. Defaults to author"),
    ban_user: bool = False
):
    """Leaves cache server(s). Use all to remove from all servers, cs to add to cache servers only or specify guild ids/names to add to specific servers."""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        usp = StaffPermissions(user_positions=[], perm_overrides=[])
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    if user:
        if not has_perm(resolved, "borealis.cs_leave_other"):
            return await ctx.send("You need ``borealis.cs_leave_other`` permission to remove other people from cache servers!")

        # Check if member has lower index than us, if so then error
        try:
            usp_user = await get_user_staff_perms(bot.pool, user.id)
        except:
            usp_user = StaffPermissions(user_positions=[], perm_overrides=[])

        lowest_index = 655356553565535
        for p in usp.user_positions:
            if p.index < lowest_index:
                lowest_index = p.index
        
        lowest_index_user = 655356553565535
        for p in usp_user.user_positions:
            if p.index < lowest_index_user:
                lowest_index_user = p.index
        
        if lowest_index_user <= lowest_index:
            return await ctx.send("You cannot remove someone with a lower or equal index (higher or equal in hierarchy) than you from cache servers")        

    resolved_guilds = await resolve_guilds_from_str(guilds, lambda g: g.me.guild_permissions.ban_members if ban_user else g.me.guild_permissions.kick_members)

    if not resolved_guilds:
        return await ctx.send("No servers found")

    user = user if user else ctx.author
    for g in resolved_guilds:
        if g.id in bot.config.pinned_servers:
            continue

        member = g.get_member(user.id)

        if not member:
            continue
            
        # Check if member is higher in hierarchy than me
        if member.top_role > g.me.top_role:
            await ctx.send(f"User {user.id} ({user.name}) is higher in hierarchy than me in guild {g.id} ({g.name}), skipping ({member.top_role} >= {g.me.top_role})")
            continue

        await ctx.send(f"{'Banning' if ban_user else 'Kicking'} user {user.id} ({user.name}) from {g.id} ({g.name})")
        
        if ban_user:
            await member.ban(reason=f"cs_leave: {reason}")
        else:
            await member.kick(reason=f"cs_leave: {reason}")
            
        await ctx.send(f"Successfully {'banned' if ban_user else 'kicked'} {user.id} ({user.name}) from {g.id} ({g.name})")
        await asyncio.sleep(5)

    await ctx.send("Done")

@bot.hybrid_command()
async def cs_migrate(
    ctx: commands.Context,
    guilds: str = "all",
):  
    """Apply migrations to cache servers"""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        usp = StaffPermissions(user_positions=[], perm_overrides=[])
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    if not has_perm(resolved, "service_account.marker"):
        return await ctx.send("You need ``service_account.marker`` permission to perform migrations!")

    guilds_split = []

    if guilds != "all":
        guilds_split = guilds.split(",")
        for i in range(len(guilds_split)):
            guilds_split[i] = guilds_split[i].strip()

    for migration in MIGRATION_LIST:
        migration_cls: Migration = migration(
            pool=bot.pool,
            ctx=ctx
        )

        migrations_applied = await bot.pool.fetchval("SELECT states from cache_server_migrations_done WHERE migration_id = $1", migration_cls.id())

        if migrations_applied and "done" in migrations_applied:
            continue

        if not migrations_applied:
            await migration_cls.database_migration_func()
            await bot.pool.execute("INSERT INTO cache_server_migrations_done (migration_id, states) VALUES ($1, $2)", migration_cls.id(), ["db_done"])

        cache_servers = await bot.pool.fetch("SELECT guild_id from cache_servers")

        for cs in cache_servers:
            guild = bot.get_guild(int(cs["guild_id"]))

            if not guild:
                continue

            if guilds != "all" and str(guild.id) not in guilds_split:
                continue
            
            current_migration_data = await bot.pool.fetchrow("SELECT state from cache_server_migrations WHERE guild_id = $1 AND migration_id = $2", str(guild.id), migration_cls.id())

            if not current_migration_data:
                await bot.pool.execute("INSERT INTO cache_server_migrations (guild_id, migration_id, state) VALUES ($1, $2, $3)", str(guild.id), migration_cls.id(), [])
                current_migration_data = {"state": []}

            if current_migration_data and "done" in current_migration_data["state"]:
                continue

            await ctx.send(f"Applying migration {migration_cls.id()} on {guild.id} ({guild.name})")
            
            await migration_cls.run_one(guild)

            try:
                await bot.pool.execute("INSERT INTO cache_server_migrations_done (migration_id, states) VALUES ($1, $2)", migration_cls.id(), ["done"])
            except asyncpg.exceptions.UniqueViolationError:
                await bot.pool.execute("UPDATE cache_server_migrations SET state = $1 WHERE guild_id = $2 AND migration_id = $3", ["done"], str(guild.id), migration_cls.id())

            await ctx.send(f"Migration {migration_cls.id()} applied on {guild.id} ({guild.name})")
        
        if guilds == "all":
            await migration_cls.finish()
            try:
                await bot.pool.execute("INSERT INTO cache_server_migrations_done (migration_id) VALUES ($1)", migration_cls.id())
            except asyncpg.exceptions.UniqueViolationError:
                await bot.pool.execute("UPDATE cache_server_migrations_done SET states = states || $1 WHERE migration_id = $2", ["done"], migration_cls.id())
            await ctx.send(f"Migration {migration_cls.id()} applied on all cache servers")
        else:
            await ctx.send(f"Migration {migration_cls.id()} applied on specified cache servers")
    await ctx.send("Done")

@bot.hybrid_command()
async def cs_migration_rollback(
    ctx: commands.Context,
    migration_id: str,
    guilds: str = "all"
):
    """Rollback a database migration"""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        usp = StaffPermissions(user_positions=[], perm_overrides=[])
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    if not has_perm(resolved, "service_account.marker"):
        return await ctx.send("You need ``service_account.marker`` permission to perform migrations!")


    migration_cls: Migration | None = None
    for migration in MIGRATION_LIST:
        cls = migration(
            pool=bot.pool,
            ctx=ctx
        )
        if cls.id() == migration_id:
            migration_cls = cls
            break

    if not migration_cls:
        return await ctx.send("Migration not found")

    await bot.pool.execute("DELETE FROM cache_server_migrations_done WHERE migration_id = $1", migration_id)

    guilds_split = []

    if guilds != "all":
        guilds_split = guilds.split(",")
        for i in range(len(guilds_split)):
            guilds_split[i] = guilds_split[i].strip()

    cache_servers = await bot.pool.fetch("SELECT guild_id from cache_servers")

    for cs in cache_servers:
        guild = bot.get_guild(int(cs["guild_id"]))

        if not guild:
            continue

        if guilds != "all" and str(guild.id) not in guilds_split:
            continue

        #mig_entry = await bot.pool.fetchrow("SELECT state from cache_server_migrations WHERE guild_id = $1 AND migration_id = $2", str(cs["guild_id"]), migration_id)

        #if not mig_entry:
        #    continue
        
        #if "done" not in mig_entry["state"]:
        #    await ctx.send(f"Migration {migration_id} not fully applied on {cs['guild_id']}, errors may occur!")

        await ctx.send(f"Rolling back migration {migration_id} on {cs['guild_id']} ({guild.name})")
        await migration_cls.rollback(guild)
        await bot.pool.execute("DELETE FROM cache_server_migrations WHERE migration_id = $1", migration_id)
        await ctx.send(f"Migration {migration_id} rolled back on {cs['guild_id']} ({guild.name})")

    if guilds == "all":
        await migration_cls.finish_rollback()
        await ctx.send(f"Migration {migration_id} rolled back on all cache servers")
    else:
        await ctx.send("Migration rolled back")

@bot.hybrid_command()
async def cs_oauth_list(
    ctx: commands.Context
):
    """Lists all oauth2s configured for cache servers"""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    oauths = await bot.pool.fetch("SELECT user_id, bot from cache_server_oauths")   
    oauth_md = await bot.pool.fetchrow("SELECT owner_id from cache_server_oauth_md")

    msg = "OAuths:\n"

    for o in oauths:
        try:
            usp = await get_user_staff_perms(bot.pool, int(o["user_id"]))
            resolved = usp.resolve()
        except:
            resolved = []

        service_account = has_perm(resolved, "service_account.marker")

        user = bot.get_user(int(o["user_id"]))

        msg += f"\n- {o['user_id']} [{user}] (bot={o['bot']}, service_account={service_account})"

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
):
    """Sets up oauth2 for a user"""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    await ctx.send(f"Visit {config.base_url}/oauth2 to continue. Note that you may need to authorize yourself twice in a row through Discord (so don't get confused), once for Borealis and a second time for Doxycycline (if you have permission such as Human Resources etc)")    

@bot.hybrid_command()
async def cs_oauth_join(
    ctx: commands.Context,
    guilds: str
):
    """Joins cache server(s) bypassing typical invite flow. Use all to add to all servers, cs to add to cache servers only or specify guild ids/names to add to specific servers."""
    try:
        usp = await get_user_staff_perms(bot.pool, ctx.author.id)
        resolved = usp.resolve()
    except:
        resolved = []
    
    if not resolved:
        return await ctx.send("User is not a staff member")

    _oauth_data = await bot.pool.fetchrow("SELECT user_id, access_token, refresh_token, expires_at, bot from cache_server_oauths WHERE user_id = $1 AND bot = 'borealis'", str(ctx.author.id))

    if not _oauth_data:
        return await ctx.send("User has not authorized to oauth2 yet *for borealis*, use ``cs_oauth_add`` to do so")

    oauth_data = await refresh_oauth(_oauth_data)

    resolved_guilds = await resolve_guilds_from_str(guilds, lambda g: g.me.guild_permissions.create_instant_invite)

    if not resolved_guilds:
        return await ctx.send("No servers found")

    async with aiohttp.ClientSession() as session:
        for g in resolved_guilds:
            await ctx.send(f"Joining {g.id} ({g.name})")

            data = {
                "access_token": oauth_data["access_token"],
            }
            async with session.put(f"https://discord.com/api/v9/guilds/{g.id}/members/{ctx.author.id}", headers={"Authorization": f"Bot {config.token}"}, json=data) as resp:
                if not resp.ok:
                    err = await resp.json()
                    await ctx.send(f"Failed to join {g}: {err}")
                    await asyncio.sleep(3)
                    continue

            await ctx.send(f"Joined {g}")
            await asyncio.sleep(3)
    
    await ctx.send("Done")
        
    
@bot.hybrid_command()
async def cs_oauth_mdset(
    ctx: commands.Context,
    owner_id: str
):
    """Sets a user as a service account or not"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    # We use borealis_mgmt to ensure Human Resources etc cannot use metadata commands
    if not has_perm(resolved, "borealis_mgmt.cs_oauth_mdset"):
        return await ctx.send("You need ``borealis_mgmt.cs_oauth_mdset`` permission to use this command!")

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

        whitelist_entry = await bot.pool.fetchval("SELECT COUNT(*) from bot_whitelist WHERE bot_id = $1", str(bot_obj.id))

        if whitelist_entry or bot_obj.id == ctx.me.id:
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

        whitelist_entry = await bot.pool.fetchval("SELECT COUNT(*) from bot_whitelist WHERE bot_id = $1", str(bot_obj.id))

        if whitelist_entry or bot_obj.id == ctx.me.id:
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
