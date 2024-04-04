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

MAX_PER_CACHE_SERVER = 40

logging.basicConfig(level=logging.INFO)

class NeededBots(BaseModel):
    id: int
    name: str
    invite: str

class Config(BaseModel):
    token: str
    postgres_url: str
    pinned_servers: list[int]
    needed_bots: list[NeededBots]

yaml = YAML(typ="safe")
with open("config.yaml", "r") as f:
    config = Config(**yaml.load(f))

class BorealisBot(commands.AutoShardedBot):
    pool: asyncpg.pool.Pool

    def __init__(self, config: Config):
        super().__init__(command_prefix="#", intents=discord.Intents.all())
        self.config = config
        self.pool = None

    async def run(self):
        self.pool = await asyncpg.pool.create_pool(self.config.postgres_url)
        await super().start(self.config.token)

intents = discord.Intents.all()

bot = BorealisBot(config)

# On ready handler
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name}#{bot.user.discriminator} ({bot.user.id})")
    validate_members.start()

async def handle_member(member: discord.Member, cache_server_info = None):
    """
    Handles a member, including adding them to any needed roles
    
    This is a seperate function to allow for better debugging using the WIP fastapi webserver
    """
    # Ignore non-bots
    if not member.bot:
        return

    # If cache_server_info is None, check if present
    if not cache_server_info:
        cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role from cache_servers WHERE guild_id = $1", str(member.guild.id))

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
    
    # Check if this bot has been selected for this cache server
    count = await bot.pool.fetchval("SELECT COUNT(*) from cache_server_bots WHERE guild_id = $1 AND bot_id = $2", str(member.guild.id), str(member.id))

    if not count:
        # Not white-listed, kick it
        return await member.kick(reason="Not white-listed for cache server")
    
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
    await handle_member(member)

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
        cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role from cache_servers WHERE guild_id = $1", str(guild.id))

        if not cache_server_info:
            if guild.id in bot.config.pinned_servers:
                continue

            print(f"ALERT: Found unknown server {guild.name} ({guild.id}), leaving/deleting")

            if os.environ.get("DELETE_GUILDS", "false").lower() == "true":
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
async def csbots(ctx: commands.Context, only_show_not_on_server: bool):
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
    selected = await bot.pool.fetch("SELECT bot_id, created_at, added from cache_server_bots WHERE guild_id = $1 ORDER BY created_at DESC", str(ctx.guild.id))

    if len(selected) < MAX_PER_CACHE_SERVER:
        # Try selecting other bots and adding it to db
        not_yet_selected = await bot.pool.fetch("SELECT bot_id, created_at from bots WHERE bot_id NOT IN (SELECT bot_id from cache_server_bots) ORDER BY RANDOM() DESC LIMIT 50")

        for b in not_yet_selected:
            if len(selected) >= MAX_PER_CACHE_SERVER:
                break

            created_at = await bot.pool.fetchval("INSERT INTO cache_server_bots (guild_id, bot_id) VALUES ($1, $2) RETURNING created_at", str(ctx.guild.id), b["bot_id"])
            selected.append({"bot_id": b["bot_id"], "created_at": created_at, "added": 0})
    
    elif len(selected) > MAX_PER_CACHE_SERVER:
        # Remove 10 bots
        to_remove = selected[:10]
        await bot.pool.execute("DELETE FROM cache_server_bots WHERE guild_id = $1 AND bot_id = ANY($2)", str(ctx.guild.id), [b["bot_id"] for b in to_remove])
        selected = selected[10:]

    msg = "Selected bots:\n"

    showing = 0
    for b in selected:
        if only_show_not_on_server:
            # Check if in server
            in_server = ctx.guild.get_member(b["bot_id"])

            if in_server:
                continue
        
        showing += 1

        name = await bot.pool.fetchval("SELECT username from internal_user_cache__discord WHERE id = $1", b["bot_id"])
        msg += f"\n- {name} [{b['bot_id']}]: https://discord.com/api/oauth2/authorize?client_id={b['bot_id']}&guild_id={ctx.guild.id}&scope=bot ({b['added']}, {b['created_at']})"

        if len(msg) >= 1500:
            await ctx.send(msg)
            msg = ""

    if msg:
        await ctx.send(msg)
    
    await ctx.send(f"Total: {len(selected)} bots\nShowing: {showing} bots")


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

    if not is_cache_server:
        msg = f"""
1. Create a new server with the following name: IBLCS-{secrets.token_hex(4)}
2. Add the following bots to the server: 

    """

        for b in bot.config.needed_bots:
            invite = b.invite.replace("{id}", str(b.id)).replace("{perms}", "8")
            msg += f"\n- {b.name}: [{invite}]\n"
        
        msg += "\n3. Run the following command in the server: ``#make_cache_server true``"

        return await ctx.send(msg)
    else:
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
            
            bots_role = await ctx.guild.create_role(name="Bots", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)

            hs_role = await ctx.guild.create_role(name="Holding Staff", permissions=discord.Permissions(administrator=True), color=discord.Color.blurple(), hoist=True)

            await ctx.me.add_roles(needed_bots_role, bots_role)

            for m in needed_bots_members:
                await m.add_roles(needed_bots_role, bots_role)

            welcome_category = await ctx.guild.create_category("Welcome")
            welcome_channel = await welcome_category.create_text_channel("welcome")
            invite = await welcome_channel.create_invite(reason="Cache server invite", unique=True, max_uses=0, max_age=0)
            logs_category = await ctx.guild.create_category('Logging')
            logs_channel = await logs_category.create_text_channel('system-logs')
            
            await bot.pool.execute("INSERT INTO cache_servers (guild_id, bots_role, system_bots_role, logs_channel, staff_role, welcome_channel, invite_code) VALUES ($1, $2, $3, $4, $5, $6, $7)", str(ctx.guild.id), str(bots_role.id), str(needed_bots_role.id), str(logs_channel.id), str(hs_role.id), str(welcome_channel.id), invite.code)
            await ctx.send("Cache server added to database")
        else:
            msg = "The following bots have not been added to the server yet:\n"
            
            for b in not_yet_added:
                invite = b.invite.replace("{id}", str(b.id)).replace("{perms}", "8")
                msg += f"\n- {b.name}: [{invite}]\n"
            
            msg += "\nPlease add these bots to the server and run the command again"
            return await ctx.send(msg)

loop = asyncio.get_event_loop()
loop.run_until_complete(bot.run())
