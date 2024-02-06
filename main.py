import discord
from discord.ext import commands
from pydantic import BaseModel
from ruamel.yaml import YAML
import logging
import asyncpg
import asyncio
from kittycat.perms import get_user_staff_perms
from kittycat.kittycat import has_perm
import secrets

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

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name}#{bot.user.discriminator} ({bot.user.id}), scanning servers")

    for guild in bot.guilds:
        if guild.id in bot.config.pinned_servers:
            continue

        count = await bot.pool.fetchval("SELECT COUNT(*) from cache_servers WHERE guild_id = $1", str(guild.id))

        if count > 0:
            print(f"Found cache server: {guild.name} ({guild.id})")
            continue

        print(f"ALERT: Found unknown server {guild.name} ({guild.id}), leaving/deleting")

        try:
            if guild.owner_id == bot.user.id:
                print(f"ALERT: Guild owner is bot, deleting guild")
                #await guild.delete()
            else:
                print(f"ALERT: Guild owner is not bot, leaving guild")
                #await guild.leave()
        except discord.HTTPException:
            print(f"ALERT: Failed to leave/delete guild {guild.name} ({guild.id})")

# Error handler
@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return

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
async def make_cache_server(
    ctx: commands.Context, 
    is_cache_server: bool | None = commands.parameter(default=False, description="Whether the server should be setup as a cache server or not")
):
    """Creates a cache server"""
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if not has_perm(resolved, "borealis.make_cache_servers"):
        return await ctx.send("You need ``borealis.make_cache_servers`` permission to use this command!")

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
        needed_bots = list(map(lambda b: b.id, bot.config.needed_bots))
        needed_bots_members = []
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

            logs_category = await ctx.guild.create_category('Logging')

            logs_channel = await logs_category.create_text_channel('system-logs')

            await bot.pool.execute("INSERT INTO cache_servers (guild_id, bots_role, system_bots_role, logs_channel, staff_role) VALUES ($1, $2, $3, $4, $5)", str(ctx.guild.id), str(bots_role.id), str(needed_bots_role.id), str(logs_channel.id), str(hs_role.id))
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
