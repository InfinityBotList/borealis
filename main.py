import discord
from discord.ext import commands
from pydantic import BaseModel
from ruamel.yaml import YAML
import logging
import asyncpg
import asyncio
from kittycat.perms import get_user_staff_perms

logging.basicConfig(level=logging.INFO)

class Config(BaseModel):
    token: str
    postgres_url: str

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
    print(f"Logged in as {bot.user.name}#{bot.user.discriminator} ({bot.user.id})")
    pool = asyncpg.pool.create_pool(config.postgres_url)

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
async def kittycat_test(
    ctx: commands.Context, 
    only_show_resolved: bool | None = commands.parameter(default=True, description="Whether to only show resolved permissions or not")
):
    usp = await get_user_staff_perms(bot.pool, ctx.author.id)
    resolved = usp.resolve()

    if only_show_resolved:
        await ctx.send(f"**Resolved**: ``{' | '.join(resolved)}``")
    else:
        await ctx.send(f"**Positions:** {[f'{usp.id} [{usp.index}]' for usp in usp.user_positions]} with overrides: {usp.perm_overrides}\n\n**Resolved**: ``{' | '.join(resolved)}``")

loop = asyncio.get_event_loop()
loop.run_until_complete(bot.run())