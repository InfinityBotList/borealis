import discord
from discord.ext import commands
from pydantic import BaseModel
from ruamel.yaml import YAML
import logging
import asyncpg
import asyncio

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

@bot.hybrid_command()
async def db_test(ctx: commands.Context):
    await bot.pool.acquire()
    await ctx.send("Acquired connection")

@bot.hybrid_command()
async def kittycat_test(ctx: commands.Context):
    ...

loop = asyncio.get_event_loop()
loop.run_until_complete(bot.run())