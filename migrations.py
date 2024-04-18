from asyncpg import Pool
from discord.ext.commands import Context
import discord
import asyncpg
import asyncio
from constants import BOTS_ROLE_PERMS

class Migration():
    """Represents a cache server migration"""
    def __init__(self, pool: Pool, ctx: Context):
        self.pool = pool
        self.ctx = ctx
    
    def id(self) -> str:
        raise NotImplementedError

    async def database_migration_func(self):
        raise NotImplementedError
    
    async def run_one(self, guild: discord.Guild):
        raise NotImplementedError

    async def rollback(self, guild: discord.Guild):
        raise NotImplementedError

    async def finish_rollback(self):
        raise NotImplementedError

    async def finish(self):
        raise NotImplementedError

class TestMigration(Migration):
    async def database_migration_func(self):
        pass

    async def run_one(self, guild: discord.Guild):
        pass

    async def rollback(self, guild: discord.Guild):
        pass

    async def finish_rollback(self):
        pass

    async def finish(self):
        pass

    def id(self) -> str:
        return "test_migration"

class StaffRoleSeperation(Migration):
    """
        Create a new web_moderator_role above bots with limited permissions
    """
    async def database_migration_func(self):
        try:
            await self.pool.execute("ALTER TABLE cache_servers ADD COLUMN web_moderator_role TEXT")
        except asyncpg.exceptions.DuplicateColumnError:
            pass

    async def run_one(self, guild: discord.Guild):
        # Delete older roles
        roles = [r for r in guild.roles if r.name == "Web Moderator" or r.name == "Bots" or r.name == "Staff Moderator"]

        for r in roles:
            try:
                await r.delete()
            except discord.NotFound:
                pass

        bots_role = await self.pool.fetchval("SELECT bots_role FROM cache_servers WHERE guild_id = $1", str(guild.id))
        bot_role = guild.get_role(int(bots_role))
        
        if bot_role:
            # Delete it and remake it
            try:
                await bot_role.delete()
            except discord.NotFound:
                pass

        sm = await guild.create_role(
            name="Web Moderator", 
            permissions=discord.Permissions(
                manage_guild=True,
                kick_members=True,
                ban_members=True,
                moderate_members=True,
            ), 
            color=discord.Color.green(),
            hoist=True, 
            mentionable=True
        )

        # Make new bots role, this will be below the web moderator role anyways
        await asyncio.sleep(1)
        bots_role = await guild.create_role(
            name="Bots",
            permissions=BOTS_ROLE_PERMS,
            color=discord.Color.brand_red(),
            hoist=True,
            mentionable=True
        )

        await self.pool.execute("UPDATE cache_servers SET web_moderator_role = $1, bots_role = $2 WHERE guild_id = $3", str(sm.id), str(bots_role.id), str(guild.id))
        await self.ctx.send("NOTICE: Critical role changes have been made, Restart the bot after successful migration")

    async def finish(self):
        await self.pool.execute("ALTER TABLE cache_servers ALTER COLUMN web_moderator_role SET NOT NULL")

    async def rollback(self, guild: discord.Guild):
        web_mod_role = await self.pool.fetchval("SELECT web_moderator_role FROM cache_servers WHERE guild_id = $1", str(guild.id))

        if not web_mod_role:
            return
        
        role = guild.get_role(int(web_mod_role))

        if role:
            await role.delete()
        
        # Find all roles named Web Moderator
        roles = [r for r in guild.roles if r.name == "Web Moderator"]

        for r in roles:
            await r.delete()

        await self.pool.execute("UPDATE cache_servers SET web_moderator_role = '' WHERE guild_id = $1", str(guild.id))
    
    async def finish_rollback(self):
        await self.pool.execute("ALTER TABLE cache_servers DROP COLUMN web_moderator_role")
    

    def id(self) -> str:
        return "staff_role_seperation"

MIGRATION_LIST: list[Migration] = [
    TestMigration,
    StaffRoleSeperation
]