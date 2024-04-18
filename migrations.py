from asyncpg import Pool
from discord.ext.commands import Context
import discord

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
        Create a new staff_moderator role above bots with limited permissions
    """
    async def database_migration_func(self):
        await self.pool.execute("ALTER TABLE cache_servers ADD COLUMN staff_moderator_role TEXT")
    
    async def run_one(self, guild: discord.Guild):
        sm_role_current = await self.pool.fetchval("SELECT staff_moderator_role FROM cache_servers WHERE guild_id = $1", str(guild.id))

        if sm_role_current:
            return

        bots_role = await self.pool.fetchval("SELECT bots_role FROM cache_servers WHERE guild_id = $1", str(guild.id))
        bot_role = guild.get_role(int(bots_role))
        
        if not bot_role:
            raise Exception("Bot role not found")

        sm = await guild.create_role(
            name="Staff Moderator", 
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

        # Move above bots
        await sm.edit(position=bot_role.position - 1)
        await self.pool.execute("UPDATE cache_servers SET staff_moderator_role = $1 WHERE guild_id = $2", str(sm.id), str(guild.id))

    async def finish(self):
        await self.pool.execute("ALTER TABLE cache_servers ALTER COLUMN staff_moderator_role SET NOT NULL")

    async def rollback(self, guild: discord.Guild):
        web_mod_role = await self.pool.fetchval("SELECT staff_moderator_role FROM cache_servers WHERE guild_id = $1", str(guild.id))

        if not web_mod_role:
            return
        
        role = guild.get_role(int(web_mod_role))

        if role:
            await role.delete()

        await self.pool.execute("UPDATE cache_servers SET staff_moderator_role = '' WHERE guild_id = $1", str(guild.id))
    
    async def finish_rollback(self):
        await self.pool.execute("ALTER TABLE cache_servers ALTER COLUMN staff_moderator_role DROP NOT NULL")
    

    def id(self) -> str:
        return "staff_role_seperation"

MIGRATION_LIST: list[Migration] = [
    TestMigration,
    StaffRoleSeperation
]