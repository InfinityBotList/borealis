import fastapi
from fastapi import HTTPException, Request
from pydantic import BaseModel
from main import config, bot, MAX_PER_CACHE_SERVER

app = fastapi.FastAPI()

async def check_internal(request: Request):
    print(request.headers)
    if request.headers.get("X-Forwarded-For"):
        raise HTTPException(status_code=403, detail="Forbidden")
    
@app.get("/getCacheServerOfBot")
async def get_cache_server_of_bot(request: Request, bot_id: str):
    """Returns the cache server of a bot"""
    cache_server = await bot.pool.fetchval("SELECT guild_id FROM cache_server_bots WHERE bot_id = $1", bot_id)

    if cache_server is None:
        raise HTTPException(status_code=404, detail="Bot not found in any cache server")
    
    data = await bot.pool.fetchrow("SELECT invite_code FROM cache_servers WHERE guild_id = $1", cache_server)
    
    guild = bot.get_guild(int(cache_server))
    if guild is None:
        raise HTTPException(status_code=500, detail="Cache server not found despite existing in database")

    member = guild.get_member(int(bot_id))

    return {"guild_id": cache_server, "invite_code": data["invite_code"], "member": member is not None}

class AddBotToCacheServer(BaseModel):
    guild_id: str
    name: str
    invite_code: str
    added: bool

@app.post("/addBotToCacheServer", response_model=AddBotToCacheServer)
async def add_bot_to_cache_server(request: Request, bot_id: str, ignore_bot_type: bool):
    """Adds a bot to a cache server. Internal-only"""
    await check_internal(request)

    # Check if bot is approved/certified
    if not ignore_bot_type:
        typ = await bot.pool.fetchval("SELECT type FROM bots WHERE bot_id = $1", str(bot_id))

        if typ is None:
            raise HTTPException(status_code=404, detail="Bot not found")
        
        if typ not in ["approved", "certified"]:
            raise HTTPException(status_code=403, detail="Bot not approved/certified")
    
    # Check if bot is already in a cache server
    cache_server = await bot.pool.fetchrow("SELECT guild_id, name FROM cache_server_bots WHERE bot_id = $1", bot_id)

    if cache_server is not None:
        # Return invite code
        data = await bot.pool.fetchrow("SELECT invite_code FROM cache_servers WHERE guild_id = $1", cache_server)
        return {
            "guild_id": cache_server["guild_id"], 
            "name": cache_server["name"], 
            "invite_code": data["invite_code"], 
            "added": False
        }

    # Find a cache server with less than MAX_PER_CACHE_SERVER bots
    available = await bot.pool.fetch("select guild_id, count(*) from cache_server_bots group by guild_id")

    guild_id: str = None
    for data in available:
        if data["count"] < MAX_PER_CACHE_SERVER:
            guild_id = data["guild_id"]
            break
    
    if guild_id is None:
        raise HTTPException(status_code=500, detail="No available cache servers")

    # Add bot to cache server
    await bot.pool.execute("INSERT INTO cache_server_bots (guild_id, bot_id) VALUES ($1, $2)", guild_id, bot_id)

    data = await bot.pool.fetchrow("SELECT invite_code FROM cache_servers WHERE guild_id = $1", guild_id)

    return {"guild_id": guild_id, "invite_code": data["invite_code"], "added": True}