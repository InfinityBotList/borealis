import fastapi
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
import secrets
import datetime
from kittycat import has_perm, Permission
from perms import get_user_staff_perms
from main import config, bot, MAX_PER_CACHE_SERVER, handle_member

app = fastapi.FastAPI()

async def check_internal(request: Request):
    print(request.headers)
    if request.headers.get("X-Forwarded-For"):
        raise HTTPException(status_code=403, detail="Forbidden")

@app.post("/handleBotOnAllCacheServers")
async def handle_bot_on_all_cache_servers(request: Request, bot_id: int):
    """Handles the user on all cache servers they are on. Internal only"""
    await check_internal(request)

    cache_server = await bot.pool.fetchval("SELECT guild_id FROM cache_server_bots WHERE bot_id = $1", str(bot_id))

    if cache_server is None:
        raise HTTPException(status_code=404, detail="Bot not found in any cache server")
    
    cache_server_info = await bot.pool.fetchrow("SELECT bots_role, system_bots_role, logs_channel, staff_role from cache_servers WHERE guild_id = $1", str(member.guild.id))

    if not cache_server_info:
        raise HTTPException(status_code=500, detail="Cache server not found despite existing in database")

    member = bot.get_guild(int(cache_server)).get_member(bot_id)

    if member is None:
        raise HTTPException(status_code=500, detail="Bot not found in cache server")

    await handle_member(member, cache_server_info=cache_server_info)

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
    cs = await bot.pool.fetchval("SELECT guild_id FROM cache_server_bots WHERE bot_id = $1", bot_id)

    if cs is not None:
        # Return invite code
        cache_server = await bot.pool.fetchrow("SELECT invite_code, name FROM cache_servers WHERE guild_id = $1", cs)
        return {
            "guild_id": cs, 
            "name": cache_server["name"], 
            "invite_code": cache_server["invite_code"], 
            "added": False
        }

    # Find a cache server with less than MAX_PER_CACHE_SERVER bots
    available = await bot.pool.fetch("select guild_id, count(*) from cache_server_bots group by guild_id order by random()")

    guild_id: str = None
    for data in available:
        if data["count"] < MAX_PER_CACHE_SERVER:
            guild_id = data["guild_id"]
            break
    
    if guild_id is None:
        print("ERROR: No available cache servers")
        raise HTTPException(status_code=500, detail="No available cache servers")

    # Add bot to cache server
    await bot.pool.execute("INSERT INTO cache_server_bots (guild_id, bot_id) VALUES ($1, $2)", guild_id, bot_id)

    data = await bot.pool.fetchrow("SELECT name, invite_code FROM cache_servers WHERE guild_id = $1", guild_id)

    return {"guild_id": guild_id, "name": data["name"], "invite_code": data["invite_code"], "added": True}

_states = {}
@app.get("/oauth2")
async def oauth2(request: Request, code: str | None = None, error: str | None = None, state: str | None = None):
    """OAuth2 callback"""
    if error:
        return HTMLResponse(f"<h1>Error: {error}</h1>")
    
    if code is None:
        state = secrets.token_urlsafe(16)
        _states[state] = [datetime.datetime.now(), "borealis"]
        return RedirectResponse(f"https://discord.com/oauth2/authorize?client_id={config.borealis_client_id}&redirect_uri={config.base_url}/oauth2&response_type=code&scope=identify%20guilds.join&state={state}")

    if state not in _states:
        return HTMLResponse("<h1>Error: Invalid state</h1>")

    state_created_at, state_bot = _states[state]
    
    if (datetime.datetime.now() - state_created_at).total_seconds() > 60:
        # Remove state
        del _states[state]
        return HTMLResponse("<h1>Error: State expired</h1>")
    
    # Exchange code for token
    data = {
        "client_id": config.cache_server_maker.client_id if state_bot == "doxycycline" else config.borealis_client_id,
        "client_secret": config.cache_server_maker.client_secret if state_bot == "doxycycline" else config.borealis_client_secret,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": f"{config.base_url}/oauth2",
        "scope": "identify guilds.join"
    }

    async with bot.session.post("https://discord.com/api/v10/oauth2/token", data=data) as resp:
        del _states[state]
        if resp.status != 200:
            err = await resp.text()
            return HTMLResponse(f"<h1>Error: {resp.status}: {err}</h1>")
        
        data = await resp.json()

    # Get user info
    async with bot.session.get("https://discord.com/api/v10/users/@me", headers={"Authorization": f"Bearer {data['access_token']}"}) as resp:
        if resp.status != 200:
            err = await resp.text()
            return HTMLResponse(f"<h1>Error: {resp.status}: {err}</h1>")
        
        user = await resp.json()

        # Check user id
        id = int(user["id"])

        try:
            usp = await get_user_staff_perms(bot.pool, id)
        except Exception as e:
            return HTMLResponse(f"<h1>Error: {e}</h1>")

        try:
            usp = await get_user_staff_perms(bot.pool, id)
            resolved = usp.resolve()
        except:
            resolved = []
        
        if not resolved:
            return HTMLResponse("<h1>Error: You are not a staff member</h1>")
    
        # Add to db
        await bot.pool.execute("INSERT INTO cache_server_oauths (user_id, access_token, refresh_token, expires_at, bot) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (user_id, bot) DO UPDATE SET access_token = $2, refresh_token = $3, expires_at = $4", str(id), data["access_token"], data["refresh_token"], datetime.datetime.now() + datetime.timedelta(seconds=data["expires_in"]), state_bot)

    if has_perm(resolved, Permission.from_str("borealis.make_cache_servers")) and state_bot == "borealis":
        # Set new state to doxycycline and refresh back to /oauth2 with state param
        _states[state] = [datetime.datetime.now(), "doxycycline"]
        return RedirectResponse(f"https://discord.com/oauth2/authorize?client_id={config.cache_server_maker.client_id}&redirect_uri={config.base_url}/oauth2&response_type=code&scope=identify%20guilds.join&state={state}")

    return HTMLResponse("<h1>Success! You can now close this tab</h1>")
