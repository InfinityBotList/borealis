-- Servers
CREATE TABLE public.cache_servers (
    guild_id text NOT NULL,
    name text NOT NULL,
    bots_role text NOT NULL,
    system_bots_role text NOT NULL,
    welcome_channel text not null,
    invite_code text NOT NULL,
    logs_channel text NOT NULL,
    staff_role text NOT NULL,
    created_at timestamptz not null default now()
);

ALTER TABLE ONLY public.cache_servers ADD CONSTRAINT cache_servers_pkey PRIMARY KEY (guild_id);

-- Bots
CREATE TABLE public.cache_server_bots (
    guild_id text NOT NULL REFERENCES cache_servers(guild_id) ON UPDATE CASCADE ON DELETE CASCADE,
    bot_id text NOT NULL UNIQUE REFERENCES bots(bot_id) ON UPDATE CASCADE ON DELETE CASCADE,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    added integer DEFAULT 0 NOT NULL
);

CREATE TABLE cache_server_oauths (
    user_id text NOT NULL UNIQUE,
    access_token text NOT NULL,
    refresh_token text NOT NULL,
    expires_at timestamptz NOT NULL
);

create table cache_server_oauth_md (
    owner_id text not null references cache_server_oauths(user_id) on update restrict on delete cascade,
);