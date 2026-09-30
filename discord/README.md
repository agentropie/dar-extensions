# discord

Discord extension for dar. DMs are accepted as before. Guild messages require an @mention by default, are stripped before forwarding, and each guild channel keeps an isolated session. Threads inherit their parent channel's addressing configuration, reply in the thread, and keep a separate session; after an accepted mention, follow-ups in that thread continue without another mention. Webhook messages and the bot's own messages are always ignored; other bots are ignored unless `allow_bots` permits them (see [Other bots](#other-bots-allow_bots)).

## Install

Copy this directory to `<agent>/extensions/discord`, then run `dar build --dir .` and `dar run --dir .`.

## Configure

```yaml
extensions:
  discord:
    bot_token: "Discord bot token"
    ack_emoji: "👀" # optional immediate acknowledgement
    history_limit: 20 # recent prior messages included with each accepted turn; 0 keeps all buffered (max 50)
    fetch_history: true # backfill recent channel messages from Discord on first use after restart
    allow_bots: false # true = any other bot may trigger the agent; or a list of bot user IDs
    clear_history_after_reply: false # set true to discard that channel/thread history after a successful reply
    sessions:
      idle_minutes: 360 # lazy expiry on next accepted turn; 0 disables
    # backend: pi # optional cap-chat backend override
    guilds:
      "guild-id":
        users: ["allowed-user-id"] # empty allows every user
        channels:
          "channel-id":
            require_mention: true # default
            # enabled: false
            # users: ["allowed-user-id"]
```

`DISCORD_BOT_TOKEN` is used when `bot_token` is omitted. Guilds and channels are deny-by-default: both IDs must be configured and enabled. Empty user allowlists allow every user; populated guild and channel allowlists must both include the sender. Enable the **Message Content Intent** and guild-message intent for the bot in Discord's developer portal.

Every accepted message is immediately acknowledged with `ack_emoji` (default `👀`). Image and file attachments are downloaded to `data/uploads` and their local paths are supplied to the agent; attachment-only messages are accepted too. Files over 25 MiB or failed downloads produce a visible error. Agent failures, a 60-second queue/no-output timeout, and failed reply delivery are surfaced with a visible error; Discord post attempts are retried three times, then the source message receives a `⚠️` reaction if an error message cannot be posted.

The gateway reconnects automatically after a disconnect, retrying after 1, 2, 4, 8, 16, then 30 seconds (maximum). A reconnect starts a fresh gateway session; messages sent while it was disconnected are not replayed and will not receive a delayed reply. On shutdown the gateway sends a close frame and all active agent turns are cancelled and awaited.

Recent human and other-bot messages are kept in memory per channel or thread (and per DM), including messages sent before the bot is mentioned. By default the most recent 20 prior messages are supplied as explicitly untrusted context and history is retained after replies. `history_limit: 0` uses all retained messages; the in-memory buffer is capped at 50 messages. Set `clear_history_after_reply: true` to clear that conversation's buffer only after a reply is delivered successfully; `/reset` also clears it. History is in memory; with `fetch_history: true` (default) the first accepted message in each conversation after a restart backfills up to `history_limit` (max 50) prior messages from the Discord API. Set `fetch_history: false` to start empty after restarts.

## Other bots (`allow_bots`)

`allow_bots` (top level only, no per-channel override) lets other bots trigger the agent, e.g. for agent-to-agent chat: `false` (default) ignores all bot authors, `true` accepts any bot, and a list accepts only those Discord bot user IDs.

- Bots must always @mention this bot (a pinging reply counts), even in channels with `require_mention: false`, so two bots cannot chatter endlessly.
- The bot's own messages and webhook messages never trigger a turn.
- Other bots' messages are always recorded in channel history (labelled `Name (<@id>, bot)`), even when `allow_bots` is off, so a later human mention has that context.
- Bot-triggered turns carry `sender = discord:<bot user id>` into dar's agent loop guard (`agent_loop:` in `agent.yaml`). The guard counts consecutive bot turns per channel/thread across restarts of the chat session; a human message resets it. Blocked turns post nothing.
- Reading other bots' message text requires the **Message Content Intent**.

## Silent turns

When the agent replies `NO_REPLY` (or the loop guard blocks a turn) nothing is posted, not even `(no response)`, and the acknowledgement reaction is removed. Discord cannot cancel a typing indicator, so it may linger for a few seconds.

## Agent tool

`discord_send_message` lets the agent proactively post `text` to exactly one target: a configured `channel` (its name or ID), or a numeric Discord `user` ID. User targets automatically open a DM channel. Channel names are resolved only among the configured channel IDs; ambiguous names require an ID.

`discord_list_users` lists members of the configured guilds as `{id, name, guildId, bot?}`, bots included (and this bot itself), with an optional case-insensitive `query` name filter and `limit` (default 100). It requires the **Server Members Intent** in the Discord developer portal; without it the tool returns a `missing_members_intent` error.

## Authors and mentions

The agent sees every author as `Name (<@id>)` or `Name (<@id>, bot)`, both for the current message (`From …:`) and for history lines, and is told to write `<@USER_ID>` to mention someone: a bare name notifies nobody, and bots only respond when mentioned.

## Commands

Sessions rotate append-only after 360 idle minutes of accepted agent-turn activity by default (`sessions.idle_minutes`; `0` disables); unaddressed history traffic does not refresh TTL. Rotation retains old generation data on disk but makes it inactive, clears buffered history and thread engagement, and posts `Previous session expired; starting fresh.` on next accepted message. Existing session layouts resume without forced rotation. `/reset` (or `/new`) clears the current channel or DM session; the next message starts fresh. `/abort` (or `/stop`) cancels the active response in that channel or DM. Both commands post a confirmation even when there is no existing session or active response.
