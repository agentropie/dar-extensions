# discord

Discord extension for dar. DMs are accepted as before. Guild messages require an @mention by default, are stripped before forwarding, and each guild channel keeps an isolated session. Threads inherit their parent channel's addressing configuration, reply in the thread, and keep a separate session; after an accepted mention, follow-ups in that thread continue without another mention. Bot and webhook messages are ignored.

## Install

Copy this directory to `<agent>/extensions/discord`, then run `dar build --dir .` and `dar run --dir .`.

## Configure

```yaml
extensions:
  discord:
    bot_token: "Discord bot token"
    ack_emoji: "👀" # optional immediate acknowledgement
    history_limit: 20 # recent prior messages included with each accepted turn; 0 keeps all buffered (max 50)
    clear_history_after_reply: false # set true to discard that channel/thread history after a successful reply
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

Recent human messages are kept in memory per channel or thread (and per DM), including messages sent before the bot is mentioned. By default the most recent 20 prior messages are supplied as explicitly untrusted context and history is retained after replies. `history_limit: 0` uses all retained messages; the in-memory buffer is capped at 50 messages. Set `clear_history_after_reply: true` to clear that conversation's buffer only after a reply is delivered successfully; `/reset` also clears it. History is lost when the extension restarts.

## Agent tool

`discord_send_message` lets the agent proactively post `text` to exactly one target: a configured `channel` (its name or ID), or a numeric Discord `user` ID. User targets automatically open a DM channel. Channel names are resolved only among the configured channel IDs; ambiguous names require an ID.

## Commands

`/reset` (or `/new`) clears the current channel or DM session; the next message starts fresh. `/abort` (or `/stop`) cancels the active response in that channel or DM. Both commands post a confirmation even when there is no existing session or active response.
