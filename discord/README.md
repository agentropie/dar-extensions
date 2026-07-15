# discord

Discord extension for dar. DMs are accepted as before. Guild messages require an @mention by default, are stripped before forwarding, and each guild channel (including a thread) keeps an isolated session. Bot and webhook messages are ignored.

## Install

Copy this directory to `<agent>/extensions/discord`, then run `dar build --dir .` and `dar run --dir .`.

## Configure

```yaml
extensions:
  discord:
    bot_token: "Discord bot token"
    ack_emoji: "👀" # optional immediate acknowledgement
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

## Commands

`/reset` (or `/new`) clears the current channel or DM session; the next message starts fresh. `/abort` (or `/stop`) cancels the active response in that channel or DM. Both commands post a confirmation even when there is no existing session or active response.
