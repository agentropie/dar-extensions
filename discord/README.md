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

Every accepted message is immediately acknowledged with `ack_emoji` (default `👀`). Agent failures, a 60-second queue/no-output timeout, and failed reply delivery are surfaced with a visible error; Discord post attempts are retried three times, then the source message receives a `⚠️` reaction if an error message cannot be posted.
