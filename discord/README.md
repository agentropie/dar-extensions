# discord

Discord DM extension for dar. It connects a bot through Discord's Gateway and sends each DM through the configured chat backend; replies are posted as one message. Sessions are keyed by Discord user and persist in `data/discord/sessions/`.

## Install

Copy this directory to `<agent>/extensions/discord`, then run `dar build --dir .` and `dar run --dir .`.

## Configure

```yaml
extensions:
  discord:
    bot_token: "Discord bot token"
    # backend: pi # optional cap-chat backend override
```

`DISCORD_BOT_TOKEN` is used when `bot_token` is omitted. Enable the **Message Content Intent** for the bot in Discord's developer portal; the extension requests the Direct Messages gateway intent.
