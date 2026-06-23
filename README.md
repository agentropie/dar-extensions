# telegram

A standalone agentropy/dar extension that makes an agent reachable for chat over a Telegram bot. It mirrors the channel pattern from nousresearch/hermes-agent (token from BotFather, long-poll updates, route to the agent, reply) but is implemented as a lean Rust `Extension` that drives the host's `cap-chat` `ChatBackend` — the same backend the TUI uses.

## How it works

- Long-polls Telegram `getUpdates` with a 30-second server-side timeout.
- For each text message, opens (or reuses) a `ChatBackend` chat session keyed by Telegram chat id, sends the message as a turn, accumulates the assistant `Delta` events until `TurnFinished`, and replies with `sendMessage` (chunked at 4096 chars if needed).
- Model/provider come from the orchestrator's `RunSnapshot` when linked; otherwise the backend defaults apply.
- One session per chat = independent conversation context, persisted under `<agent>/data/telegram/sessions/<chat_id>/`.

## Install

1. Get a bot token from @BotFather.
2. Copy this `telegram/` directory into your agent folder's `extensions/` directory (e.g. `my-agent/extensions/telegram/`).
3. Configure (see below).
4. Run `agentropy build --dir .` then `agentropy run` (or for the monolith build, add it to `dist`).

> **Note:** The git `rev` pin in Cargo.toml must match the dar version your agent composes against.

## Configure

```yaml
extensions:
  telegram:
    # bot_token can be omitted here and supplied via TELEGRAM_BOT_TOKEN in .env instead
    bot_token: "123456:ABC-DEF..."
    # optional: restrict to specific Telegram numeric user ids (empty/omitted = anyone)
    allowed_users: [12345678]
    # optional: cap-chat backend service id to drive. Omit to auto-follow the
    # orchestrator's selected runner (falling back to "pi"); set to pin one.
    backend: pi
```

Alternatively, put `TELEGRAM_BOT_TOKEN=...` in the agent's `.env`. Get your numeric user id from @userinfobot.

## Config reference

| key | type | default | meaning |
|-----|------|---------|---------|
| `bot_token` | string | none | BotFather token (or `TELEGRAM_BOT_TOKEN` env) |
| `allowed_users` | list of int | `[]` (everyone) | whitelist of Telegram user ids |
| `backend` | string | auto-follow orchestrator runner, else `"pi"` | cap-chat backend service id to chat with |

## Limitations

- Text messages only — no media, voice, or inline keyboards.
- Messages are processed sequentially: a long agent turn delays other users.
- Plain-text replies only — no Markdown formatting.
- Long-poll only — no webhook mode.
