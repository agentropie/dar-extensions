# telegram

A standalone dar extension that makes an agent reachable for chat over a Telegram bot. It mirrors the channel pattern from nousresearch/hermes-agent (token from BotFather, long-poll updates, route to the agent, reply) but is implemented as a lean Rust `Extension` that drives the host's `cap-chat` `ChatBackend` — the same backend the TUI uses.

## How it works

- Long-polls Telegram `getUpdates` with a 30-second server-side timeout.
- Each chat generation has a worker that continuously reads its backend events, including proactive replies after background work; delivery does not wait for another incoming message. Polling and other chats continue while a turn runs.
- Up to 16 messages per chat can be pending; further messages receive a busy notice and must be retried. Reset and expiry close the old worker before announcing the replacement.
- Streams the reply live while the turn runs: the first assistant text creates one **answer bubble** that is then edited in place (rate-limited to ~1s / 200 chars) instead of spamming a message per token. A separate **tool-status bubble** shows the tool currently running as `name · short target` (e.g. `read · /etc/hosts`), never the full argument JSON. At each tool boundary the visible answer text is flushed first, so a long or stuck tool run can't hide text the assistant already produced. On turn finish the status bubble collapses to a summary like `Used 3 tools: read, bash, edit`, and the answer bubble is finalized with the rich-markdown reply (falling back to a chunked `sendMessage` for multi-part replies, still capped at 4096 chars). Streaming is UI-only and never alters the agent's conversation history; a failed edit falls back to a fresh send so the final answer always lands.
- Model/provider come from the orchestrator's `RunSnapshot` when linked; otherwise the backend defaults apply.
- One session per chat = independent conversation context, persisted append-only by generation under `<agent>/data/telegram/sessions/<chat_id>/<generation_id>/`, with a `current.json` pointer tracking the live generation and last inbound time.
- Idle expiry: after `sessions.idle_minutes` of no inbound messages (default 360; `0` disables), the next message rotates to a fresh generation and the user first sees `Previous session expired; starting fresh.` before the reply. Old generation directories are kept for audit/debug.
- `/new` and `/reset` (also `/new@bot` / `/reset@bot` for group chats) start a fresh session and reply `Context cleared, new session started.` without running the agent. Only the exact command token resets — `/new please` is treated as a normal message.
- Declares `requires_stock = ["chat-pi"]` in `Cargo.toml`, so the composer links the stock `pi` chat backend into the agent binary and the channel works under the default `foreground: logs` without requiring `foreground: tui`. Backend selection: `extensions.telegram.backend` if set, else the orchestrator's `runner.use` when that id is registered as a `dyn ChatBackend`, else `pi`.

## Install

1. Get a bot token from @BotFather.
2. Copy this `telegram/` directory into your agent folder's `extensions/` directory (e.g. `my-agent/extensions/telegram/`).
3. Configure (see below).
4. Run `dar build --dir .` then `dar run` (or for the monolith build, add it to `dist`).

> **Note:** The git `rev` pin in Cargo.toml must match the dar version your agent composes against.

## Configure

```yaml
extensions:
  telegram:
    # bot_token can be omitted here and supplied via TELEGRAM_BOT_TOKEN in .env instead
    bot_token: "123456:ABC-DEF..."
    # optional: restrict to specific Telegram numeric user ids (empty/omitted = anyone)
    allowed_users: [12345678]
    # optional: pin a cap-chat backend service id to drive. Omit to auto-follow
    # the orchestrator's runner backend when it is registered as a chat backend,
    # else use the stock `pi` backend. A configured id must be registered:
    # an unknown id fails the session open with "chat backend '<id>' not registered".
    # backend: pi
    # optional: Telegram session lifecycle
    sessions:
      # idle minutes before a chat's context expires; 0 disables idle expiry
      idle_minutes: 360
```

Alternatively, put `TELEGRAM_BOT_TOKEN=...` in the agent's `.env`. Get your numeric user id from @userinfobot.

## Config reference

| key | type | default | meaning |
|-----|------|---------|---------|
| `bot_token` | string | none | BotFather token (or `TELEGRAM_BOT_TOKEN` env) |
| `allowed_users` | list of int | `[]` (everyone) | whitelist of Telegram user ids |
| `backend` | string | auto-follow orchestrator runner, else `pi` | cap-chat backend service id to drive; a configured-but-unregistered id errors at session open |
| `sessions.idle_minutes` | int | `360` | idle minutes before a chat's context expires and rotates to a fresh generation; `0` disables idle expiry |

## Limitations

- Text messages only — no media, voice, or inline keyboards.
- Each chat processes turns in order; long turns do not block other chats.
- Long-poll only — no webhook mode.

## SDK dependency

Telegram requires `dar-extension-sdk` 0.5 and a Dar build with the matching Pi backend changes. The SDK resolves from crates.io; no local Cargo override is required:

```sh
cargo test --manifest-path telegram/Cargo.toml --locked
```

Run from the `dar-extensions` root.
