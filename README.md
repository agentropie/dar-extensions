# irc

A standalone agentropy/dar extension that makes an agent reachable over IRC — primarily in **shared team channels holding both humans and multiple agents**, and over private DMs. It mirrors the `telegram` extension's channel pattern (a background `Extension` driving the host's `cap-chat` `ChatBackend`, bundling the stock `pi` backend, one persistent session per conversation) but speaks the raw IRC socket protocol over TCP/TLS instead of an HTTP bot API.

## How it works

- Connects to an IRC server over TCP (TLS by default), registers (`PASS`/`NICK`/`USER`), optionally `IDENTIFY`s to NickServ, retries the nick with a suffix on a `433` collision, and `JOIN`s the configured channels. `PING` is answered transparently; a dropped link auto-reconnects with exponential backoff.
- **Mention-gating in channels:** configurable per-channel (default: true). When enabled the agent replies only when addressed by nick (`nick: ...` or `nick, ...`, case-insensitive); when disabled it replies to every channel message. Unaddressed channel traffic is still ingested as bounded ambient context. DMs are always answered.
- **Bot-to-bot loop guard:** a hard per-channel cap on consecutive bot-authored turns with no intervening human message. Once the cap is hit the agent stays silent in that channel until a human speaks again. Per-channel isolated; DMs are never gated. This is the non-negotiable backstop against runaway token cost in agent-to-agent exchanges. The guard is **fail-closed**: senders not on the `humans` list always count as bots, so with an empty `humans` list (the default) every sender — including real people — counts toward the cap. Operators who want uncapped human-driven exchanges must list their humans explicitly in `humans`.
- For each addressed message, opens (or reuses) a `ChatBackend` session keyed by conversation (channel name, or sender nick for DMs), sends the turn, accumulates the assistant `Delta` events until `TurnFinished`, then markdown-strips and splits the reply into IRC-safe lines (~450 chars / 400 bytes, on word boundaries, multibyte-safe) and sends them paced to avoid flood kicks.
- CTCP `ACTION` (`/me`) messages are normalized to plain text context.
- Model/provider come from the orchestrator's `RunSnapshot` when linked; otherwise the backend defaults apply.
- One session per conversation = independent context, persisted under `<agent>/data/irc/sessions/<conv>/`.
- Bundles the stock `pi` chat backend under its own id (`irc-pi`), so the channel works under the default `foreground: logs` without requiring `foreground: tui`. When the orchestrator runner's backend is registered as a `dyn ChatBackend` it is used instead; `irc-pi` is the final fallback.

## Install

1. Copy this `irc/` directory into your agent folder's `extensions/` directory (e.g. `my-agent/extensions/irc/`).
2. Configure (see below).
3. Run `agentropy build --dir .` then `agentropy run` (or for the monolith build, add it to `dist`).

> **Note:** The git `rev` pin in Cargo.toml must match the dar version your agent composes against.

## Configure

```yaml
extensions:
  irc:
    server: irc.libera.chat
    port: 6697          # optional, default 6697
    tls: true           # optional, default true
    nick: darbot
    # username/realname optional; default to nick
    # server_password can be omitted here and supplied via IRC_SERVER_PASSWORD
    # nickserv_password supplied via IRC_NICKSERV_PASSWORD keeps it out of yaml
    channels: ["#team", "#agents"]   # list form: each channel inherits mention_required default
    # or map form for per-channel control:
    # channels:
    #   "#team": {}                       # inherits global mention_required
    #   "#public":
    #     mention_required: false         # always engage, even without a mention
    mention_required: true          # optional, default true; per-channel overrides this
    # optional DM nick allowlist (case-insensitive, empty/omitted = anyone)
    allowed_users: ["alice", "bob"]
    # optional channel human nicks for the loop-guard: senders NOT listed count
    # as bots toward the cap. Empty/omitted = no known humans (fail-closed: the
    # cap applies to every channel sender).
    humans: ["alice", "bob"]
    max_bot_turns: 4    # hard cap on consecutive bot-to-bot turns per channel
    context_window: 30  # ambient messages retained per conversation
    # optional: pin a cap-chat backend service id. Omit to auto-follow the
    # orchestrator runner backend (only registered under `foreground: tui`),
    # else use the bundled `irc-pi`. An unregistered id falls back to `irc-pi`.
    # backend: irc-pi
```

Every field falls back to an `IRC_*` environment variable, so secrets can live in the agent's `.env` instead of `agent.yaml`:

```
IRC_SERVER=irc.libera.chat
IRC_NICK=darbot
IRC_NICKSERV_PASSWORD=...
IRC_SERVER_PASSWORD=...
IRC_CHANNELS=#team,#agents
IRC_ALLOWED_USERS=alice,bob
IRC_HUMANS=alice,bob
```

## Config reference

| key | type | default | meaning |
|-----|------|---------|---------|
| `server` | string | none (required) | IRC server hostname (or `IRC_SERVER`) |
| `port` | int | `6697` | server port (or `IRC_PORT`) |
| `tls` | bool | `true` | connect over TLS (or `IRC_TLS`) |
| `nick` | string | none (required) | desired nick (or `IRC_NICK`) |
| `username` | string | nick | USER username (or `IRC_USERNAME`) |
| `realname` | string | nick | USER realname (or `IRC_REALNAME`) |
| `server_password` | string | none | server `PASS` (or `IRC_SERVER_PASSWORD`) |
| `nickserv_password` | string | none | NickServ `IDENTIFY` password (or `IRC_NICKSERV_PASSWORD`) |
| `channels` | list of strings **or** map `channel → {mention_required}` | `[]` | channels to join; list form inherits global default, map form allows per-channel `mention_required` (or `IRC_CHANNELS`, comma-separated) |
| `mention_required` | bool | `true` | global default for mention-gating; per-channel value overrides (or `IRC_MENTION_REQUIRED`) |
| `allowed_users` | list of string | `[]` (everyone) | DM nick allowlist, case-insensitive (or `IRC_ALLOWED_USERS`) |
| `humans` | list of string | `[]` (none) | channel human nicks for the loop-guard; unlisted senders count as bots toward the cap (or `IRC_HUMANS`) |
| `backend` | string | auto-follow runner, else `irc-pi` | cap-chat backend service id; unregistered id falls back to `irc-pi` |
| `max_bot_turns` | int | `4` | hard cap on consecutive bot-to-bot turns per channel before going silent |
| `context_window` | int | `30` | ambient (context-only) messages retained per conversation |

The loop-guard classifies channel senders using the `humans` list, which is **independent** of `allowed_users` (a DM-only authorization gate). Any channel sender **not** on `humans` counts as a bot toward the consecutive-bot-turn cap. With an empty `humans` list (the default) there are no known humans, so the cap is fail-closed: it applies to every channel sender and can never be silently disabled. List your channel humans explicitly to let human-driven exchanges run uncapped (a human message resets the per-channel counter).

## Limitations

- Text messages only — no DCC, file transfer, or CTCP beyond `ACTION`-as-text.
- Plain-text replies only — markdown is stripped, no IRC colors/formatting.
- Messages are processed sequentially: a long agent turn delays other users.
- Trusted-network trust model: presence in a channel is authorization; bare-nick trust is assumed and nick spoofing is out of scope (the DM allowlist is a convenience, not a security boundary).
- One dar agent per bot process — "multiple agents in a room" means several bot processes joined to the same channel, not one process fanning out.
