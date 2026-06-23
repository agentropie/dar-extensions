# IRC channel extension for dar

A standalone agentropy/dar extension that makes an agent reachable over IRC —
primarily in **shared team channels holding both humans and multiple agents**,
and over private DMs. It mirrors the existing `telegram` extension's shape (a
background `Extension` driving the host's `cap-chat` `ChatBackend`, bundling the
stock `pi` backend, wiring the host-tool bridge, one session per conversation)
but speaks the raw IRC socket protocol instead of an HTTP bot API.

## Problem Statement

Today a dar agent can be reached 1:1 over Telegram. But the team's real
coordination surface is IRC channels where **multiple people and multiple agents
share a room**. There is no way to drop a dar agent into such a channel so it can
(a) be addressed by name, (b) follow the surrounding discussion, and
(c) collaborate with the *other* agents in the room — hand off, debate, build on
each other's replies. Telegram's 1:1 model cannot express any of this, and a
naive port would either flood the channel, respond to everything, or — worst —
let two agents ping-pong into unbounded token cost with no human present.

## Solution

A new `irc/` extension that connects to an IRC server over TCP/TLS, joins one or
more channels, and:

- **Mention-gating in channels:** acts only when its nick is addressed
  (`nick: ...` / `nick, ...`); always answers private DMs.
- **Ambient, capped context:** ingests *all* channel traffic (humans and other
  agents) as conversation context, bounded to a recent window, so it can
  collaborate intelligently — but it only *speaks* when mentioned.
- **Agent-to-agent collaboration:** other agents in the room are first-class
  participants it can converse with by nick. Agent-to-agent dialogue is the
  point, not a side effect.
- **Bounded exchanges:** agents are expected to self-terminate (judge when an
  exchange is resolved and go quiet), backed by a **hard mechanical cap** on
  consecutive bot-to-bot turns with no intervening human message, after which
  the agent stays silent until a human speaks again.
- **Trusted-network trust model:** presence in the channel is authorization; no
  per-identity gating is required (an optional nick allowlist exists for DMs as a
  convenience, mirroring telegram). IRC-nick spoofing is explicitly out of the
  threat model.

Replies stream from the `cap-chat` session, get markdown-stripped and split to
IRC's ~512-byte line limit, and are paced to avoid flooding.

## User Stories

1. As a team operator, I want to add a dar agent to a shared IRC channel, so that everyone in the room can reach it where they already coordinate.
2. As a channel member, I want the agent to reply only when I address it by nick, so that it doesn't butt into every message.
3. As a channel member, I want to DM the agent privately, so that I can ask it things without involving the whole room.
4. As an operator, I want the agent to join multiple channels at once, so that one agent process can serve several rooms.
5. As an operator, I want the agent to authenticate to NickServ, so that it holds a registered, stable nick on the network.
6. As an operator, I want the agent to recover its nick when taken (collision), so that startup doesn't fail if the nick is briefly in use.
7. As an operator, I want the agent to auto-reconnect after a dropped connection, so that it stays reliably present without manual restarts.
8. As a channel participant, I want the agent to have followed the recent discussion even in messages not addressed to it, so that when I tag it, its answer reflects the actual context.
9. As an agent author, I want my agent to address *another* agent in the channel by nick, so that the two can collaborate on a problem.
10. As an agent author, I want two agents to hand off, debate, and build on each other's replies, so that the swarm produces better outcomes than one agent alone.
11. As a cost-conscious operator, I want agent-to-agent exchanges to stop on their own when resolved, so that they don't run longer than useful.
12. As a cost-conscious operator, I want a hard cap on consecutive bot-to-bot turns with no human present, so that a bad self-termination judgment can never spiral token cost unattended.
13. As an operator, I want each channel and each DM to keep its own conversation context, so that threads don't bleed into one another.
14. As an operator, I want the ambient context window bounded, so that following a busy room doesn't blow up token cost.
15. As an operator, I want the agent to ignore its own messages, so that it never replies to itself.
16. As an operator, I want long replies split into IRC-safe lines and paced, so that the agent is a good channel citizen and isn't flagged as flooding.
17. As an operator, I want replies markdown-stripped, so that IRC users see clean plain text rather than `**asterisks**`.
18. As an operator, I want the agent to work under the default `foreground: logs` install, so that I don't have to run the TUI for the channel to function (mirroring telegram's bundled backend).
19. As an operator, I want model/provider to follow the orchestrator's `RunSnapshot` when linked, so that the IRC agent matches the configured runner.
20. As an operator, I want connection settings via config or env vars, so that I can keep secrets out of `agent.yaml`.
21. As an operator, I want an optional DM allowlist by nick, so that on a semi-open server I can still restrict who DMs the bot, even though channels are trusted.
22. As an operator, I want CTCP ACTION (`/me`) messages treated as plain text context, so that emotes don't break parsing.
23. As an operator, I want clear logs when the agent connects, joins, drops, reconnects, or hits the bot-to-bot cap, so that I can see what it's doing.

## Implementation Decisions

Mirror the telegram extension's architecture and conventions; diverge only where
IRC's protocol and multi-party model demand it.

- **Extension shell** — a background `host_api::Extension` (`id() == "irc"`),
  `register` ensures the bundled `pi` backend is registered under a unique self id
  (e.g. `irc-pi`) so it works under `foreground: logs`; `start` spawns the
  connection loop on a Tokio task and returns. Config parsing, backend resolution
  (`resolve_backend`: registered config override → orchestrator runner →
  bundled), and `open_session` (model/provider from `RunSnapshot`, host-tool
  bridge via `runner_core::host_tool_bridge`) are ported from telegram nearly
  verbatim.
- **IRC connection module (deep)** — owns the socket lifecycle behind a small
  interface: connect (TCP, optional TLS), register (`PASS`/`NICK`/`USER`),
  NickServ `IDENTIFY`, nick-collision retry (`433` → suffixing), `JOIN` of
  configured channels, `PING`/`PONG` keepalive, line read/parse into
  `(prefix, command, params)`, and reconnect-with-backoff. Exposes an inbound
  stream of parsed `PRIVMSG` events and an outbound `send(target, text)`. No agent
  logic lives here — testable in isolation against raw IRC lines.
- **Addressing module (deep)** — pure function over a parsed message + the bot's
  nick that classifies: is this a DM or a channel message; is the bot mentioned
  (leading `nick[:,] ` or configurable); who is the sender; is the sender the bot
  itself (ignore). Returns a verdict (`Reply` / `ContextOnly` / `Ignore`).
  Fully unit-testable, no I/O.
- **Loop-guard module (deep)** — the safety core. Tracks, per channel, the count
  of consecutive bot-authored turns since the last human message; resets on any
  human `PRIVMSG`. Exposes `should_respond(sender_is_bot, channel) -> bool`
  enforcing the hard cap. Combined with the agent's own self-termination, this is
  the non-negotiable guarantee against runaway cost. Pure, deterministic,
  exhaustively unit-testable — **the most important tested module.**
- **Session manager** — keyed by conversation id (channel name for channels,
  sender nick for DMs), one `cap-chat` session each, persisted under
  `<agent>/data/irc/sessions/<conv>/`, ported from telegram's `HashMap<…, ChatConn>`
  pattern. Ambient channel messages (verdict `ContextOnly`) are fed into the
  session as bounded context; only `Reply`-verdict messages trigger a turn and a
  channel reply.
- **Message splitting** — IRC line-limit splitter (markdown strip + ~450-char /
  512-byte chunks on word boundaries where possible) with inter-line pacing,
  replacing telegram's 4096-char char-splitter.
- **Config** — `IrcConfig`: `server`, `port` (default 6697), `tls` (default
  true), `nick`, `username`, `realname`, `server_password`, `nickserv_password`,
  `channels: Vec<String>`, `allowed_users: Vec<String>` (DM nick allowlist, empty
  = anyone), `backend: Option<String>`, `max_bot_turns` (hard cap, sane default),
  `context_window` (ambient cap). Each field falls back to an `IRC_*` env var.
- **Crate setup** — standalone Build B: own `[workspace]`, git-pinned dar deps at
  the same rev as telegram (`cap-chat`, `host-api`, `orchestrator-api`,
  `runner-core`, `chat-pi`, `tokio`, `serde`, `anyhow`), TLS via `tokio-rustls`
  (or `tokio-native-tls`), `[package.metadata.agentropy] factory = "irc::extension"`.
- **Single dar instance per bot process** — "multi-agent in a room" means several
  bot processes (each one dar agent) joined to the same channel, *not* one process
  fanning out. No in-process multi-agent orchestration.

## Testing Decisions

Good tests assert **external behavior** through the deep-module interfaces, not
internals — mirroring telegram's existing unit tests (`authorized`,
`split_message`). No live IRC server; feed modules raw lines / structured inputs.

- **Loop-guard (must test, highest priority):** consecutive bot turns hit the cap
  and suppress further responses; a human message resets the counter; per-channel
  isolation (one channel capping doesn't mute another); DM path unaffected. This
  is the non-negotiable guarantee and gets the most exhaustive coverage.
- **Addressing:** DM → `Reply`; channel without mention → `ContextOnly`; channel
  with `nick:` / `nick,` prefix → `Reply`; self-authored → `Ignore`; CTCP ACTION
  normalized to text. Table-driven over crafted `PRIVMSG` lines.
- **IRC parsing:** `_parse`-equivalent splits prefix/command/params correctly for
  `PRIVMSG`, `PING`, numeric replies (`001`, `433`); malformed lines don't panic.
- **Message splitting:** short text → one line; over-limit → multiple ≤-limit
  lines on word boundaries; markdown stripped; multibyte-safe (no mid-codepoint
  or mid-byte-limit breakage).
- **Backend resolution / auth gating:** ported telegram tests adapted to nicks
  (string allowlist) — empty allowlist permits anyone; populated gates by nick
  case-insensitively.

Prior art: `telegram/src/lib.rs` `#[cfg(test)] mod tests`.

## Out of Scope

- Hardened authentication / anti-spoofing — trusted-network assumption; bare-nick
  trust is acceptable, NickServ-account verification is not required.
- Per-identity tool restrictions (OpenClaw's `toolsBySender`) — everyone present
  is trusted.
- In-process multi-agent orchestration / turn-scheduling between agents — agents
  are separate processes; collaboration emerges from mention + ambient context.
- Non-text content (DCC, file transfer, CTCP beyond ACTION-as-text).
- Markdown/formatting/colors in replies (plain text only).
- Webhook/bouncer integration, channel moderation, op commands.
- Concurrent turn processing — sequential per process is acceptable (a long turn
  delaying others is a known limitation, as in telegram).

## Further Notes

- References studied: `nousresearch/hermes-agent` IRC adapter (asyncio raw-socket
  pattern: register → NickServ → join, 433 retry, mention-gating, 0.3s pacing,
  ~450-char split, per-channel/per-user sessions, auto-reconnect) and
  `docs.openclaw.ai/channels/irc` (multi-channel, NickServ + server password,
  `requireMention`, sender allowlists). This PRD adopts the raw-socket +
  mention-gate + reconnect pattern and adds the **ambient-context + bot-to-bot
  loop-guard** model that neither reference implements, because collaborative
  multi-agent rooms are the defining goal here.
- The non-negotiable, restated: **no runaway loops or unbounded cost.** If the
  loop-guard + self-termination can ever spiral token burn unattended, the
  extension has failed regardless of how good the collaboration is. Everything
  else ranks below this.
- Keep the git `rev` pin in `Cargo.toml` matching the dar version the agent
  composes against, as noted in the telegram README.
