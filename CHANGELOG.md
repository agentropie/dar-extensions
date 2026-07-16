# Changelog

## [Unreleased]

### Added

- **discord:** session generations rotate lazily after configurable idle expiry (`sessions.idle_minutes`, default 360; `0` disables); persisted activity survives restart and legacy contexts remain resumable.
- **irc:** session generations rotate after configurable idle expiry; authorized exact `/new` and `/reset` commands start fresh context while retaining legacy generations.
- **discord:** recent channel and thread messages are now supplied as bounded, configurable agent context.
- **discord:** text-channel threads now inherit parent-channel addressing, reply in-thread, and keep isolated engaged sessions for follow-ups.
- **discord:** the gateway now reconnects with capped exponential backoff and cleanly drains active turns during shutdown.
- **discord:** agents can now send messages to configured channels or automatically opened user DMs with `discord_send_message`.
- **discord:** message attachments are downloaded and routed to the agent.
- **discord:** `/reset` (`/new`) starts a fresh channel session, while `/abort` (`/stop`) cancels the active response with a visible confirmation.
- **discord:** accepted messages now receive a configurable immediate acknowledgement and delivery failures surface visibly instead of being dropped.
- **discord:** guild channels now support mention-gated, allowlisted addressing with isolated per-channel sessions.
- **discord:** added a Discord DM extension with persisted per-user agent sessions.
- **discord:** replies now stream through live edits and continue across Discord-sized messages.

- **irc, slack, telegram:** now declare `agent_singleton`, so a non-default
  `dar run --workflow` process skips them — the default-workflow process
  owns the external connection.
- **telegram:** Telegram-only session lifecycle: idle expiry
  (`extensions.telegram.sessions.idle_minutes`, default 360, `0` disables)
  rotates a stale chat to a fresh session generation on the next message and
  prefixes the reply with `Previous session expired; starting fresh.`. `/new`
  and `/reset` (and their `@bot` forms) start a fresh session with
  `Context cleared, new session started.` and skip the agent turn. Sessions are
  stored append-only by generation under `sessions/<chat_id>/<generation_id>/`;
  old generations are retained for audit/debug. (ALG-347)

### Fixed

- **discord:** active threads are restored from the gateway's initial guild state, preserving parent-channel addressing after startup or reconnect.
- **slack:** assistant replies now appear as the runner emits them instead of
  arriving together after a turn completes.

- **telegram:** upgrading from the pre-generation session layout
  (`sessions/<chat_id>/` directly) now migrates existing session data into a
  generation instead of silently dropping prior chat context. (ALG-347)

## [0.3.1] - 2026-07-02

### Added

- **irc, telegram:** agent system context (skills, environment) is now shared
  with the SDK chat helper so extension-driven turns carry the same grounding as
  native ones. (ALG-315)
- **irc:** a 👀 reaction acknowledges a message the moment the agent picks it
  up. (ALG-318)
- **telegram:** self-clearing 👀 acknowledgement plus a typing indicator while a
  turn is in flight. (ALG-319)
- **telegram:** agent replies render as rich Markdown with an automatic
  plain-text fallback when formatting can't be applied. (ALG-320)
- **telegram:** replies stream live, with in-progress tool status surfaced as the
  turn runs. (ALG-325)
- **irc:** `debounce_ms` config (env `IRC_DEBOUNCE_MS`, default 1500, `0` to
  disable) coalesces rapid successive lines from the same conversation — such as a
  pasted multi-line DM — into a single agent turn instead of spawning serial
  turns. (ALG-324)

### Fixed

- **irc:** replies are no longer dropped after rapid multi-line DM input. Agent
  turns now run on a dedicated worker task off the socket read loop, so server
  `PING`s stay answered during long turns and the connection no longer goes stale
  (previously the inline turn starved the read loop and outbound `PRIVMSG` failed
  with `Broken pipe`). A completed reply produced while the link is down is queued
  and retried on reconnect instead of being silently lost. (ALG-324)
