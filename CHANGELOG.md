# Changelog

## [Unreleased]

### Added

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

- **telegram:** upgrading from the pre-generation session layout
  (`sessions/<chat_id>/` directly) now migrates existing session data into a
  generation instead of silently dropping prior chat context. (ALG-347)

## [0.3.1] - 2026-07-02

### Added

- **irc, telegram:** agent system context (skills, environment) is now shared
  with the SDK chat helper so extension-driven turns carry the same grounding as
  native ones. (ALG-315)
- **irc:** a 👀 reaction acknowledges a human message the moment the agent picks
  it up. (ALG-318)
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
