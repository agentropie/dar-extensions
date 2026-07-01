# Changelog

## [Unreleased]

### Fixed

- **irc:** replies are no longer dropped after rapid multi-line DM input. Agent
  turns now run on a dedicated worker task off the socket read loop, so server
  `PING`s stay answered during long turns and the connection no longer goes stale
  (previously the inline turn starved the read loop and outbound `PRIVMSG` failed
  with `Broken pipe`). A completed reply produced while the link is down is queued
  and retried on reconnect instead of being silently lost. (ALG-324)

### Added

- **irc:** `debounce_ms` config (env `IRC_DEBOUNCE_MS`, default 1500, `0` to
  disable) coalesces rapid successive lines from the same conversation — such as a
  pasted multi-line DM — into a single agent turn instead of spawning serial
  turns. (ALG-324)
