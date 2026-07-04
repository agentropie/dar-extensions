# Changelog

## [Unreleased]

### Fixed
- Upgrade compatibility (ALG-347): existing sessions stored in the old `sessions/<chat_id>/` layout are now migrated into a generation on the first message after upgrade, preserving prior chat context instead of silently starting empty.

### Added
- Session lifecycle controls (ALG-347): chat context now expires after an idle window (`extensions.telegram.sessions.idle_minutes`, default 360, `0` disables), rotating to a fresh session generation on the next message and prefixing the reply with `Previous session expired; starting fresh.`. `/new` and `/reset` (plus `/new@bot` / `/reset@bot`) start a fresh session and reply `Context cleared, new session started.` without running the agent. Sessions are stored append-only by generation under `sessions/<chat_id>/<generation_id>/`; old generations are never deleted.
- Live reply streaming: the assistant answer now appears while the turn runs in a single editable bubble (rate-limited to ~1s / 200 chars) instead of only after the turn finishes.
- Tool status bubble: a separate editable message shows the running tool as `name · short target/preview` (not full argument JSON), flushes any pre-tool assistant text first, and collapses to a `Used N tools: …` summary when the turn finishes.
