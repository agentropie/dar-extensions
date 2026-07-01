# Changelog

## [Unreleased]

### Added
- Live reply streaming: the assistant answer now appears while the turn runs in a single editable bubble (rate-limited to ~1s / 200 chars) instead of only after the turn finishes.
- Tool status bubble: a separate editable message shows the running tool as `name · short target/preview` (not full argument JSON), flushes any pre-tool assistant text first, and collapses to a `Used N tools: …` summary when the turn finishes.
