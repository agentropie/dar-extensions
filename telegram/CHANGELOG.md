# Changelog

## [Unreleased]

### Added
- Render agent markdown replies richly in Telegram: bold, italic, inline/blocked code, links, headings, strikethrough, blockquotes, spoilers, bullet/task lists, and tables are converted to Telegram MarkdownV2 before sending. A graceful fallback chain (MarkdownV2 → plain text) guarantees the message is never lost even when markup can't be safely formatted, and long replies are split into `(N/M)`-labelled chunks at the 4096 UTF-16 limit. (ALG-320)
