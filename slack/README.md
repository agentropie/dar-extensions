# slack

Slack Socket Mode extension for one self-contained DAR agent.

## Configuration

```yaml
extensions:
  slack:
    token: xoxb-...       # or SLACK_BOT_TOKEN
    appToken: xapp-...    # or SLACK_APP_TOKEN
    channels:
      C0123456789:
        requireMention: true
        threadPolicy: always # always | never | follow
        users: []
    dm:
      enabled: false
      users: []
      threadPolicy: always # always | never | follow
    showThinking: true
    deleteThinkingOnComplete: true
    historyLimit: 20
    clearHistoryAfterReply: false
```

Configured channels are allowlisted. Empty `channels` permits all channels,
with mention gating. Empty user lists permit all users in enabled route. DMs
are disabled by default. Each DM, channel root, and thread gets distinct
workspace-scoped DAR session directory.

## Slack app setup

Enable Socket Mode. Create app-level `xapp-` token with `connections:write`.
Install bot with `chat:write`, `files:write`, `app_mentions:read`, `channels:history`,
`groups:history`, `im:history`, `reactions:read`, `reactions:write`, `users:read`,
and `channels:read`/`groups:read` for listing tools. Subscribe to `message.im`,
`message.channels`, `message.groups`, `app_mention`, `reaction_added`, and
`reaction_removed` as needed. Configure
`/new`, `/stop`, `/help`, and `/ping` slash commands in Slack; Slack commands
must be created in app configuration, not this extension.

## Behavior

- ACKs Socket Mode envelopes before model, download, or Slack Web API work.
- Routes Slack messages to DAR chat backend and replies with Slack-safe chunks.
- Adds `:eyes:` while a turn runs when `showThinking` is enabled. Thinking
  deltas create/update a separate `🧠 Thinking:` reply (3-second coalescing,
  3000-character cap); `deleteThinkingOnComplete` controls its retention.
  Slack has no bot typing API. Reaction failures never fail agent turn.
- Downloads private attachments into agent `data/uploads`; runner receives only
  local relative path and untrusted metadata, never private Slack URL/token.
- Keeps process-lifetime Slack-local context per workspace/DM/channel/thread:
  newest 50 accepted messages, with `historyLimit` (default 20; `0` keeps all).
  This untrusted context is prefixed to each prompt; it does not reset DAR
  backend history. `clearHistoryAfterReply` clears it only after agent and Slack
  reply success.
- Registers `slack.send_message`, `slack.list_users`, and `slack.list_channels`
  for normal agent and scheduled-job use. Sending renders Markdown as Slack mrkdwn
  and chunks long messages; `threadTs` replies in a thread (`thread_ts` accepted
  for compatibility). Listing accepts case-insensitive `query` filters.

## Limits

Socket delivery and dedupe are currently best effort: reconnect/crash can lose
in-memory session and duplicate suppression state. Generated files must be written under `data/artifact-exports/` then published with
`artifact.publish`; verified artifacts upload to originating Slack channel/thread after
successful agent response text. Enable DAR `tool-registry-host` so `artifact.publish`
and artifact store service are registered. `slack.send_message` takes Slack
channel or DM conversation ID; direct-message creation from a user ID is not
implemented. File downloads are capped at 25 MiB. Never place Slack tokens in
agent config committed to source control.
