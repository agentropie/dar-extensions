# Mattermost

Standalone singleton chat extension for one dar agent and one Mattermost bot.
Copy this crate to the agent's `extensions/mattermost/` directory and run
`dar build` in that agent. The `chat-pi` stock backend is required; file output
and agent tools also require dar's `tool-registry-host` feature.

## Setup

Create a bot under **Integrations → Bot Accounts**, enable its access token,
and put the token in the agent environment as `MATTERMOST_BOT_TOKEN`.
Set `MATTERMOST_URL=https://chat.example.com`, or configure `url` below.
Add the bot to the teams and channels it should serve. It needs permission to
read and create posts, edit/delete its own posts, add/remove reactions, upload
and download files, read users and channels, and open direct channels.
Server policy may restrict bot accounts or personal access tokens.
Channel posting, reactions, user/channel listing and file transfers were checked
with an ordinary `system_user` bot, without elevated roles.

The configuration never accepts a token key. Do not commit tokens or include
them in prompts. HTTPS is recommended; HTTP supports local testing.

```yaml
extensions:
  mattermost:
    url: https://chat.example.com       # or MATTERMOST_URL
    ackReaction: true
    ackEmoji: eyes                      # emoji name without colons
    showThinking: false
    deleteThinkingOnComplete: true
    historyLimit: 20                    # 0 = all buffered, at most 50
    fetchHistory: true
    clearHistoryAfterReply: false
    allowBots: false                    # false | true | [bot user ids]
    sessions: { idleMinutes: 360 }      # 0 disables expiry
    dm: { enabled: true, users: [], threadPolicy: follow }
    channels:                           # omit to serve every joined channel
      aaaaaaaaaaaaaaaaaaaaaaaaaa:
        requireMention: true
        threadPolicy: always
        users: []
```

All shown values except `url` and `channels` are defaults. `backend` optionally
selects a registered chat backend. Unknown configuration keys are errors.
URLs may end in `/api/v4`; that suffix and trailing slashes are normalized.
`channels: {}` disables channels; omitted `channels` permits all joined channels.
Public/private channels and group DMs use channel settings. DMs default on.
Empty user allowlists permit everyone; entries match user IDs or usernames,
case-insensitively with an optional leading `@`.

## Conversations and commands

Channels require `@bot-username` by default. A broadcast mention such as
`@channel` does not address the bot. Attachment-only human posts are accepted
without a mention. Set `requireMention: false` for a dedicated conversation.

`threadPolicy` is `always`, `never`, or `follow`: always open/reuse a thread,
always reply at the channel root, or follow the incoming post's thread.
Channels default to `always`; DMs default to `follow`. Each DM, channel root
and thread has its own persistent session. A mention that opens a thread shares
its session with follow-ups. After an accepted turn in a thread, humans need
no further mention there until reset or idle expiry. All allowlists still apply.

Type these exact messages (optionally after mentioning the bot):

| Command | Result |
| --- | --- |
| `!new`, `!reset` | Stop the turn and start a fresh session; clear history and engagement. |
| `!stop`, `!abort` | Stop the active response. |
| `!help` | Show command help. |
| `!ping` | Reply `pong`. |

The `/` forms are also recognized as text. Prefer `!`: the Mattermost web client
intercepts unknown slash commands. Every command confirms receipt, including
when there is no active turn. Extra words make a message ordinary chat.
A new accepted message stops the previous response in the same conversation.
Idle expiry preserves old session files and starts a new generation with a notice.

## Replies, context and files

An independent `eyes` reaction acknowledges work. Typing refreshes while a turn
runs. Human-triggered replies grow through coalesced edits; bot-triggered replies
are posted once, complete, so other bots see the whole answer. Optional thinking
appears in a separate `🧠 Thinking:` post. `deleteThinkingOnComplete` controls its
normal cleanup. `NO_REPLY` and blocked bot loops delete provisional output and
remove the acknowledgement, regardless of display settings; even an expiry
notice is suppressed. Empty non-silent answers show `(no response)`.

History is untrusted context, capped at 50 posts per conversation. `historyLimit`
selects how much is included; restart backfill runs once per conversation when
enabled. Other bots remain in history even when they cannot trigger turns.
Only accepted agent turns refresh idle activity.

Up to 10 input files per post are downloaded to the agent's `data/uploads`.
Each is limited to 25 MiB before and during download. Redirects and downloads
from another origin are rejected. The agent receives local relative paths,
original names, MIME types and an untrusted-source label, never private file URLs
or tokens. Download failures produce a visible generic error.

Files published with `artifact_publish` are verified and claimed through the
artifact store, uploaded after reply text, and posted to the same channel/thread.
A compatible chat backend must forward dar's `ArtifactReady` notifications.
File output is implemented here, but the current Pi callback does not reach
extensions; end-to-end file output with Pi requires the dar-core fix in ALG-631.
A failed upload releases the claim and posts `Generated file could not be uploaded.`
Successful delivery receipts prevent duplicate uploads.

## Bots, tools and scheduler

Other bots are off by default. `allowBots: true` allows all other bot authors;
a list allows only those user IDs. Bots must always mention this bot, including
in DMs and channels without mention gating. Own posts, webhook/plugin posts and
system posts never trigger turns. Per-conversation dar loop guards limit bot
exchanges; human messages reset the guard.

| Tool | Arguments and result |
| --- | --- |
| `mattermost_send_message` | Required non-empty `text`, exactly one of `channel` or `user`, optional `rootId`. Returns delivery details. |
| `mattermost_list_users` | Optional case-insensitive `query` and `limit` (100 default); returns ID, username, name and bot flag. |
| `mattermost_list_channels` | Optional substring `query` and `limit` (100 default); returns ID, name, displayName, type and teamId. |

Channels resolve by ID or case-insensitive name/display name across the bot's
teams; leading `#`/`~` is optional. Ambiguous names require an ID. Users resolve
by ID or exact username. Sending to a user opens a DM. Outbound messages obey
the resolved channel/DM allowlists, including when given a raw DM channel ID.
Disallowed destinations return `not_allowed`. Only send is marked as a write.

The scheduler uses the same send path and restrictions:

```json
{ "target": "mattermost", "channel": "ops" }
```

```json
{ "target": "mattermost", "user": "alex" }
```

## Limits and recovery

Replies split at 15,000 Unicode characters with balanced code fences, below
Mattermost's usual 16,383-character post limit. Broadcast mentions (`@channel`,
`@all`, `@here`) in generated text are neutralized. Confirm `MaxPostSize` on your
server before deployment. Thinking is capped at 3,000 characters and coalesced
at three seconds; reply edits coalesce at two seconds.

REST requests have 30-second deadlines (120 seconds for file transfers).
Network failures, HTTP 429 and 5xx retry up to three attempts; other 4xx do not.
Agent acceptance and time between agent events each have 60-second timeouts.
Failures show a short generic error; if that cannot be posted, a warning reaction
is attempted. Cancellation is quiet.

WebSocket reconnect delays are 1, 2, 4, 8, 16, then 30 seconds. Resume state
requests replay; a fresh connection catches up watched channels with five seconds
of overlap, capped to the last ten minutes. Recent post IDs suppress duplicates.
Pings run every 30 seconds; 75 seconds without traffic forces reconnect.
Shutdown closes the connection, cancels active turns, and waits for them.

Run local checks from this directory:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Tests use local fake servers and invented data. A deployment still needs live
checks for permissions, bot visibility/classification, typing timeout, server
post limit, DMs, threads, files, commands, scheduled delivery and bot loop guards.
