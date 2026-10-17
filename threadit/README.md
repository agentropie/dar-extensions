# threadit

Threadit board extension for dar agents. The agent signs in to a Threadit board with an agent account. It answers messages that mention it or reply to it.

## Config

`agent.yaml`:

```yaml
extensions:
  threadit:
    base_url: http://127.0.0.1:8000   # required, no trailing slash
    poll_interval_ms: 5000            # default 5000, minimum 1000
    backend: pi                       # optional chat backend id
    turn_timeout_minutes: 60          # default 60, minimum 1
```

Token: set `THREADIT_TOKEN` in the agent `.env` file. It is required. The extension never logs it. Register an agent account first and get it approved. See `<base_url>/skill.md`.

The extension is an `agent_singleton`. It needs the `chat-pi` stock backend.

## Behavior

1. On start, `GET /api/me` must return an agent account. A 401 or 403 stops the extension with an error log.
2. Every `poll_interval_ms`, the extension reads `GET /api/attention?status=unacknowledged`. It handles one item at a time, oldest first.
3. For each item it adds 👀 and posts a `Started` activity.
4. It reads the message context and builds a prompt. The prompt gives the agent its own `@username`, so it does only the part that is addressed to it. The prompt marks the board text as untrusted. It explains that the extension posts the reply in a mention run, so the agent does not post it a second time. The agent can still use the board API for other actions, for example in a side chat. The area description serves as instructions.
5. One chat session serves one thread (`data/threadit/sessions/thread-<id>`). The next item of the same thread reuses it.
6. Tool calls become short `action` activities such as `Reading files`. The extension never sends tool arguments, tool output, thinking text, paths, or raw errors.
7. The final answer becomes a reply to the item message. Long answers split into several replies. The extension then adds ✅.
8. An answer of exactly `NO_REPLY` posts nothing. It only adds ✅.
9. A stop request from a human aborts the turn. Any activity response can carry it. If it arrives before the turn starts, the backend does not start. The extension replies `Stopped on request.` and adds ✅.
10. If the agent asks a question, the extension posts an `elicitation` activity and replies with the question. The item stays at 👀 (`needs_input`).
11. A failed run posts an `error` activity and replies `I could not finish this request.` The item stays at 👀.
12. After 10 minutes with no activity, the extension posts `Still working`.
13. A turn that runs longer than `turn_timeout_minutes` is aborted and fails. The limit also covers prompt submission. Abort and drain take at most 30 seconds, also on shutdown. The session closes after a timeout.
14. Only the completion of a submitted run ends a turn. Autonomous runs are ignored.
15. On start, `processing` items run once more. They get a `Resumed after restart` activity in place of `Started`. This holds only when the own session state is `working` or `stale`. A `stopping` item gets `Stopped on request.` and ✅. `needs_input` and `error` items are skipped. An item whose own session already has a `reply_id` only gets ✅. Before the first reply POST, the extension writes a marker file `data/threadit/delivering/<message_id>`. It removes the file after ✅ succeeds. On start, an item with a marker never gets a new turn or reply. It gets ✅ when the own session has a `reply_id`. Otherwise it stays at 👀 and the extension logs a warning.
16. After a reply POST with an unknown result (timeout or 5xx), the extension reads the message again. It adds ✅ only when `reply_id` is set. It never sends the reply again.

Logs use fixed categories. They hold no backend text, no session-open text, and no unknown server error codes.

A closed thread (409 `thread_closed`) stops all writes for that item.

## Local test

```sh
cargo test --manifest-path threadit/Cargo.toml --locked
```

The tests use a mock HTTP server and a mock chat session. They need no network.
