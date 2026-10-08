# whatsapp-web

Unofficial WhatsApp linked-device channel (DMs and groups) for one dar agent and one phone number. It runs in process with [`whatsapp-rust`](https://github.com/jlucaso1/whatsapp-rust) and uses the normal `chat-pi` turn. No Meta Business Cloud API account is needed.

## Install and configure

Copy this crate to `<agent>/extensions/whatsapp-web/`. Use Rust 1.94 or newer for `dar build --dir <agent>`, then start the composed binary with `<agent>/bin/dar run --dir <agent>`; if rustup pins an older toolchain, prefix both commands with `RUSTUP_TOOLCHAIN=stable` after installing a compatible stable toolchain.

```yaml
extensions:
  whatsapp-web:
    phone_number: "33612345678" # international digits only, no +; enables pairing code
    allowed_users: ["33698765432"] # sender phones (DMs and groups); omitted/empty allows all
    allowed_groups: ["120363000000000000"] # group id (JID user part); omitted/empty allows all groups
    # backend: pi
    # sessions:
    #   idle_minutes: 1440 # optional; omitted or 0 = sessions never expire
    #   on_message_during_turn: interrupt # or queue
    # messages: # optional replies to commands (defaults shown)
    #   stopped: "Stopped."
    #   new_session: "New session started."
    #   compacted: "Compacted."
```

On first start, the agent creates `<agent>/.whatsapp-web/session.db` and logs the eight-character pairing code to the agent log and TUI Logs tab. Enter it in WhatsApp Business → Linked devices → Link with phone number. The first QR is also rendered once in the Logs tab as fallback (later rotations go only to `logs/agent.log`). These pairing values are sensitive; keep agent logs private. Subsequent starts reuse the SQLite session and skip the pairing request; the Logs tab shows `Connected as +<phone>` on each (re)connect. If WhatsApp unlinks the device, a `Logged out by WhatsApp` line appears: delete `.whatsapp-web/session.db` and restart to pair again. The extension writes a `.gitignore` inside `.whatsapp-web/` to exclude its contents. Deleting the agent or its session store removes this linked-device identity. `phone_number` can be omitted for QR-only pairing. Legacy `bridge_port` and `proxy_url` fields have no effect with the in-process library.

Every allowed DM starts a turn. In a group, a turn starts only when the agent is @-mentioned (its phone or LID JID, including media captions); each group has one chat session (`group-<id>`). Edits, self-sent messages, and broadcasts are ignored. Allowed users are international phone digits and filter the sender in DMs and groups; LID senders are resolved through the library's persisted LID-to-phone mapping, and unresolved LIDs are rejected when `allowed_users` is nonempty. During a turn the extension sends typing state, marks the inbound message read, and quotes it in its reply. `**bold**` is adapted to WhatsApp `*bold*`.

Each turn's text starts with a metadata header, e.g. `[WhatsApp group "Family" (120363…) · from Thinh · +33695189048 · 2026-10-06 18:02 +02:00]` (agent local time; the group subject is fetched once and cached, falling back to the id). Optional lines add `↪ replying to <who>: "<quote, ≤200 chars>"` and `forwarded`.

Media (image, video, audio/voice, document, sticker) is downloaded to `<session dir>/uploads/<message-id>-<name>` (max 25 MiB, 60 s) and described by an appended `Attachment metadata (untrusted data…)` JSON line; oversized or failed downloads are noted as skipped. A media message without text is a valid DM turn, and a group turn only if mentioned.

Reactions never start a turn. Reactions and unaddressed group messages from allowed users and groups are kept (max 20 per session, oldest dropped) and prepended to that session's next turn under `(since your last reply)`.

## Sessions, commands and compaction

Each chat (`pn-<phone>`, `lid-<id>`, `group-<id>`) has generations under `<agent>/.whatsapp-web/<chat>/<generation>/` with a `current` pointer; uploads live in the current generation. Older chat directories without generations keep working and simply start at generation 1. When a session opens, the newest archived backend session in the current generation is resumed, so a dar restart continues the conversation (backends that cannot resume start fresh). A failed turn keeps the session; it is dropped and reopened (with resume) only if the backend reports the session closed, the turn times out or `send_turn` fails.

`sessions.idle_minutes` is optional and has no default. When set, a message arriving after that many idle minutes (last activity is stored on disk, so it survives restarts) starts a new generation and logs `Session <chat> expired after <n> min idle; starting fresh`; the old transcript stays on disk.

`sessions.on_message_during_turn` (default `interrupt`) decides what a new message does while the chat's agent turn runs. With `interrupt`, a message that would start a turn aborts the running one (its partial reply is dropped, nothing is sent, the Logs tab says `Turn for <chat> interrupted by a new message`) and the next turn starts with `[Your previous reply was interrupted by this message]`; the backend keeps the interrupted exchange in its history. Commands, reactions and unaddressed group messages never interrupt, and `/compact` (manual or automatic) is never interrupted. With `queue`, messages wait until the running turn finishes.

Chats are processed concurrently, one worker per chat, in message order. Commands must be the whole message, sent by an allowed user; in groups the agent must be @-mentioned and the rest of the text (mention removed) must be exactly the command. They never start a normal agent turn:

- `/new` closes the session, starts a new generation, clears the chat's pending context and replies `messages.new_session`.
- `/stop` aborts the chat's in-flight turn (partial output is discarded) and replies `messages.stopped`.
- `/compact` sends `/compact` to the backend, does not relay its text, and replies `messages.compacted` on success (failures are only logged as `Compaction failed for <chat>: <error>`).

Auto-compaction: after a successful turn whose latest backend context report is at least 80% of the context window, `/compact` is sent silently (log lines `Auto-compacting <chat> (<pct>% of context)` and `Compacted <chat>`). It does not fire again until a later report is below 80%. If the backend reports usage without a window, the Logs tab says once `Auto-compaction unavailable: backend did not report a context window`.

Backend caveats: `chat-pi` and the built-in backend support `/compact`, `/stop` (real abort) and auto-compaction (the built-in backend needs `runner.context_window` set so it can report context usage). On `chat-opencode` and `chat-codex`, `/compact` is just sent to the model as text, no context usage is reported (so auto-compaction never fires), while `/stop` still aborts. Resume works where the backend archives its session (`chat-pi`, `chat-opencode`, built-in transcript resume); `chat-codex` starts fresh after a restart.

## Human setup runbook

1. Order a Free Mobile eSIM and add it to the physical phone as a second line for SMS.
2. On the Mac Studio, create an Android Studio emulator with a Google Play image. Use scrcpy or Screen Sharing for the initial GUI, then run it headless. Install WhatsApp Business, register with the Free number using the SMS code from the phone, and set a two-step PIN.
3. Warm the number manually for a few days before connecting the bot.
4. Start the agent. In WhatsApp Business on the emulator, open Linked devices → Link with phone number and enter the logged code.
5. Keep the emulator alive. A primary device offline for about 14 days logs out linked devices.

Live pairing, a phone-originated DM, and session reuse after agent restart require the real number and emulator and are validated by Thinh.
