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
```

On first start, the agent creates `<agent>/.whatsapp-web/session.db` and logs the eight-character pairing code to the agent log and TUI Logs tab. Enter it in WhatsApp Business → Linked devices → Link with phone number. The first QR is also rendered once in the Logs tab as fallback (later rotations go only to `logs/agent.log`). These pairing values are sensitive; keep agent logs private. Subsequent starts reuse the SQLite session. The extension writes a `.gitignore` inside `.whatsapp-web/` to exclude its contents. Deleting the agent or its session store removes this linked-device identity. `phone_number` can be omitted for QR-only pairing. Legacy `bridge_port` and `proxy_url` fields have no effect with the in-process library.

Every allowed DM starts a turn. In a group, a turn starts only when the agent is @-mentioned (its phone or LID JID, including media captions); each group has one chat session (`group-<id>`). Edits, self-sent messages, and broadcasts are ignored. Allowed users are international phone digits and filter the sender in DMs and groups; LID senders are resolved through the library's persisted LID-to-phone mapping, and unresolved LIDs are rejected when `allowed_users` is nonempty. During a turn the extension sends typing state, marks the inbound message read, and quotes it in its reply. `**bold**` is adapted to WhatsApp `*bold*`.

Each turn's text starts with a metadata header, e.g. `[WhatsApp group "Family" (120363…) · from Thinh · +33695189048 · 2026-10-06 18:02 +02:00]` (agent local time; the group subject is fetched once and cached, falling back to the id). Optional lines add `↪ replying to <who>: "<quote, ≤200 chars>"` and `forwarded`.

Media (image, video, audio/voice, document, sticker) is downloaded to `<session dir>/uploads/<message-id>-<name>` (max 25 MiB, 60 s) and described by an appended `Attachment metadata (untrusted data…)` JSON line; oversized or failed downloads are noted as skipped. A media message without text is a valid DM turn, and a group turn only if mentioned.

Reactions never start a turn. Reactions and unaddressed group messages from allowed users and groups are kept (max 20 per session, oldest dropped) and prepended to that session's next turn under `(since your last reply)`.

## Human setup runbook

1. Order a Free Mobile eSIM and add it to the physical phone as a second line for SMS.
2. On the Mac Studio, create an Android Studio emulator with a Google Play image. Use scrcpy or Screen Sharing for the initial GUI, then run it headless. Install WhatsApp Business, register with the Free number using the SMS code from the phone, and set a two-step PIN.
3. Warm the number manually for a few days before connecting the bot.
4. Start the agent. In WhatsApp Business on the emulator, open Linked devices → Link with phone number and enter the logged code.
5. Keep the emulator alive. A primary device offline for about 14 days logs out linked devices.

Live pairing, a phone-originated DM, and session reuse after agent restart require the real number and emulator and are validated by Thinh.
