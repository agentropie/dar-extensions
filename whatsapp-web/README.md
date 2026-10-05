# whatsapp-web

Unofficial WhatsApp linked-device text DM channel for one dar agent and one phone number. It runs in process with [`whatsapp-rust`](https://github.com/jlucaso1/whatsapp-rust), uses the normal `chat-pi` turn, and replies only to inbound DMs. No Meta Business Cloud API account is needed.

## Install and configure

Copy this crate to `<agent>/extensions/whatsapp-web/`. Use Rust 1.94 or newer for `dar build --dir <agent>`, then start the composed binary with `<agent>/bin/dar run --dir <agent>`; if rustup pins an older toolchain, prefix both commands with `RUSTUP_TOOLCHAIN=stable` after installing a compatible stable toolchain.

```yaml
extensions:
  whatsapp-web:
    phone_number: "33612345678" # international digits only, no +; enables pairing code
    allowed_users: ["33698765432"] # omitted/empty allows all text DMs
    # backend: pi
```

On first start, the agent creates `<agent>/.whatsapp-web/session.db` and logs the eight-character pairing code. Enter it in WhatsApp Business → Linked devices → Link with phone number. A QR payload is logged as fallback. These pairing values are sensitive; keep agent logs private. Subsequent starts reuse the SQLite session. The extension writes a `.gitignore` inside `.whatsapp-web/` to exclude its contents. Deleting the agent or its session store removes this linked-device identity. `phone_number` can be omitted for QR-only pairing. Legacy `bridge_port` and `proxy_url` fields have no effect with the in-process library.

Only inbound text DMs receive replies. Groups, media, edits, self-sent messages, and broadcasts are ignored. Allowed users are international phone digits; LID senders are resolved through the library's persisted LID-to-phone mapping before applying the allowlist. Unresolved LIDs are rejected when the allowlist is nonempty. During a turn the extension sends typing state, marks the inbound message read, and quotes that message in its reply. `**bold**` is adapted to WhatsApp `*bold*`.

## Human setup runbook

1. Order a Free Mobile eSIM and add it to the physical phone as a second line for SMS.
2. On the Mac Studio, create an Android Studio emulator with a Google Play image. Use scrcpy or Screen Sharing for the initial GUI, then run it headless. Install WhatsApp Business, register with the Free number using the SMS code from the phone, and set a two-step PIN.
3. Warm the number manually for a few days before connecting the bot.
4. Start the agent. In WhatsApp Business on the emulator, open Linked devices → Link with phone number and enter the logged code.
5. Keep the emulator alive. A primary device offline for about 14 days logs out linked devices.

Live pairing, a phone-originated DM, and session reuse after agent restart require the real number and emulator and are validated by Thinh.
