# whatsapp

WhatsApp Business Cloud API channel for a dar agent. It owns a small local webhook listener and uses Meta's Graph API for outgoing text; text DMs only in v1.

## Meta setup

1. Create a Meta app with the **WhatsApp** product and add a WhatsApp Business Account/phone number.
2. Copy the **Phone number ID** (not the displayed phone number), a permanent System User access token with WhatsApp permissions, your app secret, and a chosen webhook verify token.
3. Expose the configured local port at a public HTTPS origin. `tailscale funnel` is suitable; `tailscale serve` alone is tailnet-only. A normal reverse proxy is equally fine.
4. In Meta, open WhatsApp → Configuration, set callback URL to `<public>/whatsapp/webhook`, set the same verify token, then subscribe to the `messages` field.

Meta retries non-200 callbacks for days, so accepted signed payloads are acknowledged immediately. The in-memory `wamid` deduplication cache is intentionally lost on restart.

## Install and configure

Copy this directory to `<agent>/extensions/whatsapp/`, then run `dar build --dir <agent>` and `dar run --dir <agent>`. `requires_stock = ["chat-pi"]` links the normal `pi` chat backend; do not add `chat-pi` as a direct dependency.

```yaml
extensions:
  whatsapp:
    # Non-blank YAML values win; every credential also supports its env fallback.
    phone_number_id: "1234567890" # WHATSAPP_PHONE_NUMBER_ID
    access_token: "..."          # WHATSAPP_ACCESS_TOKEN
    app_secret: "..."            # WHATSAPP_APP_SECRET
    verify_token: "..."          # WHATSAPP_VERIFY_TOKEN
    api_version: v20.0
    allowed_users: ["33612345678"] # empty or omitted allows all DMs
    webhook:
      bind: 127.0.0.1
      port: 8090
      path: /whatsapp/webhook
      public_url: https://host.tailnet.ts.net
    # backend: pi
    tool_status: true
    sessions: { idle_minutes: 360 }
```

The startup log prints the listening URL and, when `public_url` is configured, the exact Meta callback URL. Missing `verify_token` or `app_secret` lets the agent start but makes verification/inbound POST return 503.

## Behaviour and limits

- Only signed text DMs for the configured Phone Number ID are dispatched. Statuses, media, reactions, other phone IDs, and duplicate `wamid`s are acknowledged but ignored; malformed JSON returns `400` so configuration mistakes are visible.
- Replies are sent once a turn finishes (there is no Cloud API message-edit endpoint). `**bold**` is lightly adapted to WhatsApp `*bold*`; headers lose their `#` prefix.
- Each outgoing message is split at 4096 Unicode characters. The initial response quotes the inbound message. Graph API success means accepted, not delivered.
- WhatsApp permits ordinary messages only during Meta's customer-service window (normally 24 hours after the user's last message). Proactive sends through `whatsapp_send_message` surface Graph's rejection; templates are out of scope.
- Sessions live under `data/whatsapp/sessions/<wa_id>/<generation>/`; wa_ids must be ASCII digits before becoming path components. `/new` and `/reset` create a fresh generation.
