# dar-extensions

External extensions for dar agents: [irc](irc), [slack](slack), [telegram](telegram). Each crate is standalone — its own `Cargo.toml` and `[workspace]` — and depends only on `dar-extension-sdk`.

## Using an extension with an agent

Vendor the extension into the agent folder: copy (or symlink) the crate directory into `extensions/<name>/`, e.g. `my-agent/extensions/telegram/`. `dar build` discovers every subdirectory of `extensions/` that has a `Cargo.toml` with `package.metadata.dar.factory` set (dirs without it are ignored) and generates a per-agent `.dar/` crate that:

- adds each discovered extension as a path dependency and calls its `factory` from `main.rs`,
- links any stock crates named in that extension's `package.metadata.dar.requires_stock` (e.g. `chat-pi`, the bundled `pi` chat backend all three of these extensions require),
- patches the pinned `dar-*` registry crates (including `dar-extension-sdk`) to the local dar checkout used to run `dar build`, so the extension's `"0.x"` crates.io version requirement resolves against that checkout instead of a published release.

The extension activates through its own `extensions.<id>:` section in the agent's `agent.yaml` (each extension validates its own required fields, e.g. `irc.server`/`irc.nick`, at register time). Secrets — bot tokens, passwords — come from environment variables or the agent's `.env`, never from committed `agent.yaml`:

```yaml
extensions:
  telegram:
    # bot_token can be omitted here and supplied via TELEGRAM_BOT_TOKEN in .env instead
    allowed_users: [12345678]
```

## `agent_singleton`

`Extension::agent_singleton(&self) -> bool` defaults to `false`. Override it to `true` when the extension holds one external connection per agent identity — a chat bridge or a poller — that must not be opened twice for the same agent.

Effect: hosts booted for a non-default `dar run --workflow <path>` process skip any extension where this returns `true`, so only the default-workflow process owns the external connection. All three extensions here — irc, slack, telegram — set it.

Flip side: if an agent runs only `--workflow` processes (no default-workflow process), singleton extensions run nowhere.

## Extensions

| extension | what it does |
|-----------|---------------|
| [irc](irc) | Makes an agent reachable over IRC — shared team channels holding both humans and multiple agents, and private DMs. |
| [slack](slack) | Slack Socket Mode extension for one self-contained agent. |
| [telegram](telegram) | Makes an agent reachable for chat over a Telegram bot. |
