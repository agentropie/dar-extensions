#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    Reset,
    Abort,
    Help,
    Ping,
}
pub fn parse(text: &str) -> Option<Command> {
    let text = text.trim();
    let body = text.strip_prefix('!').or_else(|| text.strip_prefix('/'))?;
    match body {
        "new" | "reset" => Some(Command::Reset),
        "stop" | "abort" => Some(Command::Abort),
        "help" => Some(Command::Help),
        "ping" => Some(Command::Ping),
        _ => None,
    }
}
pub fn reply(command: Command) -> &'static str {
    match command { Command::Reset => "Context cleared, new session started.", Command::Abort => "Stopped current response.", Command::Ping => "pong", Command::Help => "Commands: !new (reset context), !stop (stop response), !help, !ping. Prefer ! because the Mattermost web client intercepts unknown / commands." }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_text_commands_accept_both_prefixes_and_confirm_every_action() {
        for text in ["!new", "/new", "!reset", " /reset "] {
            assert_eq!(parse(text), Some(Command::Reset));
        }
        for text in ["!stop", "/stop", "!abort", "/abort"] {
            assert_eq!(parse(text), Some(Command::Abort));
        }
        for text in ["!help", "/help"] {
            assert_eq!(parse(text), Some(Command::Help));
        }
        for text in ["!ping", "/ping"] {
            assert_eq!(parse(text), Some(Command::Ping));
        }
        for text in [
            "new",
            "!new please",
            "/reset please",
            "!ping now",
            "hello !help",
            "!NEW",
            "",
        ] {
            assert_eq!(parse(text), None);
        }
        assert_eq!(
            reply(Command::Reset),
            "Context cleared, new session started."
        );
        assert_eq!(reply(Command::Abort), "Stopped current response.");
        assert_eq!(reply(Command::Ping), "pong");
        assert!(reply(Command::Help).contains("!new"));
        assert!(reply(Command::Help).contains("!stop"));
        assert!(reply(Command::Help).contains("!help"));
        assert!(reply(Command::Help).contains("!ping"));
        assert!(reply(Command::Help).contains("web client intercepts"));
    }
}
