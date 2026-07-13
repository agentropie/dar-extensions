#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    New,
    Stop,
    Help,
    Ping,
}

pub fn parse(text: &str) -> Option<Command> {
    match text
        .split_whitespace()
        .next()?
        .trim_start_matches(['/', '!'])
    {
        "new" => Some(Command::New),
        "stop" => Some(Command::Stop),
        "help" => Some(Command::Help),
        "ping" => Some(Command::Ping),
        _ => None,
    }
}

pub fn reply(command: Command) -> &'static str {
    match command {
        Command::New => "Started a new conversation.",
        Command::Stop => "Stopped current response.",
        Command::Help => "Commands: /new, /stop, /help, /ping. In a thread, use !new or !stop.",
        Command::Ping => "pong",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_commands() {
        assert_eq!(parse("/new"), Some(Command::New));
        assert_eq!(parse("!new"), Some(Command::New));
        assert_eq!(parse("/unknown"), None);
    }
}
