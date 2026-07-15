#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    Reset,
    Abort,
}

pub fn parse(text: &str) -> Option<Command> {
    match text.trim() {
        "/reset" | "/new" => Some(Command::Reset),
        "/abort" | "/stop" => Some(Command::Abort),
        _ => None,
    }
}

pub fn reply(command: Command) -> &'static str {
    match command {
        Command::Reset => "Context cleared, new session started.",
        Command::Abort => "Stopped current response.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_command_aliases_only() {
        assert_eq!(parse("/reset"), Some(Command::Reset));
        assert_eq!(parse(" /new "), Some(Command::Reset));
        assert_eq!(parse("/abort"), Some(Command::Abort));
        assert_eq!(parse("/stop"), Some(Command::Abort));
        assert_eq!(parse("/reset please"), None);
    }
}
