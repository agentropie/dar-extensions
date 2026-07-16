//! Pure IRC wire-protocol parsing. No I/O: takes a raw line, yields a structured
//! message; takes a `PRIVMSG`, yields sender/target/text. Testable in isolation.

/// A parsed IRC line: optional prefix, command, and trailing params (the final
/// `:trailing` param is folded into the params list as its last element).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub prefix: Option<String>,
    pub command: String,
    pub params: Vec<String>,
}

impl Message {
    /// Parse one raw IRC line (without the trailing CRLF). Returns `None` only
    /// for an empty/whitespace line; otherwise never panics on malformed input.
    pub fn parse(line: &str) -> Option<Message> {
        let line = line.trim_end_matches(['\r', '\n']);
        let mut rest = line.trim_start();
        if rest.is_empty() {
            return None;
        }

        let mut prefix = None;
        if let Some(stripped) = rest.strip_prefix(':') {
            let (pfx, after) = match stripped.split_once(' ') {
                Some((p, a)) => (p, a),
                None => (stripped, ""),
            };
            prefix = Some(pfx.to_string());
            rest = after.trim_start();
        }

        if rest.is_empty() {
            return None;
        }

        let (command, mut after) = match rest.split_once(' ') {
            Some((c, a)) => (c.to_string(), a),
            None => (rest.to_string(), ""),
        };

        let mut params = Vec::new();
        loop {
            after = after.trim_start();
            if after.is_empty() {
                break;
            }
            if let Some(trailing) = after.strip_prefix(':') {
                params.push(trailing.to_string());
                break;
            }
            match after.split_once(' ') {
                Some((p, a)) => {
                    params.push(p.to_string());
                    after = a;
                }
                None => {
                    params.push(after.to_string());
                    break;
                }
            }
        }

        Some(Message {
            prefix,
            command,
            params,
        })
    }

    /// The nick portion of the prefix (`nick!user@host` -> `nick`), if any.
    pub fn sender_nick(&self) -> Option<&str> {
        self.prefix
            .as_deref()
            .map(|p| p.split(['!', '@']).next().unwrap_or(p))
    }
}

/// A parsed `PRIVMSG`: who sent it, where it went, and the (CTCP-normalized) text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivMsg {
    pub sender: String,
    pub target: String,
    pub text: String,
}

/// CTCP delimiter byte (`\x01`).
const CTCP: char = '\u{1}';

impl PrivMsg {
    /// Build a `PrivMsg` from a parsed `PRIVMSG` message, normalizing a CTCP
    /// `ACTION` (`/me`) into plain text (`* nick does thing`). Returns `None`
    /// for non-PRIVMSG messages or ones missing sender/target/text.
    pub fn from_message(msg: &Message) -> Option<PrivMsg> {
        if !msg.command.eq_ignore_ascii_case("PRIVMSG") {
            return None;
        }
        let sender = msg.sender_nick()?.to_string();
        if sender.is_empty() {
            return None;
        }
        let target = msg.params.first()?.clone();
        let raw = msg.params.get(1)?;
        let text = normalize_ctcp(&sender, raw);
        Some(PrivMsg {
            sender,
            target,
            text,
        })
    }
}

/// Normalize CTCP content: an `ACTION` becomes `* nick rest`; any other CTCP is
/// stripped of its delimiters; plain text passes through unchanged.
fn normalize_ctcp(sender: &str, raw: &str) -> String {
    let trimmed = raw.trim_matches(CTCP);
    if trimmed.len() == raw.len() {
        // No CTCP delimiters present.
        return raw.to_string();
    }
    if let Some(action) = trimmed
        .strip_prefix("ACTION ")
        .or_else(|| trimmed.strip_prefix("ACTION"))
    {
        return format!("* {sender} {}", action.trim_start())
            .trim_end()
            .to_string();
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_privmsg_with_prefix_and_trailing() {
        let m = Message::parse(":alice!a@host PRIVMSG #room :hello world").unwrap();
        assert_eq!(m.prefix.as_deref(), Some("alice!a@host"));
        assert_eq!(m.command, "PRIVMSG");
        assert_eq!(m.params, vec!["#room", "hello world"]);
        assert_eq!(m.sender_nick(), Some("alice"));
    }

    #[test]
    fn parses_ping() {
        let m = Message::parse("PING :server.example").unwrap();
        assert_eq!(m.prefix, None);
        assert_eq!(m.command, "PING");
        assert_eq!(m.params, vec!["server.example"]);
    }

    #[test]
    fn parses_numeric_replies() {
        let welcome = Message::parse(":srv 001 darbot :Welcome to the network").unwrap();
        assert_eq!(welcome.command, "001");
        assert_eq!(welcome.params, vec!["darbot", "Welcome to the network"]);

        let collision = Message::parse(":srv 433 * darbot :Nickname is already in use").unwrap();
        assert_eq!(collision.command, "433");
        assert_eq!(collision.params[1], "darbot");
    }

    #[test]
    fn malformed_lines_do_not_panic() {
        assert!(Message::parse("").is_none());
        assert!(Message::parse("   ").is_none());
        assert!(Message::parse(":").is_none());
        assert!(Message::parse(":onlyprefix").is_none());
        // Bare command with no params is fine.
        let m = Message::parse("QUIT").unwrap();
        assert_eq!(m.command, "QUIT");
        assert!(m.params.is_empty());
    }

    #[test]
    fn privmsg_extraction() {
        let m = Message::parse(":bob!b@h PRIVMSG darbot :hi there").unwrap();
        let pm = PrivMsg::from_message(&m).unwrap();
        assert_eq!(pm.sender, "bob");
        assert_eq!(pm.target, "darbot");
        assert_eq!(pm.text, "hi there");
    }

    #[test]
    fn non_privmsg_yields_none() {
        let m = Message::parse("PING :x").unwrap();
        assert!(PrivMsg::from_message(&m).is_none());
    }

    #[test]
    fn ctcp_action_normalized_to_text() {
        let m = Message::parse(":bob!b@h PRIVMSG #room :\u{1}ACTION waves hello\u{1}").unwrap();
        let pm = PrivMsg::from_message(&m).unwrap();
        assert_eq!(pm.text, "* bob waves hello");
    }

    #[test]
    fn plain_text_passes_through() {
        let m = Message::parse(":bob!b@h PRIVMSG #room :just talking").unwrap();
        let pm = PrivMsg::from_message(&m).unwrap();
        assert_eq!(pm.text, "just talking");
    }
}
