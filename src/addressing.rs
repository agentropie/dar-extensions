//! Pure addressing logic: given a parsed `PRIVMSG` and the bot's own nick,
//! decide whether to reply, ingest as ambient context, or ignore. No I/O.

use crate::proto::PrivMsg;

/// What the channel loop should do with an incoming `PRIVMSG`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Address the bot: run a turn and reply.
    Reply,
    /// Not addressed but in a watched channel: feed as ambient context only.
    ContextOnly,
    /// The bot's own message, or otherwise irrelevant: drop it.
    Ignore,
}

/// The conversation a message belongs to: a channel (by name) or a DM (by the
/// sender's nick). Used as the session key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Conversation {
    Channel(String),
    Dm(String),
}

impl Conversation {
    /// A filesystem-safe key for the session directory.
    pub fn key(&self) -> String {
        match self {
            Conversation::Channel(c) => format!("chan_{}", sanitize(c)),
            Conversation::Dm(n) => format!("dm_{}", sanitize(n)),
        }
    }

    /// A stable key identifying this conversation for the loop-guard. Distinct
    /// namespaces for channels vs. DMs so a channel `#x` and a DM from nick `x`
    /// never collide.
    pub fn guard_key(&self) -> String {
        match self {
            Conversation::Channel(c) => c.clone(),
            Conversation::Dm(n) => format!("dm:{n}"),
        }
    }

    /// The IRC target to send replies to.
    pub fn reply_target(&self, sender: &str) -> String {
        match self {
            // Replies to a channel go to the channel.
            Conversation::Channel(c) => c.clone(),
            // Replies to a DM go back to the sender.
            Conversation::Dm(_) => sender.to_string(),
        }
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// True if `target` names a channel (`#`, `&`, `+`, `!` prefixes per RFC 2812).
pub fn is_channel(target: &str) -> bool {
    target.starts_with(['#', '&', '+', '!'])
}

/// Classify a `PRIVMSG` for the bot identified by `bot_nick`. Returns the
/// verdict plus the conversation it belongs to.
pub fn classify(pm: &PrivMsg, bot_nick: &str) -> (Verdict, Conversation) {
    if pm.sender.eq_ignore_ascii_case(bot_nick) {
        // Never react to our own messages. Key is best-effort.
        let conv = if is_channel(&pm.target) {
            Conversation::Channel(pm.target.clone())
        } else {
            Conversation::Dm(pm.sender.clone())
        };
        return (Verdict::Ignore, conv);
    }

    if is_channel(&pm.target) {
        let conv = Conversation::Channel(pm.target.clone());
        if is_mention(&pm.text, bot_nick) {
            (Verdict::Reply, conv)
        } else {
            (Verdict::ContextOnly, conv)
        }
    } else {
        // A DM (target is our own nick): always a reply, keyed by sender.
        (Verdict::Reply, Conversation::Dm(pm.sender.clone()))
    }
}

/// True if `text` addresses `bot_nick` with a leading `nick:` or `nick,` prefix
/// (case-insensitive, allowing leading whitespace).
pub fn is_mention(text: &str, bot_nick: &str) -> bool {
    let t = text.trim_start();
    let lower = t.to_ascii_lowercase();
    let nick = bot_nick.to_ascii_lowercase();
    if !lower.starts_with(&nick) {
        return false;
    }
    // Next char after the nick must be a `:` or `,` separator.
    matches!(t[bot_nick.len()..].chars().next(), Some(':') | Some(','))
}

/// Strip a leading `nick:`/`nick,` address from a mention so the turn sees just
/// the user's request.
pub fn strip_mention<'a>(text: &'a str, bot_nick: &str) -> &'a str {
    let t = text.trim_start();
    if is_mention(t, bot_nick) {
        t[bot_nick.len() + 1..].trim_start()
    } else {
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pm(sender: &str, target: &str, text: &str) -> PrivMsg {
        PrivMsg {
            sender: sender.into(),
            target: target.into(),
            text: text.into(),
        }
    }

    #[test]
    fn dm_is_reply() {
        let (v, conv) = classify(&pm("alice", "darbot", "hello"), "darbot");
        assert_eq!(v, Verdict::Reply);
        assert_eq!(conv, Conversation::Dm("alice".into()));
    }

    #[test]
    fn channel_without_mention_is_context_only() {
        let (v, conv) = classify(&pm("alice", "#room", "just chatting"), "darbot");
        assert_eq!(v, Verdict::ContextOnly);
        assert_eq!(conv, Conversation::Channel("#room".into()));
    }

    #[test]
    fn channel_with_colon_mention_is_reply() {
        let (v, _) = classify(&pm("alice", "#room", "darbot: do the thing"), "darbot");
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn channel_with_comma_mention_is_reply() {
        let (v, _) = classify(&pm("alice", "#room", "darbot, hi"), "darbot");
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn mention_is_case_insensitive() {
        let (v, _) = classify(&pm("alice", "#room", "DARBOT: hey"), "darbot");
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn substring_nick_is_not_a_mention() {
        // "darbotic" must not count as addressing "darbot".
        let (v, _) = classify(&pm("alice", "#room", "darbotic things"), "darbot");
        assert_eq!(v, Verdict::ContextOnly);
    }

    #[test]
    fn self_authored_is_ignored() {
        let (v, _) = classify(&pm("darbot", "#room", "darbot: my own message"), "darbot");
        assert_eq!(v, Verdict::Ignore);
        let (v2, _) = classify(&pm("DarBot", "#room", "hi"), "darbot");
        assert_eq!(v2, Verdict::Ignore);
    }

    #[test]
    fn strip_mention_removes_address() {
        assert_eq!(strip_mention("darbot: do it", "darbot"), "do it");
        assert_eq!(strip_mention("darbot, please", "darbot"), "please");
        assert_eq!(strip_mention("no address here", "darbot"), "no address here");
    }

    #[test]
    fn conversation_keys_are_distinct_and_safe() {
        assert_eq!(Conversation::Channel("#room".into()).key(), "chan__room");
        assert_eq!(Conversation::Dm("alice".into()).key(), "dm_alice");
    }

    #[test]
    fn reply_target_routes_correctly() {
        assert_eq!(
            Conversation::Channel("#room".into()).reply_target("alice"),
            "#room"
        );
        assert_eq!(
            Conversation::Dm("alice".into()).reply_target("alice"),
            "alice"
        );
    }
}
