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
///
/// `mention_required` controls whether a channel message must address the bot
/// by nick to get a `Reply` verdict. When `false`, every channel message (not
/// just mentions) returns `Reply`. Self-ignore and DM branches are unaffected.
pub fn classify(pm: &PrivMsg, bot_nick: &str, mention_required: bool) -> (Verdict, Conversation) {
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
        if !mention_required || is_mention(&pm.text, bot_nick) {
            (Verdict::Reply, conv)
        } else {
            (Verdict::ContextOnly, conv)
        }
    } else {
        // A DM (target is our own nick): always a reply, keyed by sender.
        (Verdict::Reply, Conversation::Dm(pm.sender.clone()))
    }
}

/// True if `text` addresses `bot_nick` at the start of the message
/// (case-insensitive), mirroring aihub's `isAddressed` regex
/// `^\s*nick\s*[:,]\s*` plus an `@nick` form. Concretely:
///
///   `\s* nick [:,]`      — the nick, optionally preceded by whitespace, must
///                          be followed by a `:` or `,` separator (itself
///                          optionally preceded by whitespace). E.g.
///                          `darbot: hi`, `darbot, hi`, `darbot , hi`.
///   `\s* @ nick <boundary>` — an `@`-prefixed nick just needs a boundary
///                          after it: end of string, whitespace, `:` or `,`.
///                          E.g. `@darbot`, `@darbot help`, `@darbot: help`.
///
/// A bare, unaddressed nick (`darbot do it`) and inline/mid-sentence mentions
/// (`hey darbot can you help`) are NOT mentions — the nick must lead the
/// message. Substrings never match (`darbotic things` is not a mention of
/// `darbot`) because the character right after the nick can't be ASCII
/// alphanumeric in either form.
pub fn is_mention(text: &str, bot_nick: &str) -> bool {
    if bot_nick.is_empty() {
        return false;
    }
    let trimmed = text.trim_start();
    let (has_at, rest) = match trimmed.strip_prefix('@') {
        Some(r) => (true, r),
        None => (false, trimmed),
    };
    // `get` returns None both when `rest` is too short and when the index
    // would land inside a multi-byte char; either way there's no mention.
    let Some(candidate) = rest.get(..bot_nick.len()) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(bot_nick) {
        return false;
    }
    let after = &rest[bot_nick.len()..];
    if has_at {
        // A boundary after the nick: end of string, whitespace, `:` or `,`.
        match after.chars().next() {
            None => true,
            Some(c) => c.is_whitespace() || c == ':' || c == ',',
        }
    } else {
        // No `@`: a `:` or `,` separator is required (optional whitespace before it).
        let after = after.trim_start();
        after.starts_with(':') || after.starts_with(',')
    }
}

/// Strip a leading address prefix from a mention so the turn sees just the
/// user's request. Handles:
///   `nick: text`  →  `text`
///   `nick, text`  →  `text`
///   `@nick: text` →  `text`
///   `@nick text`  →  `text`
///
/// If the nick is NOT at the very start (e.g. inline mention), the text is
/// returned unchanged.
pub fn strip_mention<'a>(text: &'a str, bot_nick: &str) -> &'a str {
    let t = text.trim_start();
    // Strip optional leading `@`.
    let after_at = t.strip_prefix('@').unwrap_or(t);
    // Check if the nick matches at the start (case-insensitive). `get` returns
    // None both when `after_at` is too short and when the index would land
    // inside a multi-byte char; either way the text is unchanged.
    let Some(candidate) = after_at.get(..bot_nick.len()) else {
        return t;
    };
    let rest = &after_at[bot_nick.len()..];
    if !candidate.eq_ignore_ascii_case(bot_nick)
        || rest
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
    {
        return t;
    }
    // After the nick: optional `:` or `,`, then trim whitespace.
    let rest = rest
        .strip_prefix(':')
        .or_else(|| rest.strip_prefix(','))
        .unwrap_or(rest);
    rest.trim_start()
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
        let (v, conv) = classify(&pm("alice", "darbot", "hello"), "darbot", true);
        assert_eq!(v, Verdict::Reply);
        assert_eq!(conv, Conversation::Dm("alice".into()));
    }

    #[test]
    fn channel_without_mention_is_context_only() {
        let (v, conv) = classify(&pm("alice", "#room", "just chatting"), "darbot", true);
        assert_eq!(v, Verdict::ContextOnly);
        assert_eq!(conv, Conversation::Channel("#room".into()));
    }

    #[test]
    fn channel_with_colon_mention_is_reply() {
        let (v, _) = classify(
            &pm("alice", "#room", "darbot: do the thing"),
            "darbot",
            true,
        );
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn channel_with_comma_mention_is_reply() {
        let (v, _) = classify(&pm("alice", "#room", "darbot, hi"), "darbot", true);
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn mention_is_case_insensitive() {
        let (v, _) = classify(&pm("alice", "#room", "DARBOT: hey"), "darbot", true);
        assert_eq!(v, Verdict::Reply);
    }

    #[test]
    fn substring_nick_is_not_a_mention() {
        // "darbotic" must not count as addressing "darbot".
        let (v, _) = classify(&pm("alice", "#room", "darbotic things"), "darbot", true);
        assert_eq!(v, Verdict::ContextOnly);
    }

    #[test]
    fn self_authored_is_ignored() {
        let (v, _) = classify(
            &pm("darbot", "#room", "darbot: my own message"),
            "darbot",
            true,
        );
        assert_eq!(v, Verdict::Ignore);
        let (v2, _) = classify(&pm("DarBot", "#room", "hi"), "darbot", true);
        assert_eq!(v2, Verdict::Ignore);
    }

    #[test]
    fn channel_without_mention_and_gate_off_is_reply() {
        let (v, conv) = classify(&pm("alice", "#room", "just chatting"), "darbot", false);
        assert_eq!(v, Verdict::Reply);
        assert_eq!(conv, Conversation::Channel("#room".into()));
    }

    #[test]
    fn channel_without_mention_and_gate_on_is_context_only() {
        let (v, _) = classify(&pm("alice", "#room", "just chatting"), "darbot", true);
        assert_eq!(v, Verdict::ContextOnly);
    }

    #[test]
    fn strip_mention_removes_address() {
        assert_eq!(strip_mention("darbot: do it", "darbot"), "do it");
        assert_eq!(strip_mention("darbot, please", "darbot"), "please");
        assert_eq!(
            strip_mention("no address here", "darbot"),
            "no address here"
        );
        // Substring of the nick must not be stripped.
        assert_eq!(
            strip_mention("darbotic things", "darbot"),
            "darbotic things"
        );
    }

    #[test]
    fn at_nick_leading_is_a_mention() {
        // `@dale you here?` is a mention of `dale`.
        assert!(is_mention("@dale you here?", "dale"));
        // `@darbot` alone is a mention of `darbot`.
        assert!(is_mention("@darbot", "darbot"));
    }

    #[test]
    fn inline_nick_is_not_a_mention_anymore() {
        // Nick appearing mid-sentence (not addressing the bot) is no longer a mention.
        assert!(!is_mention("yo dale you here?", "dale"));
        assert!(!is_mention("hey darbot can you help", "darbot"));
    }

    #[test]
    fn inline_nick_substring_is_not_a_mention() {
        // "darbotic" must not count as addressing "darbot".
        assert!(!is_mention("darbotic things", "darbot"));
    }

    #[test]
    fn leading_whitespace_addressing_is_a_mention() {
        assert!(is_mention("  darbot: hi", "darbot"));
    }

    #[test]
    fn bare_leading_nick_without_separator_is_not_a_mention() {
        assert!(!is_mention("darbot do it", "darbot"));
    }

    #[test]
    fn at_prefixed_substring_is_not_a_mention() {
        assert!(!is_mention("@darbotic", "darbot"));
    }

    #[test]
    fn strip_mention_at_prefix_forms() {
        // `@dale: do it` → `do it`
        assert_eq!(strip_mention("@dale: do it", "dale"), "do it");
        // `@dale do it` → `do it`
        assert_eq!(strip_mention("@dale do it", "dale"), "do it");
        // Inline nick: text is unchanged.
        assert_eq!(
            strip_mention("yo dale you here?", "dale"),
            "yo dale you here?"
        );
    }

    #[test]
    fn conversation_keys_are_distinct_and_safe() {
        assert_eq!(Conversation::Channel("#room".into()).key(), "chan__room");
        assert_eq!(Conversation::Dm("alice".into()).key(), "dm_alice");
    }

    #[test]
    fn is_mention_on_non_char_boundary_does_not_panic() {
        // "Findev" is 6 bytes, but byte index 6 in this message falls inside
        // the 3-byte em dash "—" (bytes 4..7). A naive `split_at` would panic.
        assert!(!is_mention("Yes — saw it. Findev reported:", "Findev"));
    }

    #[test]
    fn strip_mention_on_non_char_boundary_returns_unchanged() {
        let text = "Yes — saw it. Findev reported:";
        assert_eq!(strip_mention(text, "Findev"), text);
    }

    #[test]
    fn is_mention_on_short_multibyte_message_does_not_panic() {
        // Already handled by the length guard, but cheap to pin.
        assert!(!is_mention("👀", "Findev"));
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
