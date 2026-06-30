//! IRC connection configuration, parsed from `extensions.irc` in `agent.yaml`
//! with per-field fallback to `IRC_*` environment variables.

use std::collections::BTreeMap;

use serde::Deserialize;

/// Default secure IRC port (TLS).
pub const DEFAULT_PORT: u16 = 6697;
/// Default hard cap on consecutive bot-authored turns with no human message.
pub const DEFAULT_MAX_BOT_TURNS: u32 = 4;
/// Default number of ambient (context-only) messages retained per conversation.
pub const DEFAULT_CONTEXT_WINDOW: usize = 30;

/// A channel entry: the name to JOIN and an optional per-channel mention-gating
/// override. When `mention_required` is `None` the global default (or `true`)
/// applies.
#[derive(Clone, Debug, Default)]
pub struct ChannelEntry {
    pub name: String,
    pub mention_required: Option<bool>,
}

/// Connection + behaviour settings for the IRC channel extension.
///
/// Every field is optional/empty by default; resolver methods
/// (`effective_port`, `effective_max_bot_turns`, `effective_context_window`,
/// `tls`) supply the documented defaults. Keeping the numeric fields `Option`
/// (rather than defaulting them in the struct) lets `with_env_fallbacks`
/// distinguish "unset in yaml" from "explicitly set to the default value", so an
/// explicit `max_bot_turns: 4` is never silently clobbered by an env var.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct IrcConfig {
    /// IRC server hostname. Falls back to `IRC_SERVER`.
    pub server: Option<String>,
    /// IRC server port. Falls back to `IRC_PORT` only when unset in yaml.
    /// Defaults to 6697. Use [`IrcConfig::effective_port`] for the resolved value.
    pub(crate) port: Option<u16>,
    /// Connect over TLS. Falls back to `IRC_TLS` only when unset in yaml.
    /// Defaults to true. Use [`IrcConfig::tls`] for the resolved value.
    pub(crate) tls: Option<bool>,
    /// Desired nick. Falls back to `IRC_NICK`.
    pub nick: Option<String>,
    /// USER username. Falls back to `IRC_USERNAME`; defaults to nick.
    pub username: Option<String>,
    /// USER realname. Falls back to `IRC_REALNAME`; defaults to nick.
    pub realname: Option<String>,
    /// Server password (`PASS`). Falls back to `IRC_SERVER_PASSWORD`.
    pub server_password: Option<String>,
    /// NickServ password for `IDENTIFY`. Falls back to `IRC_NICKSERV_PASSWORD`.
    pub nickserv_password: Option<String>,
    /// Channels to join, with optional per-channel mention-gating. Accepts a
    /// list of channel names (every channel inherits the global default) or a
    /// map of `channel -> { mention_required: bool }` for per-channel overrides.
    /// Falls back to `IRC_CHANNELS` (comma-separated list form).
    #[serde(default, deserialize_with = "deserialize_channels")]
    pub channels: Vec<ChannelEntry>,
    /// Global default for whether a channel mention is required before the bot
    /// replies. Per-channel `mention_required` overrides this. Resolved default
    /// is `true` (fail-safe to quiet). Falls back to `IRC_MENTION_REQUIRED`.
    pub(crate) mention_required: Option<bool>,
    /// DM nick allowlist (case-insensitive); empty = anyone. Falls back to
    /// `IRC_ALLOWED_USERS` (comma-separated). Strictly a DM authorization gate;
    /// never used to classify channel humans vs. bots (see `humans`).
    pub allowed_users: Vec<String>,
    /// Channel human nicks (case-insensitive) for the loop-guard. Senders NOT on
    /// this list are treated as bots, so the consecutive-bot-turn cap engages.
    /// Empty (the default) means "no known humans" => every channel sender counts
    /// toward the cap, keeping the non-negotiable guarantee fail-closed: the cap
    /// can never be silently disabled. Falls back to `IRC_HUMANS` (comma-sep).
    pub humans: Vec<String>,
    /// Chat backend service id to drive; defaults to the bundled `irc-pi`.
    /// Falls back to `IRC_BACKEND`.
    pub backend: Option<String>,
    /// Hard cap on consecutive bot turns with no human message. Falls back to
    /// `IRC_MAX_BOT_TURNS` only when unset in yaml. Use
    /// [`IrcConfig::effective_max_bot_turns`] for the resolved value.
    pub(crate) max_bot_turns: Option<u32>,
    /// Ambient context messages retained per conversation. Falls back to
    /// `IRC_CONTEXT_WINDOW` only when unset in yaml. Use
    /// [`IrcConfig::effective_context_window`] for the resolved value.
    pub(crate) context_window: Option<usize>,
    /// Send an immediate `👀` acknowledgement the moment a human's message is
    /// picked up for a reply (before the agent turn runs). Falls back to
    /// `IRC_ACK` only when unset in yaml. Defaults to true. Use
    /// [`IrcConfig::effective_ack`] for the resolved value.
    pub(crate) ack: Option<bool>,
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_list(key: &str) -> Vec<String> {
    env_opt(key)
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn deserialize_channels<'de, D>(deserializer: D) -> Result<Vec<ChannelEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(serde::Deserialize, Default)]
    struct ChannelSettings {
        #[serde(default)]
        mention_required: Option<bool>,
    }

    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum ChannelList {
        List(Vec<String>),
        Map(BTreeMap<String, ChannelSettings>),
    }

    match ChannelList::deserialize(deserializer)? {
        ChannelList::List(names) => Ok(names
            .into_iter()
            .map(|name| ChannelEntry {
                name,
                mention_required: None,
            })
            .collect()),
        ChannelList::Map(map) => Ok(map
            .into_iter()
            .map(|(name, settings)| ChannelEntry {
                name,
                mention_required: settings.mention_required,
            })
            .collect()),
    }
}

impl IrcConfig {
    /// Apply `IRC_*` env-var fallbacks to any field left unset by `agent.yaml`.
    pub fn with_env_fallbacks(mut self) -> Self {
        if self.server.is_none() {
            self.server = env_opt("IRC_SERVER");
        }
        if self.port.is_none() {
            self.port = env_opt("IRC_PORT").and_then(|p| p.parse().ok());
        }
        if self.tls.is_none() {
            self.tls = env_opt("IRC_TLS").and_then(|v| v.parse().ok());
        }
        if self.nick.is_none() {
            self.nick = env_opt("IRC_NICK");
        }
        if self.username.is_none() {
            self.username = env_opt("IRC_USERNAME");
        }
        if self.realname.is_none() {
            self.realname = env_opt("IRC_REALNAME");
        }
        if self.server_password.is_none() {
            self.server_password = env_opt("IRC_SERVER_PASSWORD");
        }
        if self.nickserv_password.is_none() {
            self.nickserv_password = env_opt("IRC_NICKSERV_PASSWORD");
        }
        if self.channels.is_empty() {
            self.channels = env_list("IRC_CHANNELS")
                .into_iter()
                .map(|name| ChannelEntry {
                    name,
                    mention_required: None,
                })
                .collect();
        }
        if self.mention_required.is_none() {
            self.mention_required =
                env_opt("IRC_MENTION_REQUIRED").and_then(|v| v.parse().ok());
        }
        if self.allowed_users.is_empty() {
            self.allowed_users = env_list("IRC_ALLOWED_USERS");
        }
        if self.humans.is_empty() {
            self.humans = env_list("IRC_HUMANS");
        }
        if self.backend.is_none() {
            self.backend = env_opt("IRC_BACKEND");
        }
        if self.max_bot_turns.is_none() {
            self.max_bot_turns = env_opt("IRC_MAX_BOT_TURNS").and_then(|v| v.parse().ok());
        }
        if self.context_window.is_none() {
            self.context_window = env_opt("IRC_CONTEXT_WINDOW").and_then(|v| v.parse().ok());
        }
        if self.ack.is_none() {
            self.ack = env_opt("IRC_ACK").and_then(|v| v.parse().ok());
        }
        self
    }

    /// Resolved TLS setting: the yaml/env value if set, else the default (true).
    pub fn tls(&self) -> bool {
        self.tls.unwrap_or(true)
    }

    /// Resolved port: the yaml/env value if set, else the default (6697).
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }

    /// Resolved hard cap on consecutive bot turns: the yaml/env value if set,
    /// else the default.
    pub fn effective_max_bot_turns(&self) -> u32 {
        self.max_bot_turns.unwrap_or(DEFAULT_MAX_BOT_TURNS)
    }

    /// Resolved ambient context window: the yaml/env value if set, else the
    /// default.
    pub fn effective_context_window(&self) -> usize {
        self.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW)
    }

    /// Resolved pickup-ack setting: the yaml/env value if set, else the default
    /// (true). When true, a `👀` is sent the instant a human's message is picked
    /// up for a reply.
    pub fn effective_ack(&self) -> bool {
        self.ack.unwrap_or(true)
    }

    /// USER username, defaulting to the nick.
    pub fn effective_username(&self) -> String {
        self.username
            .clone()
            .or_else(|| self.nick.clone())
            .unwrap_or_default()
    }

    /// USER realname, defaulting to the nick.
    pub fn effective_realname(&self) -> String {
        self.realname
            .clone()
            .or_else(|| self.nick.clone())
            .unwrap_or_default()
    }

    /// Resolved mention-gating for `channel`: the channel's own setting, else the
    /// top-level default, else true (fail-safe to quiet).
    pub fn mention_required_for(&self, channel: &str) -> bool {
        self.channels
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case(channel))
            .and_then(|e| e.mention_required)
            .or(self.mention_required)
            .unwrap_or(true)
    }

    /// Channel names to JOIN, in config order.
    pub fn channel_names(&self) -> impl Iterator<Item = &str> {
        self.channels.iter().map(|e| e.name.as_str())
    }
}

/// True if `nick` may DM the bot given the allowlist (case-insensitive;
/// empty allowlist permits anyone).
pub fn dm_authorized(nick: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    allowed.iter().any(|a| a.eq_ignore_ascii_case(nick))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_allowlist_permits_anyone() {
        assert!(dm_authorized("alice", &[]));
        assert!(dm_authorized("", &[]));
    }

    #[test]
    fn allowlist_gates_by_nick_case_insensitively() {
        let allowed = vec!["Alice".to_string(), "bob".to_string()];
        assert!(dm_authorized("alice", &allowed));
        assert!(dm_authorized("ALICE", &allowed));
        assert!(dm_authorized("BoB", &allowed));
        assert!(!dm_authorized("carol", &allowed));
    }

    #[test]
    fn defaults_are_sane() {
        let cfg = IrcConfig::default();
        assert_eq!(cfg.effective_port(), 6697);
        assert!(cfg.tls());
        assert_eq!(cfg.effective_max_bot_turns(), DEFAULT_MAX_BOT_TURNS);
        assert_eq!(cfg.effective_context_window(), DEFAULT_CONTEXT_WINDOW);
        assert!(cfg.channels.is_empty());
    }

    #[test]
    fn explicit_default_valued_numerics_survive_env_fallback() {
        // An operator who explicitly writes the default value in yaml must NOT be
        // silently overridden by a concurrent env var. This protects the
        // non-negotiable loop-guard: `max_bot_turns: 4` (== default) must stay 4
        // even if `IRC_MAX_BOT_TURNS=0` is set in the environment.
        let prev = std::env::var("IRC_MAX_BOT_TURNS").ok();
        // SAFETY: tests in this module that touch this env var are serialized by
        // running them in one process; we restore the prior value below.
        unsafe {
            std::env::set_var("IRC_MAX_BOT_TURNS", "0");
        }
        let cfg = IrcConfig {
            max_bot_turns: Some(DEFAULT_MAX_BOT_TURNS), // explicit, equals default
            port: Some(DEFAULT_PORT),
            context_window: Some(DEFAULT_CONTEXT_WINDOW),
            ..IrcConfig::default()
        }
        .with_env_fallbacks();
        assert_eq!(
            cfg.effective_max_bot_turns(),
            DEFAULT_MAX_BOT_TURNS,
            "explicit yaml value must not be clobbered by env"
        );
        unsafe {
            match prev {
                Some(v) => std::env::set_var("IRC_MAX_BOT_TURNS", v),
                None => std::env::remove_var("IRC_MAX_BOT_TURNS"),
            }
        }
    }

    #[test]
    fn explicit_tls_is_not_overridden_by_default_resolution() {
        // An explicit `tls: false` in yaml must survive resolution.
        let cfg = IrcConfig {
            tls: Some(false),
            ..IrcConfig::default()
        };
        assert!(!cfg.tls());
        // Unset yaml resolves to the default (true).
        let cfg = IrcConfig::default();
        assert!(cfg.tls());
    }

    #[test]
    fn username_realname_fall_back_to_nick() {
        let cfg = IrcConfig {
            nick: Some("darbot".into()),
            ..IrcConfig::default()
        };
        assert_eq!(cfg.effective_username(), "darbot");
        assert_eq!(cfg.effective_realname(), "darbot");
    }

    #[test]
    fn channels_list_form_parses() {
        let cfg: IrcConfig =
            serde_json::from_value(json!({"channels": ["#a", "#b"]})).unwrap();
        assert_eq!(cfg.channels.len(), 2);
        assert_eq!(cfg.channels[0].name, "#a");
        assert!(cfg.channels[0].mention_required.is_none());
        assert_eq!(cfg.channels[1].name, "#b");
        assert!(cfg.channels[1].mention_required.is_none());
        // unset per-channel => falls back to global default => true
        assert!(cfg.mention_required_for("#a"));
    }

    #[test]
    fn channels_map_form_parses_and_overrides() {
        let cfg: IrcConfig = serde_json::from_value(json!({
            "mention_required": true,
            "channels": {
                "#team": {},
                "#public": { "mention_required": false }
            }
        }))
        .unwrap();
        assert!(cfg.mention_required_for("#team"));
        assert!(!cfg.mention_required_for("#public"));
        // case-insensitive lookup
        assert!(!cfg.mention_required_for("#PUBLIC"));
    }

    #[test]
    fn global_default_applies_to_unknown_and_map_channels() {
        let cfg: IrcConfig = serde_json::from_value(json!({
            "mention_required": false,
            "channels": { "#x": {} }
        }))
        .unwrap();
        // channel with no override inherits global false
        assert!(!cfg.mention_required_for("#x"));
        // completely unknown channel also gets global false
        assert!(!cfg.mention_required_for("#nope"));
    }

    #[test]
    fn unset_default_resolves_to_true() {
        let cfg = IrcConfig::default();
        assert!(cfg.mention_required_for("#any"));
    }

    #[test]
    fn ack_defaults_to_true_when_unset() {
        let cfg = IrcConfig::default();
        assert!(cfg.effective_ack());
    }

    #[test]
    fn ack_honors_explicit_false() {
        let cfg = IrcConfig {
            ack: Some(false),
            ..IrcConfig::default()
        };
        assert!(!cfg.effective_ack());
    }

    #[test]
    fn ack_honors_env_fallback() {
        // Env fallback only applies when unset in yaml. Serialize via the prior
        // value to keep the process-wide env clean for other tests.
        let prev = std::env::var("IRC_ACK").ok();
        // SAFETY: restored below; env access in this module is single-process.
        unsafe {
            std::env::set_var("IRC_ACK", "false");
        }
        let cfg = IrcConfig::default().with_env_fallbacks();
        assert!(!cfg.effective_ack(), "IRC_ACK=false must disable the ack");
        unsafe {
            match prev {
                Some(v) => std::env::set_var("IRC_ACK", v),
                None => std::env::remove_var("IRC_ACK"),
            }
        }
    }

    #[test]
    fn explicit_ack_is_not_overridden_by_env() {
        // An explicit yaml value must survive even with a conflicting env var.
        let prev = std::env::var("IRC_ACK").ok();
        // SAFETY: restored below; env access in this module is single-process.
        unsafe {
            std::env::set_var("IRC_ACK", "true");
        }
        let cfg = IrcConfig {
            ack: Some(false),
            ..IrcConfig::default()
        }
        .with_env_fallbacks();
        assert!(
            !cfg.effective_ack(),
            "explicit yaml ack:false must not be clobbered by env"
        );
        unsafe {
            match prev {
                Some(v) => std::env::set_var("IRC_ACK", v),
                None => std::env::remove_var("IRC_ACK"),
            }
        }
    }
}
