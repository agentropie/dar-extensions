use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct MattermostConfig {
    pub url: Option<String>,
    pub backend: Option<String>,
    pub ack_reaction: bool,
    pub ack_emoji: String,
    pub show_thinking: bool,
    pub delete_thinking_on_complete: bool,
    pub history_limit: usize,
    pub fetch_history: bool,
    pub clear_history_after_reply: bool,
    pub allow_bots: AllowBots,
    pub sessions: SessionsConfig,
    pub dm: DmConfig,
    pub channels: Option<HashMap<String, ChannelConfig>>,
}
impl Default for MattermostConfig {
    fn default() -> Self {
        Self {
            url: None,
            backend: None,
            ack_reaction: true,
            ack_emoji: "eyes".into(),
            show_thinking: false,
            delete_thinking_on_complete: true,
            history_limit: 20,
            fetch_history: true,
            clear_history_after_reply: false,
            allow_bots: AllowBots::default(),
            sessions: SessionsConfig::default(),
            dm: DmConfig::default(),
            channels: None,
        }
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum AllowBots {
    All(bool),
    Only(Vec<String>),
}
impl Default for AllowBots {
    fn default() -> Self {
        Self::All(false)
    }
}
impl AllowBots {
    pub fn allows(&self, id: &str) -> bool {
        match self {
            Self::All(all) => *all,
            Self::Only(ids) => ids.iter().any(|other| other == id),
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ThreadPolicy {
    Always,
    Never,
    Follow,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SessionsConfig {
    pub idle_minutes: u64,
}
impl Default for SessionsConfig {
    fn default() -> Self {
        Self { idle_minutes: 360 }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct DmConfig {
    pub enabled: bool,
    pub users: Vec<String>,
    pub thread_policy: ThreadPolicy,
}
impl Default for DmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            users: Vec::new(),
            thread_policy: ThreadPolicy::Follow,
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ChannelConfig {
    pub require_mention: bool,
    pub users: Vec<String>,
    pub thread_policy: ThreadPolicy,
}
impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            require_mention: true,
            users: Vec::new(),
            thread_policy: ThreadPolicy::Always,
        }
    }
}

#[derive(Clone)]
pub struct SecretToken(String);
impl SecretToken {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretToken([REDACTED])")
    }
}
#[derive(Clone, Debug)]
pub struct ResolvedConfig {
    pub url: String,
    pub token: SecretToken,
}
impl MattermostConfig {
    pub fn resolve(
        &self,
        url_env: Option<&str>,
        token_env: Option<&str>,
    ) -> anyhow::Result<ResolvedConfig> {
        let base = self
            .url
            .as_deref()
            .or(url_env)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("mattermost URL is required: set url or MATTERMOST_URL")
            })?;
        let mut url = url::Url::parse(base.trim()).map_err(|_| {
            anyhow::anyhow!("invalid MATTERMOST_URL: expected an http or https URL")
        })?;
        anyhow::ensure!(matches!(url.scheme(), "http" | "https") && url.host_str().is_some() && url.username().is_empty() && url.password().is_none() && url.query().is_none() && url.fragment().is_none(), "invalid MATTERMOST_URL: expected an http or https URL without credentials, query or fragment");
        let path = url
            .path()
            .trim_end_matches('/')
            .strip_suffix("/api/v4")
            .unwrap_or(url.path().trim_end_matches('/'))
            .to_owned();
        url.set_path(&path);
        let token = token_env.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
            anyhow::anyhow!("mattermost token is required: set MATTERMOST_BOT_TOKEN")
        })?;
        Ok(ResolvedConfig {
            url: url.as_str().trim_end_matches('/').to_owned(),
            token: SecretToken::new(token.to_owned()),
        })
    }
    pub fn credentials(&self) -> anyhow::Result<ResolvedConfig> {
        self.resolve(
            std::env::var("MATTERMOST_URL").ok().as_deref(),
            std::env::var("MATTERMOST_BOT_TOKEN").ok().as_deref(),
        )
    }
}
pub fn parse(
    config: &dar_extension_sdk::ConfigStore,
    id: &str,
) -> anyhow::Result<MattermostConfig> {
    Ok(config
        .get(id)
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default())
}
pub fn allowed(users: &[String], id: &str, username: &str) -> bool {
    users.is_empty()
        || users.iter().any(|entry| {
            entry == id || entry.trim_start_matches('@').eq_ignore_ascii_case(username)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_config_preserves_chat_behaviour() {
        let cfg: MattermostConfig = serde_json::from_str("{}").unwrap();
        assert!(cfg.ack_reaction);
        assert_eq!(cfg.ack_emoji, "eyes");
        assert!(!cfg.show_thinking);
        assert!(cfg.delete_thinking_on_complete);
        assert_eq!(cfg.history_limit, 20);
        assert!(cfg.fetch_history);
        assert!(!cfg.clear_history_after_reply);
        assert!(!cfg.allow_bots.allows("oooooooooooooooooooooooooo"));
        assert_eq!(cfg.sessions.idle_minutes, 360);
        assert!(cfg.dm.enabled);
        assert!(cfg.dm.users.is_empty());
        assert_eq!(cfg.dm.thread_policy, ThreadPolicy::Follow);
        assert!(cfg.channels.is_none());
        let channel: ChannelConfig = serde_json::from_str("{}").unwrap();
        assert!(channel.require_mention);
        assert_eq!(channel.thread_policy, ThreadPolicy::Always);
        assert!(channel.users.is_empty());
    }
    #[test]
    fn credentials_are_env_only_normalized_and_redacted() {
        let cfg = MattermostConfig::default();
        let resolved = cfg
            .resolve(
                Some("https://chat.example.com/api/v4/"),
                Some("invented-test-token"),
            )
            .unwrap();
        assert_eq!(resolved.url, "https://chat.example.com");
        assert_eq!(resolved.token.expose(), "invented-test-token");
        assert!(!format!("{resolved:?}").contains("invented-test-token"));
        assert!(cfg
            .resolve(Some("https://chat.example.com"), None)
            .unwrap_err()
            .to_string()
            .contains("MATTERMOST_BOT_TOKEN"));
        assert!(cfg
            .resolve(None, Some("invented-test-token"))
            .unwrap_err()
            .to_string()
            .contains("MATTERMOST_URL"));
        assert!(cfg
            .resolve(Some("file:///tmp/chat"), Some("invented-test-token"))
            .is_err());
        assert!(
            serde_json::from_str::<MattermostConfig>(r#"{"token":"invented-test-token"}"#).is_err()
        );
    }

    #[test]
    fn config_forms_reject_typos_and_resolve_explicit_values() {
        for value in [
            serde_json::json!({"unexpected":true}),
            serde_json::json!({"dm":{"userss":[]}}),
            serde_json::json!({"sessions":{"idleMinutess":1}}),
            serde_json::json!({"channels":{"cccccccccccccccccccccccccc":{"requireMentions":false}}}),
        ] {
            assert!(serde_json::from_value::<MattermostConfig>(value).is_err());
        }
        for (value, yes, no) in [
            (serde_json::json!(false), false, false),
            (serde_json::json!(true), true, true),
            (
                serde_json::json!(["aaaaaaaaaaaaaaaaaaaaaaaaaa"]),
                true,
                false,
            ),
        ] {
            let cfg: MattermostConfig =
                serde_json::from_value(serde_json::json!({"allowBots":value})).unwrap();
            assert_eq!(cfg.allow_bots.allows("aaaaaaaaaaaaaaaaaaaaaaaaaa"), yes);
            assert_eq!(cfg.allow_bots.allows("bbbbbbbbbbbbbbbbbbbbbbbbbb"), no);
        }
        let cfg: MattermostConfig = serde_json::from_value(serde_json::json!({"url":"http://chat.example.com/api/v4", "sessions":{"idleMinutes":0}, "dm":{"threadPolicy":"never"}, "channels":{}})).unwrap();
        assert_eq!(
            cfg.resolve(Some("https://ignored.example.com"), Some("invented"))
                .unwrap()
                .url,
            "http://chat.example.com"
        );
        assert_eq!(cfg.sessions.idle_minutes, 0);
        assert_eq!(cfg.dm.thread_policy, ThreadPolicy::Never);
        assert!(cfg.channels.unwrap().is_empty());
        let values = HashMap::from([("mattermost".into(), serde_json::json!({"backend":"pi"}))]);
        assert_eq!(
            parse(
                &dar_extension_sdk::ConfigStore::from_values(values),
                "mattermost"
            )
            .unwrap()
            .backend
            .as_deref(),
            Some("pi")
        );
        assert!(allowed(
            &["@ALICE".into()],
            "uuuuuuuuuuuuuuuuuuuuuuuuuu",
            "alice"
        ));
        assert!(allowed(
            &["uuuuuuuuuuuuuuuuuuuuuuuuuu".into()],
            "uuuuuuuuuuuuuuuuuuuuuuuuuu",
            "other"
        ));
        assert!(!allowed(
            &["alice".into()],
            "uuuuuuuuuuuuuuuuuuuuuuuuuu",
            "alice2"
        ));
    }
}
