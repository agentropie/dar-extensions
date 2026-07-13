use std::collections::HashMap;

use anyhow::{bail, Result};
use dar_extension_sdk::ConfigStore;
use regex::Regex;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SlackConfig {
    pub token: Option<String>,
    #[serde(alias = "bot_token")]
    pub bot_token: Option<String>,
    pub app_token: Option<String>,
    pub channels: HashMap<String, ChannelConfig>,
    pub dm: DirectMessageConfig,
    pub mention_patterns: Vec<String>,
    pub show_thinking: bool,
    pub delete_thinking_on_complete: bool,
    /// Number of prior Slack turns injected into each prompt. Zero preserves
    /// AIHub compatibility: retain all locally buffered turns.
    pub history_limit: usize,
    /// Clear Slack-local context after successful agent reply and Slack post.
    pub clear_history_after_reply: bool,
}

impl Default for SlackConfig {
    fn default() -> Self {
        Self {
            token: None,
            bot_token: None,
            app_token: None,
            channels: HashMap::new(),
            dm: DirectMessageConfig::default(),
            mention_patterns: Vec::new(),
            show_thinking: false,
            delete_thinking_on_complete: true,
            history_limit: 20,
            clear_history_after_reply: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ChannelConfig {
    pub require_mention: bool,
    pub thread_policy: ThreadPolicy,
    pub users: Vec<String>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            require_mention: true,
            thread_policy: ThreadPolicy::Always,
            users: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DirectMessageConfig {
    pub enabled: bool,
    #[serde(alias = "allowFrom")]
    pub users: Vec<String>,
    pub thread_policy: ThreadPolicy,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ThreadPolicy {
    #[default]
    Always,
    Never,
    Follow,
}

#[derive(Clone)]
pub struct ResolvedTokens {
    pub bot: String,
    pub app: String,
}

impl std::fmt::Debug for ResolvedTokens {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedTokens")
            .field("bot", &"[redacted]")
            .field("app", &"[redacted]")
            .finish()
    }
}

impl SlackConfig {
    pub fn compiled_mention_patterns(&self) -> Result<Vec<Regex>> {
        self.mention_patterns
            .iter()
            .map(|pattern| Regex::new(pattern).map_err(Into::into))
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .mention_patterns
            .iter()
            .any(|pattern| pattern.len() > 512)
        {
            bail!("slack.mentionPatterns entries must be at most 512 bytes");
        }
        self.compiled_mention_patterns()?;
        Ok(())
    }

    pub fn tokens(&self) -> Result<ResolvedTokens> {
        let bot = nonempty(&self.token)
            .or_else(|| nonempty(&self.bot_token))
            .or_else(|| env("SLACK_BOT_TOKEN"));
        let app = nonempty(&self.app_token).or_else(|| env("SLACK_APP_TOKEN"));
        match (bot, app) {
            (Some(bot), Some(app)) => Ok(ResolvedTokens { bot, app }),
            (None, None) => bail!(
                "slack token and appToken are required: set extensions.slack.token and \
                 extensions.slack.appToken, or SLACK_BOT_TOKEN and SLACK_APP_TOKEN"
            ),
            (None, _) => bail!("slack token is required (set slack.token or SLACK_BOT_TOKEN)"),
            (_, None) => {
                bail!("slack appToken is required (set slack.appToken or SLACK_APP_TOKEN)")
            }
        }
    }
}

pub fn parse_config(config: &ConfigStore, id: &str) -> Result<SlackConfig> {
    let config = match config.get(id) {
        Some(value) => serde_json::from_value(value.clone())?,
        None => SlackConfig::default(),
    };
    config.validate()?;
    Ok(config)
}

fn nonempty(value: &Option<String>) -> Option<String> {
    value.clone().filter(|value| !value.trim().is_empty())
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_single_bot_behavior() {
        let config = SlackConfig::default();
        assert!(!config.dm.enabled);
        assert!(config.channels.is_empty());
        assert!(!config.show_thinking);
        assert_eq!(ChannelConfig::default().thread_policy, ThreadPolicy::Always);
    }

    #[test]
    fn validation_rejects_bad_pattern() {
        let config = SlackConfig {
            mention_patterns: vec!["[".into()],
            ..SlackConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn missing_tokens_do_not_echo_secret_values() {
        let error = SlackConfig::default().tokens().unwrap_err().to_string();
        assert!(error.contains("SLACK_BOT_TOKEN"));
        assert!(!error.contains("xoxb-"));
    }
}
