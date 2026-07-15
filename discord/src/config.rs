use anyhow::Result;
use dar_extension_sdk::ConfigStore;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordConfig {
    pub bot_token: Option<String>,
    pub backend: Option<String>,
    pub ack_emoji: String,
    pub history_limit: usize,
    pub clear_history_after_reply: bool,
    pub guilds: HashMap<String, GuildConfig>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            bot_token: None,
            backend: None,
            ack_emoji: "👀".into(),
            history_limit: 20,
            clear_history_after_reply: false,
            guilds: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GuildConfig {
    pub enabled: bool,
    pub users: Vec<String>,
    pub channels: HashMap<String, ChannelConfig>,
}

impl Default for GuildConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            users: Vec::new(),
            channels: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChannelConfig {
    pub enabled: bool,
    pub require_mention: bool,
    pub users: Vec<String>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_mention: true,
            users: Vec::new(),
        }
    }
}

pub fn parse(config: &ConfigStore, id: &str) -> Result<DiscordConfig> {
    let config = config
        .get(id)
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    Ok(config)
}
pub fn token(config: &DiscordConfig) -> Result<String> {
    config.bot_token.clone().filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("DISCORD_BOT_TOKEN").ok().filter(|value| !value.trim().is_empty()))
        .ok_or_else(|| anyhow::anyhow!("discord.bot_token is required: set extensions.discord.bot_token in agent.yaml or the DISCORD_BOT_TOKEN environment variable"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_token_is_clear() {
        assert!(token(&DiscordConfig::default())
            .unwrap_err()
            .to_string()
            .contains("DISCORD_BOT_TOKEN"));
    }
    #[test]
    fn rejects_unknown_config() {
        assert!(serde_json::from_value::<DiscordConfig>(serde_json::json!({"nope":true})).is_err());
    }
    #[test]
    fn parses_extension_config() {
        let mut values = std::collections::HashMap::new();
        values.insert(
            "discord".into(),
            serde_json::json!({"bot_token":"configured","backend":"pi"}),
        );
        let parsed = parse(&ConfigStore::from_values(values), "discord").unwrap();
        assert_eq!(parsed.bot_token.as_deref(), Some("configured"));
        assert_eq!(parsed.backend.as_deref(), Some("pi"));
    }
}
