use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dar_extension_sdk::tools::{ToolExecutor, ToolOutcome, ToolSpec};
use dar_extension_sdk::deliver::{DeliverySink, Destination};
use serde_json::{json, Value};

use crate::config::DiscordConfig;

const API: &str = "https://discord.com/api/v10";
const MAX_TEXT: usize = 2_000;

pub fn spec() -> ToolSpec {
    ToolSpec::new(
        "discord_send_message",
        "Send a Discord message to a configured channel (by name or ID) or direct-message a user ID.",
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "channel": {"type": "string", "minLength": 1, "description": "Configured Discord channel name or ID."},
                "user": {"type": "string", "minLength": 1, "description": "Discord user ID to DM; opens a DM channel automatically."},
                "text": {"type": "string", "minLength": 1, "maxLength": 2000, "description": "Message text."}
            },
            "required": ["text"],
            "oneOf": [{"required": ["channel"]}, {"required": ["user"]}]
        }),
    )
    .writes()
}

pub struct DiscordSendTool {
    client: reqwest::Client,
    token: String,
    config: DiscordConfig,
}

impl DiscordSendTool {
    pub fn new(token: String, config: DiscordConfig) -> Arc<Self> {
        Arc::new(Self {
            client: reqwest::Client::new(),
            token,
            config,
        })
    }

    async fn send(&self, args: Value) -> Result<ToolOutcome> {
        let Some(text) = args
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty() && text.chars().count() <= MAX_TEXT)
        else {
            return Ok(invalid(
                "discord_send_message requires non-empty text up to 2000 characters",
            ));
        };
        let channel = args
            .get("channel")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let user = args
            .get("user")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let destination = match (channel, user) {
            (Some(_), Some(_)) | (None, None) => {
                return Ok(invalid(
                    "discord_send_message requires exactly one of channel or user",
                ))
            }
            (Some(channel), None) => self.resolve_channel(channel).await,
            (None, Some(user)) => self.open_dm(user).await,
        };
        let destination = match destination {
            Ok(destination) => destination,
            Err(error) => return Ok(api_error("invalid_target", "Discord target invalid", error)),
        };
        self.request(
            "POST",
            &format!("channels/{destination}/messages"),
            Some(json!({"content": text})),
        )
        .await
        .map(|_| ToolOutcome::ok(format!("sent Discord message to {destination}")))
        .or_else(|error| {
            Ok(api_error(
                "discord_send_failed",
                "Discord send failed",
                error,
            ))
        })
    }

    async fn resolve_channel(&self, target: &str) -> Result<String> {
        if self
            .config
            .guilds
            .values()
            .any(|guild| guild.channels.contains_key(target))
        {
            return Ok(target.to_owned());
        }
        let name = target.trim_start_matches('#');
        let mut matches = Vec::new();
        for (guild_id, guild) in &self.config.guilds {
            let channels = self
                .request("GET", &format!("guilds/{guild_id}/channels"), None)
                .await?;
            let Some(channels) = channels.as_array() else {
                continue;
            };
            matches.extend(channels.iter().filter_map(|channel| {
                let id = channel.get("id")?.as_str()?;
                (channel.get("name")?.as_str()? == name && guild.channels.contains_key(id))
                    .then(|| id.to_owned())
            }));
        }
        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => {
                anyhow::bail!("channel '{target}' was not found among configured Discord channels")
            }
            _ => anyhow::bail!("channel name '{target}' is ambiguous; use its Discord channel ID"),
        }
    }

    async fn open_dm(&self, user: &str) -> Result<String> {
        if !snowflake(user) {
            anyhow::bail!("Discord user must be a numeric user ID")
        }
        let channel = self
            .request(
                "POST",
                "users/@me/channels",
                Some(json!({"recipient_id": user})),
            )
            .await?;
        channel
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("Discord did not return a DM channel ID"))
    }

    async fn request(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
        let method = method.parse()?;
        let mut request = self
            .client
            .request(method, format!("{API}/{path}"))
            .header("Authorization", format!("Bot {}", self.token));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            anyhow::bail!("HTTP {status}: {body}");
        }
        Ok(serde_json::from_str(&body)?)
    }
}

#[async_trait]
impl ToolExecutor for DiscordSendTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        self.send(args).await
    }
}

#[async_trait]
impl DeliverySink for DiscordSendTool {
    async fn deliver(&self, dest: &Destination, text: &str) -> Result<()> {
        let mut args = json!({"text": text});
        if let Some(channel) = &dest.channel { args["channel"] = json!(channel); }
        if let Some(user) = &dest.user { args["user"] = json!(user); }
        let outcome = self.execute(args).await?;
        if outcome.is_error { anyhow::bail!("{}", outcome.text); }
        Ok(())
    }
}

fn snowflake(value: &str) -> bool {
    value.len() <= 20 && !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}
fn invalid(message: &str) -> ToolOutcome {
    ToolOutcome::error_code("invalid_args", message, None::<String>)
}
fn api_error(code: &str, prefix: &str, error: anyhow::Error) -> ToolOutcome {
    ToolOutcome::error_code(code, format!("{prefix}: {error}"), None::<String>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_requires_one_target() {
        let schema = spec().input_schema;
        assert_eq!(schema["oneOf"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn user_ids_are_discord_snowflakes() {
        assert!(snowflake("123456789012345678"));
        assert!(!snowflake("alice"));
        assert!(!snowflake(""));
    }

    #[tokio::test]
    async fn invalid_targets_return_agent_errors() {
        let tool = DiscordSendTool::new("token".into(), DiscordConfig::default());
        let missing = tool.execute(json!({"text": "hello"})).await.unwrap();
        assert!(missing.is_error);
        assert_eq!(missing.error.unwrap().code, "invalid_args");
        let invalid_user = tool
            .execute(json!({"user": "alice", "text": "hello"}))
            .await
            .unwrap();
        assert!(invalid_user.is_error);
        assert_eq!(invalid_user.error.unwrap().code, "invalid_target");
    }
}
