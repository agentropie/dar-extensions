use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dar_extension_sdk::deliver::{DeliverySink, Destination};
use dar_extension_sdk::tools::{ToolExecutor, ToolOutcome, ToolSpec};
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

pub fn list_users_spec() -> ToolSpec {
    ToolSpec::new(
        "discord_list_users",
        "List members (humans and bots) of the configured Discord guilds with their user IDs. To mention a user or bot, write <@id> in your message; a bare name does not notify them.",
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "query": {"type": "string", "description": "Case-insensitive name filter."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Max users to return (default 100)."}
            }
        }),
    )
}

/// Lists guild members for `discord_list_users`.
pub struct DiscordListUsersTool(pub Arc<DiscordSendTool>);

#[async_trait]
impl ToolExecutor for DiscordListUsersTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        let query = args.get("query").and_then(Value::as_str).unwrap_or("");
        let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let mut users = Vec::new();
        for guild_id in self.0.config.guilds.keys() {
            match self.0.guild_members(guild_id).await {
                Ok(members) => users.extend(select_members(&members, guild_id, query)),
                Err(error) if error.to_string().starts_with("HTTP 403") => {
                    return Ok(api_error(
                        "missing_members_intent",
                        "Discord refused the member list; enable the Server Members Intent for this bot in the Discord developer portal",
                        error,
                    ))
                }
                Err(error) => {
                    return Ok(api_error("discord_list_failed", "Discord member list failed", error))
                }
            }
            if users.len() >= limit {
                break;
            }
        }
        users.truncate(limit);
        Ok(ToolOutcome::ok(Value::Array(users).to_string()))
    }
}

/// `{id, name, guildId, bot?}` for members matching `query` (bots included).
fn select_members(members: &[Value], guild_id: &str, query: &str) -> Vec<Value> {
    let query = query.to_lowercase();
    members
        .iter()
        .filter_map(|member| {
            let user = &member["user"];
            let id = user["id"].as_str()?;
            let name = member["nick"]
                .as_str()
                .or(user["global_name"].as_str())
                .or(user["username"].as_str())
                .unwrap_or(id);
            let names = [name, user["username"].as_str().unwrap_or("")];
            if !query.is_empty() && !names.iter().any(|n| n.to_lowercase().contains(&query)) {
                return None;
            }
            let mut out = json!({"id": id, "name": name, "guildId": guild_id});
            if user["bot"].as_bool().unwrap_or(false) {
                out["bot"] = json!(true);
            }
            Some(out)
        })
        .collect()
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

    async fn guild_members(&self, guild_id: &str) -> Result<Vec<Value>> {
        let mut members = Vec::new();
        let mut after = "0".to_owned();
        loop {
            let page = self
                .request(
                    "GET",
                    &format!("guilds/{guild_id}/members?limit=1000&after={after}"),
                    None,
                )
                .await?;
            let page = page.as_array().cloned().unwrap_or_default();
            let full = page.len() == 1000;
            match page.last().and_then(|m| m["user"]["id"].as_str()) {
                Some(last) if full => after = last.to_owned(),
                _ => {
                    members.extend(page);
                    return Ok(members);
                }
            }
            members.extend(page);
        }
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
        if let Some(channel) = &dest.channel {
            args["channel"] = json!(channel);
        }
        if let Some(user) = &dest.user {
            args["user"] = json!(user);
        }
        let outcome = self.execute(args).await?;
        if outcome.is_error {
            anyhow::bail!("{}", outcome.text);
        }
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

    #[test]
    fn list_users_includes_bots_and_filters_by_name() {
        let members = vec![
            json!({"user": {"id": "1", "username": "iris", "bot": true}}),
            json!({"user": {"id": "2", "username": "thinh", "global_name": "Thinh"}}),
            json!({"nick": "Pom", "user": {"id": "3", "username": "pom_bot", "bot": true}}),
        ];
        let all = select_members(&members, "g", "");
        assert_eq!(all.len(), 3);
        assert_eq!(
            all[0],
            json!({"id": "1", "name": "iris", "guildId": "g", "bot": true})
        );
        assert_eq!(all[1], json!({"id": "2", "name": "Thinh", "guildId": "g"}));
        assert_eq!(
            select_members(&members, "g", "POM"),
            [json!({"id": "3", "name": "Pom", "guildId": "g", "bot": true})]
        );
    }
}
