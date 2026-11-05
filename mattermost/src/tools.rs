use crate::{api::MattermostClient, config::MattermostConfig};
use anyhow::Result;
use async_trait::async_trait;
use dar_extension_sdk::{
    deliver::{DeliverySink, Destination},
    tools::{ToolExecutor, ToolOutcome, ToolSpec},
};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub enum ToolKind {
    Send,
    Users,
    Channels,
}
pub struct MattermostTool {
    client: MattermostClient,
    config: MattermostConfig,
    kind: ToolKind,
}
impl MattermostTool {
    pub fn new(client: MattermostClient, config: MattermostConfig, kind: ToolKind) -> Arc<Self> {
        Arc::new(Self {
            client,
            config,
            kind,
        })
    }
}
#[async_trait]
impl ToolExecutor for MattermostTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        match self.kind {
            ToolKind::Send => {
                let channel = args.get("channel").and_then(Value::as_str).filter(|value| !value.trim().is_empty());
                let user = args.get("user").and_then(Value::as_str).filter(|value| !value.trim().is_empty());
                if channel.is_some() == user.is_some() || !args.get("text").and_then(Value::as_str).is_some_and(|value| !value.trim().is_empty()) {
                    return Ok(ToolOutcome::error_code("invalid_args", "Provide non-empty text and exactly one channel or user.", None::<String>));
                }
                let resolved = match user {
                    Some(user) => self.resolve_user(user).await,
                    None => self.resolve_channel(channel.unwrap_or_default()).await,
                };
                let destination = match resolved {
                    Ok(channel) => channel,
                    Err(error) => return Ok(*error),
                };
                match self.outbound_allowed(&destination).await {
                    Ok(true) => {},
                    Ok(false) => return Ok(not_allowed()),
                    Err(error) => return Ok(api_error("mattermost_resolve_failed",error)),
                }
                let text = args["text"].as_str().unwrap_or_default();
                let mut last_post = String::new();
                for chunk in crate::markdown::chunk(&crate::markdown::render(text)) {
                    match self.client.create_post(&destination.id,&chunk,args.get("rootId").and_then(Value::as_str),&[]).await {
                        Ok(post) => last_post = post.id,
                        Err(error) => return Ok(api_error("mattermost_send_failed",error)),
                    }
                }
                Ok(ToolOutcome::ok(format!("sent Mattermost message {last_post}")))
            }
            ToolKind::Users => {
                match self.client.list_users(limit(&args),query(&args)).await {
                    Ok(users) => Ok(ToolOutcome::ok(Value::Array(users.into_iter().map(|user| json!({"id":user.id,"username":user.username,"name":user.name(),"bot":user.is_bot})).collect()).to_string())),
                    Err(error) => Ok(api_error("mattermost_list_users_failed",error)),
                }
            }
            ToolKind::Channels => {
                match self.client.list_channels(limit(&args),query(&args)).await {
                    Ok(channels) => Ok(ToolOutcome::ok(Value::Array(channels.into_iter().map(|channel| json!({"id":channel.id,"name":channel.name,"displayName":channel.display_name,"type":channel.kind,"teamId":channel.team_id})).collect()).to_string())),
                    Err(error) => Ok(api_error("mattermost_list_channels_failed",error)),
                }
            },
        }
    }
}
impl MattermostTool {
    async fn resolve_user(
        &self,
        value: &str,
    ) -> std::result::Result<crate::api::Channel, Box<ToolOutcome>> {
        if !self.config.dm.enabled {
            return Err(Box::new(not_allowed()));
        }
        let result = if valid_id(value) {
            self.client.user(value).await
        } else {
            self.client
                .username(&value.trim_start_matches('@').to_ascii_lowercase())
                .await
        };
        let user =
            result.map_err(|error| Box::new(api_error("mattermost_resolve_failed", error)))?;
        if !crate::config::allowed(&self.config.dm.users, &user.id, &user.username) {
            return Err(Box::new(not_allowed()));
        }
        let me = self
            .client
            .me()
            .await
            .map_err(|error| Box::new(api_error("mattermost_resolve_failed", error)))?;
        self.client
            .open_dm(&me.id, &user.id)
            .await
            .map_err(|error| Box::new(api_error("mattermost_open_dm_failed", error)))
    }
    async fn resolve_channel(
        &self,
        value: &str,
    ) -> std::result::Result<crate::api::Channel, Box<ToolOutcome>> {
        if valid_id(value) {
            return self
                .client
                .channel(value)
                .await
                .map_err(|error| Box::new(api_error("mattermost_resolve_failed", error)));
        }
        let name = value.trim_start_matches(['#', '~']);
        let channels = self
            .client
            .list_channels(usize::MAX, None)
            .await
            .map_err(|error| Box::new(api_error("mattermost_resolve_failed", error)))?;
        let mut matches = channels.into_iter().filter(|channel| {
            channel.name.eq_ignore_ascii_case(name)
                || channel.display_name.eq_ignore_ascii_case(name)
        });
        let Some(channel) = matches.next() else {
            return Err(Box::new(ToolOutcome::error_code(
                "channel_not_found",
                "Mattermost channel was not found.",
                None::<String>,
            )));
        };
        if matches.next().is_some() {
            return Err(Box::new(ToolOutcome::error_code(
                "ambiguous_channel",
                "Several channels match; use a channel id.",
                None::<String>,
            )));
        }
        Ok(channel)
    }
    async fn outbound_allowed(
        &self,
        channel: &crate::api::Channel,
    ) -> std::result::Result<bool, crate::api::ApiError> {
        if channel.kind != "D" {
            return Ok(self
                .config
                .channels
                .as_ref()
                .is_none_or(|channels| channels.contains_key(&channel.id)));
        }
        if !self.config.dm.enabled {
            return Ok(false);
        }
        if self.config.dm.users.is_empty() {
            return Ok(true);
        }
        let me = self.client.me().await?;
        let Some((one, two)) = channel.name.split_once("__") else {
            return Ok(false);
        };
        let other = if one == me.id {
            two
        } else if two == me.id {
            one
        } else {
            return Ok(false);
        };
        let user = self.client.user(other).await?;
        Ok(crate::config::allowed(
            &self.config.dm.users,
            &user.id,
            &user.username,
        ))
    }
}

#[async_trait]
impl DeliverySink for MattermostTool {
    async fn deliver(&self, destination: &Destination, text: &str) -> Result<()> {
        let mut args = json!({"text":text});
        if let Some(channel) = &destination.channel {
            args["channel"] = channel.clone().into();
        }
        if let Some(user) = &destination.user {
            args["user"] = user.clone().into();
        }
        let result = self.execute(args).await?;
        if result.is_error {
            anyhow::bail!("{}", result.text);
        }
        Ok(())
    }
}
fn limit(args: &Value) -> usize {
    args.get("limit")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(100)
}
fn query(args: &Value) -> Option<&str> {
    args.get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
fn not_allowed() -> ToolOutcome {
    ToolOutcome::error_code(
        "not_allowed",
        "Mattermost destination is not allowed by extension config.",
        None::<String>,
    )
}
fn valid_id(value: &str) -> bool {
    value.len() == 26
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}
fn api_error(code: &str, error: crate::api::ApiError) -> ToolOutcome {
    ToolOutcome::error_code(code, error.to_string(), None::<String>)
}
pub fn specs() -> [ToolSpec; 3] {
    [
        ToolSpec::new("mattermost_send_message", "Send a Mattermost channel message or direct message, optionally in a thread.", json!({"type":"object","additionalProperties":false,"properties":{"channel":{"type":"string"},"user":{"type":"string"},"text":{"type":"string","minLength":1},"rootId":{"type":"string"}},"required":["text"],"oneOf":[{"required":["channel"]},{"required":["user"]}]})).writes(),
        ToolSpec::new("mattermost_list_users", "List Mattermost users and bots visible to the bot.", json!({"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}}})),
        ToolSpec::new("mattermost_list_channels", "List channels the Mattermost bot can see.", json!({"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}}})),
    ]
}
