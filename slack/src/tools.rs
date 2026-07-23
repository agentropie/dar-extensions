use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dar_extension_sdk::tools::{ToolExecutor, ToolOutcome, ToolSpec};
use dar_extension_sdk::deliver::{DeliverySink, Destination};
use serde_json::{json, Value};

use crate::{api::SlackClient, config::SlackConfig};

const MAX_TEXT: usize = 40_000;
const MAX_RESULTS: usize = 200;

pub fn specs() -> [ToolSpec; 3] {
    [
        ToolSpec::new(
            "slack_send_message",
            "Send a Slack message to an exact allowed channel or direct-message conversation.",
            json!({"type":"object","additionalProperties":false,"properties":{"channel":{"type":"string","minLength":1},"text":{"type":"string","minLength":1},"threadTs":{"type":"string"},"thread_ts":{"type":"string"}},"required":["channel","text"]}),
        )
        .writes(),
        ToolSpec::new(
            "slack_list_users",
            "List active Slack users visible to configured bot.",
            json!({"type":"object","additionalProperties":false,"properties":{"query":{"type":"string","maxLength":256},"limit":{"type":"integer","minimum":1,"maximum":200}}}),
        ),
        ToolSpec::new(
            "slack_list_channels",
            "List active Slack channels visible to configured bot.",
            json!({"type":"object","additionalProperties":false,"properties":{"query":{"type":"string","maxLength":256},"limit":{"type":"integer","minimum":1,"maximum":200}}}),
        ),
    ]
}

pub struct SlackTool {
    client: SlackClient,
    config: SlackConfig,
    kind: ToolKind,
}

#[derive(Clone, Copy)]
pub enum ToolKind {
    Send,
    Users,
    Channels,
}

impl SlackTool {
    pub fn new(client: SlackClient, config: SlackConfig, kind: ToolKind) -> Arc<Self> {
        Arc::new(Self {
            client,
            config,
            kind,
        })
    }
}

#[async_trait]
impl ToolExecutor for SlackTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        match self.kind {
            ToolKind::Send => self.send(args).await,
            ToolKind::Users => self.users(args).await,
            ToolKind::Channels => self.channels(args).await,
        }
    }
}

#[async_trait]
impl DeliverySink for SlackTool {
    async fn deliver(&self, dest: &Destination, text: &str) -> Result<()> {
        let channel = dest.channel.as_deref().ok_or_else(|| anyhow::anyhow!("slack delivery requires channel"))?;
        let outcome = self.execute(json!({"channel": channel, "text": text})).await?;
        if outcome.is_error { anyhow::bail!("{}", outcome.text); }
        Ok(())
    }
}

impl SlackTool {
    async fn send(&self, args: Value) -> Result<ToolOutcome> {
        let Some(channel) = args
            .get("channel")
            .and_then(Value::as_str)
            .filter(|value| valid_id(value))
        else {
            return Ok(invalid("slack_send_message requires Slack channel id"));
        };
        let Some(text) = args
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= MAX_TEXT)
        else {
            return Ok(invalid(
                "slack_send_message requires non-empty text up to 40000 bytes",
            ));
        };
        let thread_ts = args
            .get("threadTs")
            .or_else(|| args.get("thread_ts"))
            .and_then(Value::as_str);
        if thread_ts.is_some_and(|value| !valid_timestamp(value)) {
            return Ok(invalid(
                "slack_send_message thread_ts must be Slack timestamp",
            ));
        }
        let destination = if channel.starts_with('U') {
            if !self.config.dm.enabled
                || (!self.config.dm.users.is_empty()
                    && !self.config.dm.users.iter().any(|user| user == channel))
            {
                return Ok(ToolOutcome::error_code(
                    "not_allowed",
                    "Slack user is not allowed by extension config",
                    None::<String>,
                ));
            }
            match self.client.open_direct_message(channel).await {
                Ok(channel) => channel,
                Err(error) => {
                    return Ok(ToolOutcome::error_code(
                        "slack_open_dm_failed",
                        format!("Slack DM open failed: {}", error.code),
                        None::<String>,
                    ))
                }
            }
        } else {
            if !outbound_allowed(&self.config, channel) {
                return Ok(ToolOutcome::error_code(
                    "not_allowed",
                    "Slack destination not allowed by extension config",
                    None::<String>,
                ));
            }
            channel.to_owned()
        };
        let chunks = crate::mrkdwn::chunk(&crate::mrkdwn::render(text), 3900);
        let mut sent = None;
        for chunk in chunks {
            match self
                .client
                .post_message(&destination, &chunk, thread_ts)
                .await
            {
                Ok(message) => sent.get_or_insert(message),
                Err(error) => {
                    return Ok(ToolOutcome::error_code(
                        "slack_send_failed",
                        format!("Slack send failed: {}", error.code),
                        None::<String>,
                    ))
                }
            };
        }
        match sent {
            Some(sent) => Ok(ToolOutcome::ok(format!(
                "sent Slack message to {} at {}",
                sent.channel, sent.ts
            ))),
            None => Ok(invalid("slack_send_message requires non-empty text")),
        }
    }

    async fn users(&self, args: Value) -> Result<ToolOutcome> {
        let limit = limit(&args);
        match self.client.list_users(limit, query(&args)).await {
            Ok(users) => Ok(ToolOutcome::ok(serde_json::to_string(&users)?)),
            Err(error) => Ok(ToolOutcome::error_code(
                "slack_list_users_failed",
                format!("Slack list users failed: {}", error.code),
                None::<String>,
            )),
        }
    }

    async fn channels(&self, args: Value) -> Result<ToolOutcome> {
        match self.client.list_channels(limit(&args), query(&args)).await {
            Ok(channels) => Ok(ToolOutcome::ok(serde_json::to_string(&channels)?)),
            Err(error) => Ok(ToolOutcome::error_code(
                "slack_list_channels_failed",
                format!("Slack list channels failed: {}", error.code),
                None::<String>,
            )),
        }
    }
}

fn invalid(message: &str) -> ToolOutcome {
    ToolOutcome::error_code("invalid_args", message, None::<String>)
}
fn limit(args: &Value) -> usize {
    args.get("limit")
        .and_then(Value::as_u64)
        .map(|value| value.clamp(1, MAX_RESULTS as u64) as usize)
        .unwrap_or(100)
}
fn query(args: &Value) -> Option<&str> {
    args.get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 256)
}
fn valid_id(value: &str) -> bool {
    value.len() <= 32
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}
fn valid_timestamp(value: &str) -> bool {
    value.len() <= 32
        && value.split_once('.').is_some_and(|(seconds, fraction)| {
            !seconds.is_empty()
                && !fraction.is_empty()
                && seconds.bytes().all(|b| b.is_ascii_digit())
                && fraction.bytes().all(|b| b.is_ascii_digit())
        })
}
fn outbound_allowed(config: &SlackConfig, channel: &str) -> bool {
    if channel.starts_with('D') {
        // A conversation ID cannot identify its recipient. Restrict direct
        // conversation sends when a user allowlist is configured; use U... so
        // conversations.open can enforce it instead.
        return config.dm.enabled && config.dm.users.is_empty();
    }
    config.channels.is_empty() || config.channels.contains_key(channel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_tool_names_are_codex_compatible() {
        for spec in specs() {
            assert!(spec.name.chars().all(
                |character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            ));
        }
    }
}
