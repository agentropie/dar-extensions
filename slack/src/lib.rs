//! Slack Socket Mode extension for one DAR agent.

use anyhow::Result;
use dar_extension_sdk::{BoxFuture, Extension, RegisterCtx, StartCtx};

pub mod addressing;
pub mod api;
pub mod attachments;
pub mod commands;
pub mod config;
pub mod history;
pub mod live_answer;
pub mod mrkdwn;
pub mod runtime;
pub mod session;
pub mod thinking;
pub mod tools;

pub fn extension() -> Box<dyn Extension> {
    Box::new(SlackExtension)
}

struct SlackExtension;

impl Extension for SlackExtension {
    fn id(&self) -> &'static str {
        "slack"
    }

    fn register<'a>(&'a self, ctx: &'a mut RegisterCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse_config(&ctx.config, self.id())?;
            let tokens = cfg.tokens()?;
            let client = api::SlackClient::new(tokens.bot)?;
            ctx.services.service::<dyn dar_extension_sdk::deliver::DeliverySink>("slack", tools::SlackTool::new(client.clone(), cfg.clone(), tools::ToolKind::Send))?;
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn dar_extension_sdk::tools::ToolRegistryHandle>(
                    dar_extension_sdk::tools::TOOL_REGISTRY_SERVICE,
                )
            {
                let [send, users, channels] = tools::specs();
                registry.register_tool(
                    send,
                    tools::SlackTool::new(client.clone(), cfg.clone(), tools::ToolKind::Send),
                )?;
                registry.register_tool(
                    users,
                    tools::SlackTool::new(client.clone(), cfg.clone(), tools::ToolKind::Users),
                )?;
                registry.register_tool(
                    channels,
                    tools::SlackTool::new(client, cfg, tools::ToolKind::Channels),
                )?;
            }
            Ok(())
        })
    }

    fn agent_singleton(&self) -> bool {
        true
    }

    fn start<'a>(&'a self, ctx: StartCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse_config(&ctx.config, self.id())?;
            cfg.tokens()?;
            tokio::spawn(async move {
                if let Err(error) = runtime::run(ctx, cfg).await {
                    dar_extension_sdk::log::event(
                        "-",
                        "slack",
                        &format!("channel stopped: {error}"),
                    );
                }
            });
            Ok(())
        })
    }
}
