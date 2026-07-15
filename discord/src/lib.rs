//! Discord DM chat channel for dar.
use anyhow::Result;
use dar_extension_sdk::{
    tools::{ToolRegistryHandle, TOOL_REGISTRY_SERVICE},
    Extension, RegisterCtx, StartCtx,
};
use serde::Deserialize;
mod addressing;
mod attachments;
mod commands;
mod config;
mod delivery;
mod live_answer;
mod markdown;
mod runtime;
mod session;
mod tools;
pub fn extension() -> Box<dyn Extension> {
    Box::new(DiscordExtension)
}
struct DiscordExtension;

impl Extension for DiscordExtension {
    fn id(&self) -> &'static str {
        "discord"
    }
    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse(&ctx.config, self.id())?;
            let token = config::token(&cfg)?;
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn ToolRegistryHandle>(TOOL_REGISTRY_SERVICE)
            {
                registry.register_tool(tools::spec(), tools::DiscordSendTool::new(token, cfg))?;
            }
            Ok(())
        })
    }
    fn agent_singleton(&self) -> bool {
        true
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse(&ctx.config, self.id())?;
            let token = config::token(&cfg)?;
            let data = ctx.paths.data_dir(self.id())?;
            std::fs::create_dir_all(data.join("sessions"))?;
            tokio::spawn(async move {
                if let Err(error) = runtime::run(ctx, cfg, token, data).await {
                    tracing::error!(%error,"discord gateway stopped");
                }
            });
            Ok(())
        })
    }
}
#[derive(Deserialize)]
struct Gateway {
    url: String,
}
