pub mod config;

pub mod api;
pub mod attachments;
pub mod live_answer;
#[cfg(test)]
mod test_http;

pub mod addressing;
pub mod artifacts;
pub mod commands;
pub mod history;
pub mod markdown;
pub mod runtime;
pub mod session;
pub mod thinking;
pub mod tools;

use anyhow::Result;
use dar_extension_sdk::{BoxFuture, Extension, RegisterCtx, StartCtx};

pub fn extension() -> Box<dyn Extension> {
    Box::new(MattermostExtension)
}
struct MattermostExtension;
impl Extension for MattermostExtension {
    fn id(&self) -> &'static str {
        "mattermost"
    }
    fn agent_singleton(&self) -> bool {
        true
    }
    fn register<'a>(&'a self, ctx: &'a mut RegisterCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse(&ctx.config, self.id())?;
            let credentials = cfg.credentials()?;
            let _ = rustls::crypto::ring::default_provider().install_default();
            let client = api::MattermostClient::new(&credentials.url, credentials.token)?;
            ctx.services
                .service::<dyn dar_extension_sdk::deliver::DeliverySink>(
                    "mattermost",
                    tools::MattermostTool::new(client.clone(), cfg.clone(), tools::ToolKind::Send),
                )?;
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn dar_extension_sdk::tools::ToolRegistryHandle>(
                    dar_extension_sdk::tools::TOOL_REGISTRY_SERVICE,
                )
            {
                for (spec, kind) in tools::specs().into_iter().zip([
                    tools::ToolKind::Send,
                    tools::ToolKind::Users,
                    tools::ToolKind::Channels,
                ]) {
                    registry.register_tool(
                        spec,
                        tools::MattermostTool::new(client.clone(), cfg.clone(), kind),
                    )?;
                }
            }
            Ok(())
        })
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse(&ctx.config, self.id())?;
            let credentials = cfg.credentials()?;
            let client = api::MattermostClient::new(&credentials.url, credentials.token)?;
            let data = ctx.paths.data_dir(self.id())?;
            std::fs::create_dir_all(&data)?;
            tokio::spawn(async move {
                if runtime::run(ctx, cfg, client, data).await.is_err() {
                    tracing::error!("mattermost runtime stopped");
                }
            });
            Ok(())
        })
    }
}

pub mod ws;
