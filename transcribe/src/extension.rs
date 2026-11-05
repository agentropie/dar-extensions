use crate::{
    artifacts::{ArtifactStore, HttpFetcher},
    config,
    tool::{self, TranscribeTool},
};
use dar_extension_sdk::{Extension, RegisterCtx, StartCtx};
use std::sync::{Arc, OnceLock};

#[derive(Default)]
pub struct TranscribeExtension {
    tool: OnceLock<Arc<TranscribeTool>>,
}

pub fn extension() -> Box<dyn Extension> {
    Box::new(TranscribeExtension::default())
}

impl Extension for TranscribeExtension {
    fn id(&self) -> &'static str {
        "transcribe"
    }
    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let Some(config) = config::parse(ctx.config.get(self.id()))?.filter(|c| c.enabled)
            else {
                return Ok(());
            };
            let directory = tool::model_dir(&ctx.paths, &config)?;
            let tool = Arc::new(TranscribeTool {
                paths: ctx.paths.clone(),
                config,
                artifacts: Arc::new(ArtifactStore::new(directory, Arc::new(HttpFetcher::new()?))),
                engine: None,
            });
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn dar_extension_sdk::tools::ToolRegistryHandle>(
                    dar_extension_sdk::tools::TOOL_REGISTRY_SERVICE,
                )
            {
                registry.register_tool(tool::spec(), tool.clone())?;
            }
            self.tool
                .set(tool)
                .map_err(|_| anyhow::anyhow!("transcribe already registered"))?;
            Ok(())
        })
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let Some(tool) = self.tool.get().cloned() else {
                return Ok(());
            };
            tokio::spawn(async move {
                let mut shutdown = ctx.shutdown;
                let prepare = async {
                    let directory = tool::prepare_model_dir(&tool.paths, &tool.config)?;
                    let deadline =
                        tokio::time::Instant::now() + std::time::Duration::from_secs(600);
                    let _lock = crate::lock::FileLock::acquire(
                        &directory.join("transcribe.lock"),
                        deadline,
                    )
                    .await?;
                    let temporary = directory.join("tmp");
                    match tokio::fs::symlink_metadata(&temporary).await {
                        Ok(metadata) if metadata.file_type().is_symlink() => {
                            tokio::fs::remove_file(&temporary).await?;
                        }
                        Ok(_) => {
                            tokio::fs::remove_dir_all(&temporary).await?;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                    tool.artifacts.ensure().await?;
                    anyhow::Ok(())
                };
                tokio::select! {
                    _ = shutdown.cancelled() => {},
                    result = prepare => if let Err(error) = result {
                        tracing::debug!(error = %error, "transcribe artifact preparation failed");
                        dar_extension_sdk::log::event("-", "transcribe", "artifacts unavailable; next transcribe_audio call will retry");
                    }
                }
            });
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dar_extension_sdk::{
        tools::{ToolRegistryHandle, TOOL_REGISTRY_SERVICE},
        ConfigStore, HostPaths, ShutdownToken,
    };
    use host_api::{EventBus, ForegroundRegistry, HttpRegistry, ServiceRegistry};
    use serde_json::json;
    use tool_registry::ToolRegistry;
    #[tokio::test]
    async fn registration_is_opt_in_and_does_not_download() {
        for (value, count) in [
            (None, 0),
            (Some(json!({"enabled":false})), 0),
            (Some(json!({"language":"fr"})), 1),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (_, rx) = tokio::sync::watch::channel(false);
            let registry = Arc::new(ToolRegistry::new());
            let mut services = ServiceRegistry::default();
            services
                .service::<dyn ToolRegistryHandle>(TOOL_REGISTRY_SERVICE, registry.clone())
                .unwrap();
            let mut ctx = RegisterCtx {
                bus: EventBus::new(),
                http: HttpRegistry::disabled(),
                foreground: ForegroundRegistry::default(),
                services,
                paths: HostPaths::new(dir.path()).unwrap(),
                config: ConfigStore::from_values(
                    value
                        .map(|v| [("transcribe".into(), v)].into_iter().collect())
                        .unwrap_or_default(),
                ),
                shutdown: ShutdownToken::new(rx),
            };
            let extension = extension();
            extension.register(&mut ctx).await.unwrap();
            assert_eq!(registry.list().len(), count);
            assert!(
                !dir.path().join("data").exists(),
                "register must not write/download artifacts"
            );
            assert!(!extension.agent_singleton());
        }
    }
    #[tokio::test]
    async fn start_returns_while_artifact_download_is_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let register = RegisterCtx {
            bus: EventBus::new(),
            http: HttpRegistry::disabled(),
            foreground: ForegroundRegistry::default(),
            services: ServiceRegistry::default(),
            paths: HostPaths::new(dir.path()).unwrap(),
            config: ConfigStore::default(),
            shutdown: ShutdownToken::new(rx),
        };
        let ctx = StartCtx {
            paths: register.paths.clone(),
            config: register.config.clone(),
            shutdown: register.shutdown.clone(),
            host: register.into_start_services().unwrap(),
        };
        let extension = TranscribeExtension::default();
        struct PendingFetcher(tokio::sync::Notify);
        #[async_trait::async_trait]
        impl crate::artifacts::Fetcher for PendingFetcher {
            async fn fetch(
                &self,
                _: &str,
                _: &std::path::Path,
                _: u64,
            ) -> Result<(), crate::Failure> {
                self.0.notify_one();
                std::future::pending().await
            }
        }
        let fetcher = Arc::new(PendingFetcher(tokio::sync::Notify::new()));
        let manifest = vec![crate::artifacts::ArtifactSpec {
            name: "whistle.cact",
            url: "memory",
            size: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            executable: false,
        }];
        let tool = Arc::new(TranscribeTool {
            paths: ctx.paths.clone(),
            config: config::Config::default(),
            artifacts: Arc::new(ArtifactStore::with_manifest(
                dir.path().join("data/stt"),
                fetcher.clone(),
                manifest,
            )),
            engine: None,
        });
        extension.tool.set(tool).ok().unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(100), extension.start(ctx))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), fetcher.0.notified())
            .await
            .unwrap();
        tokio::task::yield_now().await; // The host keeps progressing while the download hangs.
        _tx.send(true).unwrap();
    }
}
