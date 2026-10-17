//! Threadit board extension for one DAR agent.
//!
//! Polls the board for attention items, runs one chat turn per item, shows safe
//! progress as agent activities, and posts the answer as a reply.

mod api;
mod config;
mod labels;
mod prompt;
mod runtime;

use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dar_extension_sdk::chat::{ChatBackend, ChatEvent};
use dar_extension_sdk::{BoxFuture, Extension, RegisterCtx, StartCtx};
use tokio::sync::mpsc;

use runtime::{ChatConn, SessionOpener};

pub fn extension() -> Box<dyn Extension> {
    Box::new(ThreaditExtension)
}

struct ThreaditExtension;

struct DarOpener {
    ctx: StartCtx,
    backend: Option<String>,
}

#[async_trait]
impl SessionOpener for DarOpener {
    async fn open(&self, dir: &Path) -> Result<ChatConn> {
        std::fs::create_dir_all(dir)?;
        let backend_id =
            dar_extension_sdk::chat::resolve_agent_backend(&self.ctx, self.backend.as_deref());
        let backend = self
            .ctx
            .host
            .services
            .get::<dyn ChatBackend>(&backend_id)
            .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
        let params = dar_extension_sdk::chat::agent_session_params(&self.ctx, dir).build();
        let (tx, rx) = mpsc::channel::<ChatEvent>(256);
        let session = backend.open(params, tx).await?;
        Ok(ChatConn { session, rx })
    }
}

impl Extension for ThreaditExtension {
    fn id(&self) -> &'static str {
        "threadit"
    }

    fn agent_singleton(&self) -> bool {
        true
    }

    fn register<'a>(&'a self, ctx: &'a mut RegisterCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse_config(&ctx.config, self.id())?;
            cfg.base_url()?;
            config::token()?;
            Ok(())
        })
    }

    fn start<'a>(&'a self, ctx: StartCtx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse_config(&ctx.config, self.id())?;
            let board = api::Board::new(&cfg.base_url()?, &config::token()?)?;
            // Host may enable both ring and aws-lc-rs; pick one explicitly.
            let _ = rustls::crypto::ring::default_provider().install_default();

            std::fs::create_dir_all(ctx.paths.root().join("data"))?;
            let session_root = ctx.paths.data_dir(self.id())?.join("sessions");
            std::fs::create_dir_all(&session_root)?;

            let poll = cfg.poll_interval();
            let turn_timeout = cfg.turn_timeout();
            let shutdown = ctx.shutdown.clone();
            let opener = DarOpener {
                ctx,
                backend: cfg.backend().map(str::to_string),
            };
            tokio::spawn(runtime::run(
                board,
                Box::new(opener),
                session_root,
                poll,
                turn_timeout,
                shutdown,
            ));
            Ok(())
        })
    }
}
