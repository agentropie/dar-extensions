use crate::{
    artifacts::ArtifactStore,
    audio,
    config::{normalize_keywords, Config},
    engine::{SttEngine, WhistleCli},
    lock::FileLock,
    Failure,
};
use async_trait::async_trait;
use dar_extension_sdk::{
    tools::{ToolExecutor, ToolOutcome, ToolSpec},
    HostPaths,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::time::Instant;

pub fn spec() -> ToolSpec {
    ToolSpec::new("transcribe_audio", "Transcribe a local Ogg/Opus or WAV recording on this machine. Names and amounts may be inaccurate; verify important details.", json!({
        "type":"object", "additionalProperties":false,
        "properties":{"path":{"type":"string","minLength":1}, "keywords":{"type":"array","items":{"type":"string"}}},
        "required":["path"]
    })).reads()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    path: String,
    #[serde(default)]
    keywords: Vec<String>,
}

pub struct TranscribeTool {
    pub paths: HostPaths,
    pub config: Config,
    pub artifacts: Arc<ArtifactStore>,
    pub engine: Option<Arc<dyn SttEngine>>,
}

impl TranscribeTool {
    async fn run(&self, args: Args, deadline: Instant) -> Result<Value, Failure> {
        let keywords =
            normalize_keywords(self.config.keywords.iter().cloned().chain(args.keywords))?;
        let directory = prepare_model_dir(&self.paths, &self.config)?;
        let lock = FileLock::acquire(&directory.join("transcribe.lock"), deadline).await?;
        let root = self.paths.root().to_path_buf();
        let max_bytes = self.config.max_file_mb * 1024 * 1024;
        let max_secs = self.config.max_audio_secs as f64;
        let mut decode = tokio::task::spawn_blocking(move || {
            // Keep serialization until the blocking decoder has stopped, even on cancellation.
            (
                audio::load_until(&root, &args.path, max_bytes, max_secs, deadline.into_std()),
                lock,
            )
        });
        let (input, _lock) = match tokio::time::timeout_at(deadline, &mut decode).await {
            Ok(result) => result
                .map_err(|_| Failure::new("unsupported_format", "Unable to decode recording"))?,
            Err(_) => {
                decode.abort();
                let _ = decode.await; // A running decoder checks the deadline between packets/reads.
                return Err(Failure::new("timeout", "Transcription deadline exceeded"));
            }
        };
        let input = input?;
        let artifacts = tokio::time::timeout_at(deadline, self.artifacts.ensure())
            .await
            .map_err(|_| Failure::new("timeout", "Transcription deadline exceeded"))??;
        let duration_secs = input.duration_secs;
        let transcript = match &self.engine {
            Some(engine) => {
                engine
                    .transcribe(input, self.config.language.as_deref(), &keywords, deadline)
                    .await?
            }
            None => {
                WhistleCli::new(artifacts, directory)
                    .transcribe(input, self.config.language.as_deref(), &keywords, deadline)
                    .await?
            }
        };
        tracing::info!(duration_secs, outcome = "ok", "transcribe_audio completed");
        Ok(
            json!({"text":transcript.text,"duration_secs":duration_secs,"windowed":duration_secs > 30.0,"language":transcript.language}),
        )
    }
}

#[async_trait]
impl ToolExecutor for TranscribeTool {
    async fn execute(&self, args: Value) -> anyhow::Result<ToolOutcome> {
        let result = match serde_json::from_value::<Args>(args) {
            Ok(args) if !args.path.trim().is_empty() => {
                match Instant::now().checked_add(Duration::from_secs(self.config.timeout_secs)) {
                    Some(deadline) => self.run(args, deadline).await,
                    None => Err(Failure::new("invalid_args", "timeout_secs is too large")),
                }
            }
            _ => Err(Failure::new(
                "invalid_args",
                "Provide a path and optional keywords array",
            )),
        };
        Ok(match result {
            Ok(value) => ToolOutcome::ok(value.to_string()),
            Err(err) => {
                tracing::warn!(outcome = err.code, "transcribe_audio failed");
                ToolOutcome::error_code(err.code, err.message, None::<String>)
            }
        })
    }
}

/// Resolve missing descendants using the SDK containment checks, without creating them at register time.
pub fn model_dir(paths: &HostPaths, config: &Config) -> Result<PathBuf, Failure> {
    let mut path = paths.root().to_path_buf();
    for component in std::path::Path::new(&config.model_dir).components() {
        path.push(component);
        if path.exists() || path.symlink_metadata().is_ok() {
            path = paths
                .assert_contained(&path)
                .map_err(|_| Failure::new("invalid_args", "model_dir leaves the agent root"))?;
        }
    }
    Ok(path)
}

pub fn prepare_model_dir(paths: &HostPaths, config: &Config) -> Result<PathBuf, Failure> {
    let mut directory = paths.root().to_path_buf();
    for component in std::path::Path::new(&config.model_dir).components() {
        directory.push(component);
        let checked = paths
            .assert_contained(&directory)
            .map_err(|_| Failure::new("invalid_args", "model_dir leaves the agent root"))?;
        if !checked.exists() {
            match std::fs::create_dir(&checked) {
                Ok(()) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::AlreadyExists && checked.is_dir() => {}
                Err(_) => {
                    return Err(Failure::new(
                        "model_unavailable",
                        "Cannot create model directory",
                    ))
                }
            }
        }
        directory = paths
            .assert_contained(&checked)
            .map_err(|_| Failure::new("invalid_args", "model_dir leaves the agent root"))?;
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        artifacts::{ArtifactSpec, Fetcher},
        engine::Transcript,
    };
    use std::path::Path;

    struct MemoryFetcher;
    #[async_trait]
    impl Fetcher for MemoryFetcher {
        async fn fetch(&self, _: &str, path: &Path, _: u64) -> Result<(), Failure> {
            tokio::fs::write(path, b"abc")
                .await
                .map_err(|_| Failure::new("model_unavailable", "write failed"))
        }
    }
    struct Engine;
    #[async_trait]
    impl SttEngine for Engine {
        async fn transcribe(
            &self,
            _: audio::AudioInput,
            language: Option<&str>,
            keywords: &[String],
            _: Instant,
        ) -> Result<Transcript, Failure> {
            if language != Some("fr") || keywords != ["Atlas", "Boreal"] {
                return Err(Failure::new("engine_failed", "Wrong language or keywords"));
            }
            Ok(Transcript {
                text: "Bonjour Atlas.".into(),
                language: Some("fr".into()),
                note: None,
            })
        }
    }
    fn fixture() -> (tempfile::TempDir, TranscribeTool) {
        let dir = tempfile::tempdir().unwrap();
        let paths = HostPaths::new(dir.path()).unwrap();
        let config = Config {
            language: Some("fr".into()),
            keywords: vec!["Atlas".into()],
            ..Config::default()
        };
        let artifacts = ArtifactStore::with_manifest(
            dir.path().join("data/stt"),
            Arc::new(MemoryFetcher),
            vec![
                ArtifactSpec {
                    name: "whistle.cact",
                    url: "model",
                    size: 3,
                    sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    executable: false,
                },
                ArtifactSpec {
                    name: "needle",
                    url: "helper",
                    size: 3,
                    sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    executable: true,
                },
            ],
        );
        let tool = TranscribeTool {
            paths,
            config,
            artifacts: Arc::new(artifacts),
            engine: Some(Arc::new(Engine)),
        };
        (dir, tool)
    }
    fn wav(path: &Path, seconds: u32) {
        let mut writer = hound::WavWriter::create(
            path,
            hound::WavSpec {
                channels: 1,
                sample_rate: 16000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        )
        .unwrap();
        for _ in 0..seconds * 16000 {
            writer.write_sample(0i16).unwrap();
        }
        writer.finalize().unwrap();
    }
    #[tokio::test]
    async fn returns_transcript_duration_language_and_merged_keywords() {
        let (dir, tool) = fixture();
        wav(&dir.path().join("note.bin"), 1);
        let out = tool
            .execute(json!({"path":"note.bin","keywords":[" Atlas ","Boreal"]}))
            .await
            .unwrap();
        assert!(!out.is_error, "{}", out.text);
        assert_eq!(
            serde_json::from_str::<Value>(&out.text).unwrap(),
            json!({"text":"Bonjour Atlas.","duration_secs":1.0,"windowed":false,"language":"fr"})
        );
    }
    #[tokio::test]
    async fn returns_stable_errors_for_bad_arguments_paths_formats_and_bounds() {
        let (dir, mut tool) = fixture();
        let cases = [
            json!({}),
            json!({"path":""}),
            json!({"path":"note","extra":true}),
            json!({"path":"note","keywords":[""]}),
            json!({"path":"note","keywords":["line\nbreak"]}),
        ];
        for args in cases {
            let out = tool.execute(args).await.unwrap();
            assert_eq!(out.error.unwrap().code, "invalid_args");
        }
        for (path, expected) in [("missing.wav", "not_found"), ("../escape", "invalid_args")] {
            let out = tool.execute(json!({"path":path})).await.unwrap();
            assert_eq!(out.error.unwrap().code, expected);
        }
        std::fs::write(dir.path().join("not-audio"), b"not audio").unwrap();
        assert_eq!(
            tool.execute(json!({"path":"not-audio"}))
                .await
                .unwrap()
                .error
                .unwrap()
                .code,
            "unsupported_format"
        );
        wav(&dir.path().join("note.wav"), 2);
        tool.config.max_audio_secs = 1;
        assert_eq!(
            tool.execute(json!({"path":"note.wav"}))
                .await
                .unwrap()
                .error
                .unwrap()
                .code,
            "too_long"
        );
        tool.config.max_audio_secs = 900;
        tool.config.max_file_mb = 1;
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("note.wav"))
            .unwrap()
            .set_len(1_048_577)
            .unwrap();
        assert_eq!(
            tool.execute(json!({"path":"note.wav"}))
                .await
                .unwrap()
                .error
                .unwrap()
                .code,
            "too_large"
        );
    }

    #[tokio::test]
    async fn failed_first_download_is_retried_without_restarting_the_executor() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct OfflineOnce(AtomicBool);
        #[async_trait]
        impl Fetcher for OfflineOnce {
            async fn fetch(&self, url: &str, path: &Path, size: u64) -> Result<(), Failure> {
                if self.0.swap(false, Ordering::SeqCst) {
                    return Err(Failure::new("model_unavailable", "Offline"));
                }
                MemoryFetcher.fetch(url, path, size).await
            }
        }
        let (dir, mut tool) = fixture();
        wav(&dir.path().join("note.wav"), 1);
        let manifest = vec![ArtifactSpec {
            name: "whistle.cact",
            url: "memory",
            size: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            executable: false,
        }];
        tool.artifacts = Arc::new(ArtifactStore::with_manifest(
            dir.path().join("data/stt"),
            Arc::new(OfflineOnce(AtomicBool::new(true))),
            manifest,
        ));
        let args = json!({"path":"note.wav","keywords":["Boreal"]});
        assert_eq!(
            tool.execute(args.clone())
                .await
                .unwrap()
                .error
                .unwrap()
                .code,
            "model_unavailable"
        );
        assert!(!tool.execute(args).await.unwrap().is_error);
    }

    #[tokio::test]
    async fn long_audio_is_windowed_and_simultaneous_calls_are_serialized() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct SerialEngine(AtomicBool);
        #[async_trait]
        impl SttEngine for SerialEngine {
            async fn transcribe(
                &self,
                _: audio::AudioInput,
                _: Option<&str>,
                _: &[String],
                _: Instant,
            ) -> Result<Transcript, Failure> {
                if self.0.swap(true, Ordering::SeqCst) {
                    return Err(Failure::new("engine_failed", "Overlap"));
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
                self.0.store(false, Ordering::SeqCst);
                Ok(Transcript {
                    text: "Bonjour.".into(),
                    language: Some("fr".into()),
                    note: None,
                })
            }
        }
        let (dir, mut tool) = fixture();
        tool.engine = Some(Arc::new(SerialEngine(AtomicBool::new(false))));
        wav(&dir.path().join("long.wav"), 31);
        let (first, second) = tokio::join!(
            tool.execute(json!({"path":"long.wav"})),
            tool.execute(json!({"path":"long.wav"}))
        );
        for out in [first.unwrap(), second.unwrap()] {
            assert!(!out.is_error, "{}", out.text);
            assert_eq!(
                serde_json::from_str::<Value>(&out.text).unwrap(),
                json!({"text":"Bonjour.","duration_secs":31.0,"windowed":true,"language":"fr"})
            );
        }
    }
    #[tokio::test]
    async fn waiting_for_a_busy_engine_expires_without_blocking_other_work() {
        let (dir, mut tool) = fixture();
        tool.config.timeout_secs = 1;
        let directory = prepare_model_dir(&tool.paths, &tool.config).unwrap();
        let _holder = FileLock::acquire(
            &directory.join("transcribe.lock"),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        wav(&dir.path().join("note.wav"), 1);
        let call = tool.execute(json!({"path":"note.wav"}));
        let heartbeat = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            true
        };
        let (out, progress) = tokio::join!(call, heartbeat);
        assert!(progress);
        assert!(["busy", "timeout"].contains(&out.unwrap().error.unwrap().code.as_str()));
    }
}
