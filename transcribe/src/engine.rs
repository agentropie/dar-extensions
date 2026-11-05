use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::audio::AudioData;
use async_trait::async_trait;
use serde::Deserialize;
use tokio::{io::AsyncReadExt, time::Instant};

use crate::{artifacts::Artifacts, audio::AudioInput, Failure};

#[derive(Debug)]
pub struct Transcript {
    pub text: String,
    pub language: Option<String>,
    pub note: Option<String>,
}

#[async_trait]
pub trait SttEngine: Send + Sync {
    async fn transcribe(
        &self,
        input: AudioInput,
        language: Option<&str>,
        keywords: &[String],
        deadline: Instant,
    ) -> Result<Transcript, Failure>;
}

pub struct WhistleCli {
    artifacts: Artifacts,
    model_dir: PathBuf,
}

impl WhistleCli {
    pub fn new(artifacts: Artifacts, model_dir: PathBuf) -> Self {
        Self {
            artifacts,
            model_dir,
        }
    }
}

#[async_trait]
impl SttEngine for WhistleCli {
    async fn transcribe(
        &self,
        input: AudioInput,
        language: Option<&str>,
        keywords: &[String],
        deadline: Instant,
    ) -> Result<Transcript, Failure> {
        if Instant::now() >= deadline {
            return Err(Failure::new("timeout", "Transcription timed out"));
        }
        let dir = self.model_dir.join("tmp");
        let keywords = keywords.to_vec();
        let cancellation = PreparationCancellation(Arc::new(AtomicBool::new(false)));
        let cancelled = cancellation.0.clone();
        let mut preparation = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            if std::fs::symlink_metadata(&dir)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                anyhow::bail!("engine temporary directory must not be a symlink");
            }
            check_preparation(&cancelled, deadline)?;
            std::fs::create_dir_all(&dir)?;
            let temp = tempfile::tempdir_in(dir)?;
            let audio = match input.audio {
                AudioData::Wav(bytes) => {
                    let path = temp.path().join("audio.wav");
                    write_prepared(&path, &bytes, &cancelled, deadline)?;
                    path
                }
                AudioData::Pcm16kMono(samples) => {
                    let path = temp.path().join("audio.wav");
                    let mut wav = hound::WavWriter::create(
                        &path,
                        hound::WavSpec {
                            channels: 1,
                            sample_rate: 16000,
                            bits_per_sample: 16,
                            sample_format: hound::SampleFormat::Int,
                        },
                    )?;
                    for chunk in samples.chunks(4096) {
                        check_preparation(&cancelled, deadline)?;
                        for sample in chunk {
                            wav.write_sample(*sample)?;
                        }
                    }
                    check_preparation(&cancelled, deadline)?;
                    wav.finalize()?;
                    path
                }
            };
            let keyword_path = if keywords.is_empty() {
                None
            } else {
                let path = temp.path().join("keywords.txt");
                check_preparation(&cancelled, deadline)?;
                write_prepared(&path, keywords.join("\n").as_bytes(), &cancelled, deadline)?;
                Some(path)
            };
            check_preparation(&cancelled, deadline)?;
            Ok((temp, audio, keyword_path))
        });
        let prepared = match tokio::time::timeout_at(deadline, &mut preparation).await {
            Ok(result) => result,
            Err(_) => {
                cancellation.0.store(true, Ordering::Relaxed);
                preparation.abort();
                // A running blocking task cannot be aborted. Keep the caller's
                // lock until its bounded writes stop and its temporary files drop.
                let _ = preparation.await;
                return Err(Failure::new("timeout", "Transcription timed out"));
            }
        }
        .map_err(|_| Failure::new("engine_failed", "Could not prepare engine input"))?
        .map_err(|err| {
            if Instant::now() >= deadline {
                return Failure::new("timeout", "Transcription timed out");
            }
            tracing::warn!(error = %err, "transcribe input preparation failed");
            Failure::new("engine_failed", "Could not prepare engine input")
        })?;
        drop(cancellation);
        let (_temp, audio, keyword_path) = prepared;
        let mut command = tokio::process::Command::new(&self.artifacts.helper);
        command
            .arg("--model")
            .arg(&self.artifacts.model)
            .arg("--audio")
            .arg(audio)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        if let Some(language) = language {
            command.arg("--audio-language").arg(language);
        }
        if let Some(path) = keyword_path {
            command.arg("--audio-keywords").arg(path);
        }
        if Instant::now() >= deadline {
            return Err(Failure::new("timeout", "Transcription timed out"));
        }
        let mut child = command.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()).spawn().map_err(|err| {
                tracing::warn!(helper = %self.artifacts.helper.display(), error = %err, "transcribe helper spawn failed");
                Failure::new("engine_failed", "Transcription engine failed")
            })?;
        let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take())
        else {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(Failure::new(
                "engine_failed",
                "Could not read engine output",
            ));
        };
        let output = tokio::time::timeout_at(deadline, async {
            tokio::try_join!(
                child.wait(),
                read_capped(&mut stdout, 4 * 1024 * 1024),
                read_capped(&mut stderr, 32 * 1024)
            )
        })
        .await;
        let (status, out, err, oversized) = match output {
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(Failure::new("timeout", "Transcription timed out"));
            }
            Ok(Ok((status, (out, oversized), (err, _)))) => (status, out, err, oversized),
            Ok(Err(_)) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(Failure::new(
                    "engine_failed",
                    "Could not read engine output",
                ));
            }
        };
        if !status.success() || oversized {
            tracing::warn!(helper = %self.artifacts.helper.display(), stderr = %String::from_utf8_lossy(&err), "transcribe helper failed");
            return Err(Failure::new("engine_failed", "Transcription engine failed"));
        }
        #[derive(Deserialize)]
        struct Response {
            text: String,
            language: Option<String>,
        }
        let response: Response = std::str::from_utf8(&out).ok().and_then(|json| {
            let mut lines = json.trim().lines();
            let line = lines.next()?;
            if lines.next().is_some() { return None; }
            serde_json::from_str(line).ok()
        }).ok_or_else(|| {
            tracing::warn!(helper = %self.artifacts.helper.display(), stderr = %String::from_utf8_lossy(&err), "transcribe helper returned invalid output");
            Failure::new("engine_failed", "Transcription engine returned invalid output")
        })?;
        Ok(Transcript {
            text: response.text,
            language: response.language,
            note: None,
        })
    }
}

// Cancellation also stops preparation if a client drops the tool future.
struct PreparationCancellation(Arc<AtomicBool>);

impl Drop for PreparationCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn check_preparation(cancelled: &AtomicBool, deadline: Instant) -> anyhow::Result<()> {
    if cancelled.load(Ordering::Relaxed) || Instant::now() >= deadline {
        anyhow::bail!("transcription preparation deadline exceeded");
    }
    Ok(())
}

fn write_prepared(
    path: &std::path::Path,
    bytes: &[u8],
    cancelled: &AtomicBool,
    deadline: Instant,
) -> anyhow::Result<()> {
    use std::io::Write;
    check_preparation(cancelled, deadline)?;
    let mut file = std::fs::File::create(path)?;
    for chunk in bytes.chunks(64 * 1024) {
        check_preparation(cancelled, deadline)?;
        file.write_all(chunk)?;
    }
    check_preparation(cancelled, deadline)
}

// Retain bounded diagnostics while continuing to drain both pipes until exit.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    cap: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::new();
    let mut oversized = false;
    let mut buffer = [0; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok((output, oversized));
        }
        let retained = read.min(cap - output.len());
        output.extend_from_slice(&buffer[..retained]);
        oversized |= retained < read;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::audio::AudioData;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    fn helper(script: &str) -> (tempfile::TempDir, WhistleCli) {
        let dir = tempfile::tempdir().unwrap();
        let helper = dir.path().join("needle");
        std::fs::write(&helper, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let model = dir.path().join("whistle.cact");
        std::fs::write(&model, b"model").unwrap();
        let engine = WhistleCli::new(Artifacts { model, helper }, dir.path().to_path_buf());
        (dir, engine)
    }

    fn wav(_dir: &std::path::Path) -> AudioInput {
        AudioInput {
            duration_secs: 1.0,
            audio: AudioData::Wav(b"RIFF*\0\0\0WAVEfmt \x10\0\0\0\x01\0\x01\0\x80>\0\0\0}\0\0\x02\0\x10\0data\x06\0\0\0\x01\0\xfe\xff\x03\0".to_vec()),
        }
    }

    #[tokio::test]
    async fn returns_text_and_detected_language_from_helper_json() {
        let (dir, engine) = helper("printf '%s\\n' '{\"text\":\"Bonjour le monde.\",\"language\":\"fr\",\"ttft_ms\":123.0}'");
        let result = engine
            .transcribe(
                wav(dir.path()),
                None,
                &[],
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(result.text, "Bonjour le monde.");
        assert_eq!(result.language.as_deref(), Some("fr"));
        assert!(result.note.is_none());
    }
    #[tokio::test]
    async fn passes_language_and_keywords_without_shell_interpretation_and_cleans_them() {
        let (dir, engine) = helper(
            r#"
[ "$#" -eq 8 ] || exit 21
[ "$1" = --model ] && [ -f "$2" ] || exit 22
[ "$3" = --audio ] || exit 23
[ "$5" = --audio-language ] && [ "$6" = fr ] || exit 24
[ "$7" = --audio-keywords ] || exit 25
[ "$(cat "$8")" = 'Zoé
Rue du Lac' ] || exit 26
printf '%s\n' '{"text":"Les mots sont présents.","language":"fr"}'
"#,
        );
        let result = engine
            .transcribe(
                wav(dir.path()),
                Some("fr"),
                &["Zoé".into(), "Rue du Lac".into()],
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(result.text, "Les mots sont présents.");
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn decoded_pcm_reaches_helper_as_a_mono_16khz_pcm16_wav() {
        let (dir, engine) = helper(
            r#"
[ "$#" -eq 4 ] || exit 21
# RIFF/WAVE, PCM, mono, 16 kHz, 16-bit; sample payload is exactly [1, -2, 3].
[ "$(od -An -tx1 "$4" | tr -d ' \n')" = '524946462a00000057415645666d74201000000001000100803e0000007d00000200100064617461060000000100feff0300' ] || exit 22
printf '%s\n' '{"text":"Conversion réussie.","language":null}'
"#,
        );
        let input = AudioInput {
            duration_secs: 0.0001875,
            audio: AudioData::Pcm16kMono(vec![1, -2, 3]),
        };
        let result = engine
            .transcribe(input, None, &[], Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(result.text, "Conversion réussie.");
        assert!(result.language.is_none());
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn nonzero_exit_does_not_expose_helper_stderr_or_partial_json() {
        let (dir, engine) = helper("echo 'sensitive internal path' >&2; echo '{\"text\":\"partial\",\"language\":\"fr\"}'; exit 1");
        let failure = engine
            .transcribe(
                wav(dir.path()),
                None,
                &[],
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.code, "engine_failed");
        assert_eq!(failure.message, "Transcription engine failed");
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn deadline_kills_and_reaps_the_helper_and_removes_temporary_files() {
        let (dir, engine) = helper(
            r#"
echo $$ > "$2.pid"
while :; do :; done
printf '%s\n' '{"text":"Trop tard.","language":"fr"}'
"#,
        );
        let failure = engine
            .transcribe(
                wav(dir.path()),
                None,
                &[],
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.code, "timeout");
        let pid = std::fs::read_to_string(dir.path().join("whistle.cact.pid")).unwrap();
        assert!(!std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.trim())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success());
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn oversized_output_is_drained_and_rejected_without_hanging() {
        let (dir, engine) = helper(
            r#"
printf '{"text":"'
dd if=/dev/zero bs=1048576 count=5 2>/dev/null | tr '\000' a
printf '","language":"fr"}\n'
"#,
        );
        let result = engine
            .transcribe(
                wav(dir.path()),
                None,
                &[],
                Instant::now() + Duration::from_secs(5),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code, "engine_failed");
    }

    #[tokio::test]
    async fn rejects_garbage_and_multiline_json_without_exposing_stdout() {
        for script in [
            "printf 'not JSON'",
            "printf '{\n\"text\":\"partial\",\n\"language\":\"fr\"\n}\n'",
        ] {
            let (dir, engine) = helper(script);
            let result = engine
                .transcribe(
                    wav(dir.path()),
                    None,
                    &[],
                    Instant::now() + Duration::from_secs(2),
                )
                .await;
            assert!(result.is_err());
            let failure = result.err().unwrap();
            assert_eq!(failure.code, "engine_failed");
            assert_eq!(
                failure.message,
                "Transcription engine returned invalid output"
            );
        }
    }
    #[tokio::test]
    async fn rejects_a_temporary_directory_symlink_before_writing_outside_model_dir() {
        let (dir, engine) = helper("printf '%s\\n' '{\"text\":\"Bonjour.\",\"language\":\"fr\"}'");
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("tmp")).unwrap();
        let result = engine
            .transcribe(
                wav(dir.path()),
                Some("fr"),
                &["Zoé".into()],
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code, "engine_failed");
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn an_expired_deadline_does_not_prepare_files_or_launch_the_helper() {
        let (dir, engine) = helper("exit 23");
        let result = engine
            .transcribe(wav(dir.path()), None, &["Zoé".into()], Instant::now())
            .await
            .unwrap_err();
        assert_eq!(result.code, "timeout");
        assert!(!dir.path().join("tmp").exists());
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_waits_for_preparation_to_stop_and_clean_up() {
        let (dir, engine) = helper("exit 23");
        let input = AudioInput {
            duration_secs: 900.0,
            audio: AudioData::Pcm16kMono(vec![0; 14_400_000]),
        };
        let deadline = Instant::now() + Duration::from_secs(600);
        let task = tokio::spawn(async move { engine.transcribe(input, None, &[], deadline).await });
        let wait_until = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::fs::read_dir(dir.path().join("tmp"))
                .is_ok_and(|mut entries| entries.next().is_some())
            {
                break;
            }
            assert!(
                std::time::Instant::now() < wait_until,
                "input preparation did not start"
            );
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(600)).await;
        let result = task.await.unwrap().unwrap_err();
        assert_eq!(result.code, "timeout");
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn wav_snapshot_reaches_the_helper_unchanged() {
        let (dir, engine) = helper(
            r#"
cmp "$4" "$2.expected" || exit 23
printf '%s\n' '{"text":"Le WAV est inchangé.","language":"fr"}'
"#,
        );
        let mut input = wav(dir.path());
        let AudioData::Wav(bytes) = &mut input.audio else {
            unreachable!()
        };
        // A valid ancillary RIFF chunk must survive the pass-through unchanged.
        bytes.extend_from_slice(b"JUNK\x04\0\0\0meta");
        bytes[4..8].copy_from_slice(&54u32.to_le_bytes());
        std::fs::write(dir.path().join("whistle.cact.expected"), bytes).unwrap();
        let result = engine
            .transcribe(input, None, &[], Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(result.text, "Le WAV est inchangé.");
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }
}
