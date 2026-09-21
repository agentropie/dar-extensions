use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub const DEFAULT_IDLE_MINUTES: u64 = 360;
pub const EXPIRED_NOTICE: &str = "Previous session expired; starting fresh.";
pub const RESET_REPLY: &str = "Context cleared, new session started.";

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct SessionsConfig {
    pub idle_minutes: u64,
}
impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            idle_minutes: DEFAULT_IDLE_MINUTES,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Pointer {
    generation: String,
    last_inbound: u64,
}

pub fn valid_wa_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())
}
pub fn is_reset(text: &str) -> bool {
    matches!(text.trim(), "/new" | "/reset")
}

pub struct Prepared {
    pub session_dir: PathBuf,
    pub rotated: bool,
}
pub struct SessionStore {
    chat_dir: PathBuf,
}
impl SessionStore {
    pub fn new(root: &Path, wa_id: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            valid_wa_id(wa_id),
            "WhatsApp wa_id must contain ASCII digits only"
        );
        Ok(Self {
            chat_dir: root.join(wa_id),
        })
    }
    fn pointer(&self) -> PathBuf {
        self.chat_dir.join("current.json")
    }
    fn read(&self) -> Option<Pointer> {
        serde_json::from_str(&std::fs::read_to_string(self.pointer()).ok()?).ok()
    }
    fn rotate(&self, now: u64) -> std::io::Result<PathBuf> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let generation = format!("{now}-{:06}", NEXT.fetch_add(1, Ordering::Relaxed));
        let dir = self.chat_dir.join(&generation);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            self.pointer(),
            serde_json::to_string(&Pointer {
                generation,
                last_inbound: now,
            })
            .unwrap(),
        )?;
        Ok(dir)
    }
    pub fn reset(&self, now: u64) -> std::io::Result<PathBuf> {
        self.rotate(now)
    }
    pub fn prepare(&self, idle_minutes: u64, now: u64) -> std::io::Result<Prepared> {
        if let Some(pointer) = self.read() {
            let expired = idle_minutes != 0
                && now.saturating_sub(pointer.last_inbound) >= idle_minutes.saturating_mul(60);
            if !expired {
                std::fs::write(
                    self.pointer(),
                    serde_json::to_string(&Pointer {
                        generation: pointer.generation.clone(),
                        last_inbound: now,
                    })
                    .unwrap(),
                )?;
                return Ok(Prepared {
                    session_dir: self.chat_dir.join(pointer.generation),
                    rotated: false,
                });
            }
            return Ok(Prepared {
                session_dir: self.rotate(now)?,
                rotated: true,
            });
        }
        Ok(Prepared {
            session_dir: self.rotate(now)?,
            rotated: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_path_ids() {
        assert!(!valid_wa_id("../x"));
        assert!(valid_wa_id("33612345678"));
    }
}
