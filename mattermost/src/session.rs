use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey(String);
impl SessionKey {
    pub fn new(key: String) -> Self {
        Self(key)
    }
    pub fn dm(channel: &str) -> Self {
        Self(format!("dm:{channel}"))
    }
    pub fn channel(channel: &str) -> Self {
        Self(format!("channel:{channel}:root"))
    }
    pub fn thread(is_dm: bool, channel: &str, root: &str) -> Self {
        Self(format!(
            "{}:{channel}:thread:{root}",
            if is_dm { "dm" } else { "channel" }
        ))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn is_thread(&self) -> bool {
        self.0.contains(":thread:")
    }
    pub fn root_id(&self) -> Option<&str> {
        self.0.split_once(":thread:").map(|(_, root)| root)
    }
    pub fn directory(&self, data_dir: &Path) -> PathBuf {
        data_dir.join("sessions").join(hex(self.0.as_bytes()))
    }
}
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
struct Metadata {
    key: String,
}
const ACTIVITY_FILE: &str = "last_activity";
pub const EXPIRED_NOTICE: &str = "Previous session expired; starting fresh.";
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("metadata path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn read_activity(data_dir: &Path, key: &SessionKey) -> Result<Option<u64>> {
    match std::fs::read_to_string(key.directory(data_dir).join(ACTIVITY_FILE)) {
        Ok(value) => Ok(Some(value.trim().parse().context("invalid last_activity")?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub fn is_expired(data_dir: &Path, key: &SessionKey, idle_minutes: u64, now: u64) -> Result<bool> {
    if idle_minutes == 0 {
        return Ok(false);
    }
    Ok(read_activity(data_dir, key)?
        .is_some_and(|last| now.saturating_sub(last) >= idle_minutes.saturating_mul(60)))
}
pub fn prepare_activity(
    data_dir: &Path,
    key: &SessionKey,
    idle_minutes: u64,
    now: u64,
) -> Result<bool> {
    let expired = is_expired(data_dir, key, idle_minutes, now)?;
    if expired {
        reset(data_dir, key)?;
    }
    atomic_write(
        &key.directory(data_dir).join(ACTIVITY_FILE),
        now.to_string().as_bytes(),
    )?;
    Ok(expired)
}
pub fn prepare(data_dir: &Path, key: &SessionKey) -> Result<PathBuf> {
    let directory = current_directory(data_dir, key)?;
    std::fs::create_dir_all(&directory)?;
    let metadata = directory.join("session.json");
    match std::fs::read(&metadata) {
        Ok(bytes) => {
            let stored: Metadata =
                serde_json::from_slice(&bytes).context("invalid session metadata")?;
            anyhow::ensure!(stored.key == key.0, "session metadata key mismatch");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            atomic_write(
                &metadata,
                &serde_json::to_vec(&Metadata { key: key.0.clone() })?,
            )?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(directory)
}
fn read_current(base: &Path) -> Result<Option<u64>> {
    match std::fs::read_to_string(base.join("current")) {
        Ok(value) => Ok(Some(
            value.trim().parse().context("invalid current generation")?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub fn reset(data_dir: &Path, key: &SessionKey) -> Result<()> {
    let base = key.directory(data_dir);
    std::fs::create_dir_all(&base)?;
    let mut generation = read_current(&base)?
        .unwrap_or(0)
        .checked_add(1)
        .context("generation overflow")?;
    while base.join(generation.to_string()).exists() {
        generation = generation.checked_add(1).context("generation overflow")?;
    }
    std::fs::create_dir(base.join(generation.to_string()))?;
    atomic_write(&base.join("current"), generation.to_string().as_bytes())
}
pub fn engage(data_dir: &Path, key: &SessionKey) -> Result<()> {
    std::fs::write(prepare(data_dir, key)?.join("engaged"), "")?;
    Ok(())
}
pub fn is_engaged(data_dir: &Path, key: &SessionKey) -> bool {
    current_directory(data_dir, key).is_ok_and(|directory| directory.join("engaged").is_file())
}
pub fn is_active_engagement(
    data_dir: &Path,
    key: &SessionKey,
    idle_minutes: u64,
    now: u64,
) -> bool {
    is_expired(data_dir, key, idle_minutes, now).is_ok_and(|expired| !expired)
        && is_engaged(data_dir, key)
}
pub fn reset_with_activity(data_dir: &Path, key: &SessionKey, now: u64) -> Result<()> {
    reset(data_dir, key)?;
    atomic_write(
        &key.directory(data_dir).join(ACTIVITY_FILE),
        now.to_string().as_bytes(),
    )
}
fn current_directory(data_dir: &Path, key: &SessionKey) -> Result<PathBuf> {
    let base = key.directory(data_dir);
    Ok(match read_current(&base)? {
        Some(generation) => base.join(generation.to_string()),
        None => base,
    })
}
pub fn resume_id(directory: &Path) -> Option<String> {
    let path = std::fs::read_dir(directory)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .max_by(|a, b| a.file_name().cmp(&b.file_name()))?;
    let content = std::fs::read_to_string(path).ok()?;
    let first = content.lines().find(|line| !line.trim().is_empty())?;
    serde_json::from_str::<serde_json::Value>(first)
        .ok()?
        .get("id")?
        .as_str()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| format!("{byte:02x}").chars().collect::<Vec<_>>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root(name: &str) -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../foundation-test-data")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        root
    }
    #[test]
    fn session_reset_preserves_old_backend_data_but_removes_engagement() {
        let root = root("reset");
        let key = SessionKey::thread(
            false,
            "cccccccccccccccccccccccccc",
            "pppppppppppppppppppppppppp",
        );
        assert_eq!(
            key.as_str(),
            "channel:cccccccccccccccccccccccccc:thread:pppppppppppppppppppppppppp"
        );
        let old = prepare(&root, &key).unwrap();
        std::fs::write(
            old.join("old.jsonl"),
            "{\"id\":\"xxxxxxxxxxxxxxxxxxxxxxxxxx\"}\n",
        )
        .unwrap();
        engage(&root, &key).unwrap();
        assert!(is_engaged(&root, &key));
        reset(&root, &key).unwrap();
        let fresh = prepare(&root, &key).unwrap();
        assert_ne!(old, fresh);
        assert_eq!(
            resume_id(&old).as_deref(),
            Some("xxxxxxxxxxxxxxxxxxxxxxxxxx")
        );
        assert_eq!(resume_id(&fresh), None);
        assert!(!is_engaged(&root, &key));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn corrupt_session_metadata_never_reuses_a_backend_session() {
        let root = root("bad-metadata");
        let key = SessionKey::dm("cccccccccccccccccccccccccc");
        let directory = prepare(&root, &key).unwrap();
        std::fs::write(directory.join("session.json"), "not-json").unwrap();
        assert!(prepare(&root, &key).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn idle_expiry_rotates_at_the_exact_boundary_and_zero_disables_it() {
        let root = root("expiry");
        let key = SessionKey::thread(
            true,
            "dddddddddddddddddddddddddd",
            "tttttttttttttttttttttttttt",
        );
        assert!(!prepare_activity(&root, &key, 1, 100).unwrap());
        engage(&root, &key).unwrap();
        let old = prepare(&root, &key).unwrap();
        assert!(is_active_engagement(&root, &key, 1, 159));
        assert!(!is_active_engagement(&root, &key, 1, 160));
        assert!(prepare_activity(&root, &key, 1, 160).unwrap());
        assert_ne!(prepare(&root, &key).unwrap(), old);
        assert!(!is_engaged(&root, &key));
        assert!(!prepare_activity(&root, &key, 0, u64::MAX).unwrap());
        reset_with_activity(&root, &key, 1_000_000).unwrap();
        assert!(!is_expired(&root, &key, 360, 1_000_001).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn corrupt_generation_and_activity_fail_closed() {
        let root = root("corrupt");
        let key = SessionKey::dm("cccccccccccccccccccccccccc");
        let directory = prepare(&root, &key).unwrap();
        std::fs::write(directory.join("last_activity"), "invalid").unwrap();
        assert!(prepare_activity(&root, &key, 1, 200).is_err());
        assert!(!is_active_engagement(&root, &key, 1, 200));
        std::fs::write(directory.join("current"), "invalid").unwrap();
        assert!(prepare(&root, &key).is_err());
        assert!(reset(&root, &key).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn unsafe_keys_stay_inside_the_session_store_and_topics_are_distinct() {
        let root = Path::new("data");
        let unsafe_key = SessionKey::dm("../../outside");
        let directory = unsafe_key.directory(root);
        assert_eq!(directory.parent().unwrap(), Path::new("data/sessions"));
        assert_ne!(
            SessionKey::dm("cccccccccccccccccccccccccc"),
            SessionKey::thread(
                true,
                "cccccccccccccccccccccccccc",
                "tttttttttttttttttttttttttt"
            )
        );
        assert_ne!(
            SessionKey::channel("cccccccccccccccccccccccccc"),
            SessionKey::thread(
                false,
                "cccccccccccccccccccccccccc",
                "tttttttttttttttttttttttttt"
            )
        );
    }
}
