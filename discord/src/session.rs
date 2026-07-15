use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey(String);

impl SessionKey {
    pub fn dm(user_id: &str) -> Self {
        Self(format!("dm:{user_id}"))
    }
    pub fn guild_channel(guild_id: &str, channel_id: &str) -> Self {
        Self(format!("guild:{guild_id}:channel:{channel_id}"))
    }
    pub fn guild_thread(guild_id: &str, thread_id: &str) -> Self {
        Self(format!("guild:{guild_id}:thread:{thread_id}"))
    }
    pub fn directory(&self, data_dir: &Path) -> PathBuf {
        data_dir.join("sessions").join(hex(self.0.as_bytes()))
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
struct Metadata {
    key: String,
}

pub fn prepare(data_dir: &Path, key: &SessionKey) -> Result<PathBuf> {
    let directory = current_directory(data_dir, key)?;
    std::fs::create_dir_all(&directory)?;
    std::fs::write(
        directory.join("session.json"),
        serde_json::to_vec(&Metadata { key: key.0.clone() })?,
    )?;
    Ok(directory)
}

pub fn reset(data_dir: &Path, key: &SessionKey) -> Result<()> {
    let base = key.directory(data_dir);
    std::fs::create_dir_all(&base)?;
    let current = std::fs::read_to_string(base.join("current"))
        .ok()
        .and_then(|generation| generation.trim().parse::<u64>().ok())
        .unwrap_or(0);
    std::fs::write(base.join("current"), (current + 1).to_string())?;
    Ok(())
}

pub fn engage(data_dir: &Path, key: &SessionKey) -> Result<()> {
    std::fs::write(prepare(data_dir, key)?.join("engaged"), "")?;
    Ok(())
}

pub fn is_engaged(data_dir: &Path, key: &SessionKey) -> bool {
    current_directory(data_dir, key).is_ok_and(|directory| directory.join("engaged").is_file())
}

fn current_directory(data_dir: &Path, key: &SessionKey) -> Result<PathBuf> {
    let base = key.directory(data_dir);
    match std::fs::read_to_string(base.join("current")) {
        Ok(generation) => Ok(base.join(generation.trim())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(base),
        Err(error) => Err(error.into()),
    }
}

pub fn resume_id(directory: &Path) -> Option<String> {
    let path = std::fs::read_dir(directory)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .max_by(|left, right| left.file_name().cmp(&right.file_name()))?;
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
    #[test]
    fn dm_keys_are_distinct_and_safe() {
        assert_ne!(
            SessionKey::dm("1").directory(Path::new("data")),
            SessionKey::dm("2").directory(Path::new("data"))
        );
        assert!(!SessionKey::dm("../bad")
            .directory(Path::new("data"))
            .to_string_lossy()
            .contains(".."));
    }
    #[test]
    fn thread_keys_are_isolated_from_their_parent_channel() {
        assert_ne!(
            SessionKey::guild_channel("g1", "c1"),
            SessionKey::guild_thread("g1", "t1")
        );
    }
    #[test]
    fn prepare_persists_metadata() {
        let root = std::env::temp_dir().join(format!("discord-session-{}", std::process::id()));
        let key = SessionKey::dm("42");
        let path = prepare(&root, &key).unwrap();
        let value: Metadata =
            serde_json::from_slice(&std::fs::read(path.join("session.json")).unwrap()).unwrap();
        assert_eq!(value.key, "dm:42");
        let _ = std::fs::remove_dir_all(root);
    }
    #[test]
    fn resolves_newest_persisted_backend_session() {
        let root = std::env::temp_dir().join(format!("discord-resume-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("2025-01-01_old.jsonl"), "{\"id\":\"old\"}\n").unwrap();
        std::fs::write(root.join("2025-02-01_new.jsonl"), "{\"id\":\"new\"}\n").unwrap();
        assert_eq!(resume_id(&root).as_deref(), Some("new"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reset_rotates_to_a_fresh_session_directory() {
        let root = std::env::temp_dir().join(format!("discord-reset-{}", std::process::id()));
        let key = SessionKey::dm("42");
        let old = prepare(&root, &key).unwrap();
        std::fs::write(old.join("turn.jsonl"), "old context").unwrap();
        reset(&root, &key).unwrap();
        let fresh = prepare(&root, &key).unwrap();
        assert_ne!(old, fresh);
        assert_eq!(resume_id(&fresh), None);
        assert!(old.join("turn.jsonl").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn engagement_persists_until_reset() {
        let root = std::env::temp_dir().join(format!("discord-engaged-{}", std::process::id()));
        let key = SessionKey::guild_thread("g1", "t1");
        assert!(!is_engaged(&root, &key));
        assert!(!key.directory(&root).exists());
        engage(&root, &key).unwrap();
        assert!(is_engaged(&root, &key));
        reset(&root, &key).unwrap();
        assert!(!is_engaged(&root, &key));
        let _ = std::fs::remove_dir_all(root);
    }
}
