use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionKey(String);

impl SessionKey {
    pub fn dm(user_id: &str) -> Self {
        Self(format!("dm:{user_id}"))
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
    let directory = key.directory(data_dir);
    std::fs::create_dir_all(&directory)?;
    std::fs::write(
        directory.join("session.json"),
        serde_json::to_vec(&Metadata { key: key.0.clone() })?,
    )?;
    Ok(directory)
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
}
