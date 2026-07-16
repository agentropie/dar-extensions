use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
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
pub fn history_key(
    guild_id: Option<&str>,
    channel_id: &str,
    _parent_channel_id: Option<&str>,
    author_id: Option<&str>,
) -> String {
    match guild_id {
        Some(guild_id) => format!("guild:{guild_id}:channel:{channel_id}"),
        None => format!("dm:{}", author_id.unwrap_or_default()),
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
    atomic_write(
        &directory.join("session.json"),
        &serde_json::to_vec(&Metadata { key: key.0.clone() })?,
    )?;
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
        let p = std::env::temp_dir().join(format!("discord-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }
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
    fn thread_keys_are_isolated_from_parent() {
        assert_ne!(
            SessionKey::guild_channel("g", "c"),
            SessionKey::guild_thread("g", "t")
        );
    }
    #[test]
    fn history_is_scoped() {
        assert_ne!(
            history_key(Some("g"), "t", Some("c"), Some("u")),
            history_key(Some("g"), "c", None, Some("u"))
        );
        assert_ne!(
            history_key(None, "dm", None, Some("1")),
            history_key(None, "dm", None, Some("2"))
        );
    }
    #[test]
    fn prepare_persists_metadata() {
        let r = root("prepare");
        let k = SessionKey::dm("42");
        let p = prepare(&r, &k).unwrap();
        let v: Metadata =
            serde_json::from_slice(&std::fs::read(p.join("session.json")).unwrap()).unwrap();
        assert_eq!(v.key, "dm:42");
    }
    #[test]
    fn resolves_newest_persisted_backend_session() {
        let r = root("resume");
        std::fs::create_dir_all(&r).unwrap();
        std::fs::write(r.join("2025-01-01_old.jsonl"), "{\"id\":\"old\"}\n").unwrap();
        std::fs::write(r.join("2025-02-01_new.jsonl"), "{\"id\":\"new\"}\n").unwrap();
        assert_eq!(resume_id(&r).as_deref(), Some("new"));
    }
    #[test]
    fn reset_skips_existing_and_preserves_old() {
        let r = root("reset");
        let k = SessionKey::dm("42");
        let old = prepare(&r, &k).unwrap();
        std::fs::write(old.join("turn.jsonl"), "{\"id\":\"old\"}\n").unwrap();
        std::fs::create_dir_all(k.directory(&r).join("1")).unwrap();
        reset(&r, &k).unwrap();
        let fresh = prepare(&r, &k).unwrap();
        assert_eq!(fresh.file_name().unwrap(), "2");
        assert_eq!(resume_id(&old).as_deref(), Some("old"));
        assert_eq!(resume_id(&fresh), None);
    }
    #[test]
    fn ttl_boundary_zero_and_legacy() {
        let r = root("ttl");
        let k = SessionKey::dm("ttl");
        let legacy = prepare(&r, &k).unwrap();
        std::fs::write(legacy.join("old.jsonl"), "{\"id\":\"old\"}\n").unwrap();
        assert!(!prepare_activity(&r, &k, 360, 100).unwrap());
        assert!(!is_expired(&r, &k, 360, 100 + 359 * 60).unwrap());
        assert!(prepare_activity(&r, &k, 360, 100 + 360 * 60).unwrap());
        let fresh = prepare(&r, &k).unwrap();
        assert_ne!(legacy, fresh);
        assert_eq!(resume_id(&legacy).as_deref(), Some("old"));
        assert_eq!(resume_id(&fresh), None);
        assert!(!prepare_activity(&r, &SessionKey::dm("zero"), 0, u64::MAX).unwrap());
    }
    #[test]
    fn corrupt_metadata_errors_and_engagement_fails_closed() {
        let r = root("corrupt");
        let k = SessionKey::dm("x");
        std::fs::create_dir_all(k.directory(&r)).unwrap();
        std::fs::write(k.directory(&r).join(ACTIVITY_FILE), "bad").unwrap();
        assert!(prepare_activity(&r, &k, 1, 2).is_err());
        assert!(!is_active_engagement(&r, &k, 1, 2));
        std::fs::write(k.directory(&r).join("current"), "bad").unwrap();
        assert!(prepare(&r, &k).is_err());
        assert!(reset(&r, &k).is_err());
    }
    #[test]
    fn explicit_reset_refreshes_activity() {
        let r = root("activity");
        let k = SessionKey::dm("x");
        prepare_activity(&r, &k, 360, 100).unwrap();
        reset_with_activity(&r, &k, 1_000_000).unwrap();
        assert!(!is_expired(&r, &k, 360, 1_000_001).unwrap());
    }
    #[test]
    fn expired_engagement_is_inactive_at_boundary() {
        let r = root("expired-engagement");
        let k = SessionKey::guild_thread("g", "t");
        prepare_activity(&r, &k, 360, 100).unwrap();
        engage(&r, &k).unwrap();
        assert!(!is_active_engagement(&r, &k, 360, 100 + 360 * 60));
    }
    #[test]
    fn engagement_persists_until_reset() {
        let r = root("engage");
        let k = SessionKey::guild_thread("g", "t");
        engage(&r, &k).unwrap();
        assert!(is_engaged(&r, &k));
        reset(&r, &k).unwrap();
        assert!(!is_engaged(&r, &k));
    }
}
