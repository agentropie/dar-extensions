use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub const DEFAULT_IDLE_MINUTES: u64 = 360;
pub const EXPIRED_NOTICE: &str = "Previous session expired; starting fresh.";
pub const RESET_REPLY: &str = "Context cleared, new session started.";
const POINTER: &str = "current.json";
const MIGRATION_MARKER: &str = ".migration-in-progress";

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct SessionsConfig {
    pub idle_minutes: u64,
    pub reset_users: Vec<String>,
}

impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            idle_minutes: DEFAULT_IDLE_MINUTES,
            reset_users: Vec::new(),
        }
    }
}

pub fn reset_authorized(nick: &str, users: &[String]) -> bool {
    users.is_empty() || users.iter().any(|user| user.eq_ignore_ascii_case(nick))
}

pub fn is_reset_command(text: &str) -> bool {
    matches!(text.trim(), "/new" | "/reset")
}

#[derive(Debug, Deserialize, Serialize)]
struct Pointer {
    generation: String,
    last_activity: u64,
}

pub struct Prepared {
    pub directory: PathBuf,
    pub expired: bool,
}

pub struct Store {
    base: PathBuf,
}

impl Store {
    pub fn new(root: &Path, key: &str) -> Self {
        Self {
            base: root.join(key),
        }
    }

    fn pointer_path(&self) -> PathBuf {
        self.base.join(POINTER)
    }

    fn valid_generation(name: &str) -> bool {
        let mut components = Path::new(name).components();
        let Some((timestamp, suffix)) = name.split_once('-') else {
            return false;
        };
        matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none()
            && !timestamp.is_empty()
            && !suffix.is_empty()
            && timestamp.bytes().all(|byte| byte.is_ascii_digit())
            && suffix.bytes().all(|byte| byte.is_ascii_digit())
    }

    fn read_pointer(&self) -> std::io::Result<Option<Pointer>> {
        let bytes = match std::fs::read(self.pointer_path()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let pointer: Pointer = serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if !Self::valid_generation(&pointer.generation) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid session generation",
            ));
        }
        let directory = self.base.join(&pointer.generation);
        let metadata = std::fs::metadata(&directory)?;
        if !metadata.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "session generation is not a directory",
            ));
        }
        Ok(Some(pointer))
    }

    fn write_pointer(&self, pointer: &Pointer) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.base)?;
        let temporary = self.base.join(format!(
            ".current-{}.tmp",
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(
            &temporary,
            serde_json::to_vec(pointer).expect("pointer serializes"),
        )?;
        match std::fs::rename(&temporary, self.pointer_path()) {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = std::fs::remove_file(temporary);
                Err(error)
            }
        }
    }

    fn allocate(&self, now: u64) -> std::io::Result<(String, PathBuf)> {
        std::fs::create_dir_all(&self.base)?;
        loop {
            let generation = format!("{now}-{:06}", COUNTER.fetch_add(1, Ordering::Relaxed));
            let directory = self.base.join(&generation);
            match std::fs::create_dir(&directory) {
                Ok(()) => return Ok((generation, directory)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn rotate(&self, now: u64) -> std::io::Result<PathBuf> {
        let (generation, directory) = self.allocate(now)?;
        self.write_pointer(&Pointer {
            generation,
            last_activity: now,
        })?;
        Ok(directory)
    }

    fn root_entries(&self) -> std::io::Result<Vec<PathBuf>> {
        match std::fs::read_dir(&self.base) {
            Ok(entries) => entries
                .map(|entry| entry.map(|entry| entry.path()))
                .filter(|entry| {
                    entry.as_ref().is_ok_and(|path| {
                        path.file_name().is_some_and(|name| {
                            name != POINTER && !name.to_string_lossy().starts_with(".current-")
                        })
                    })
                })
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    fn migration_generation(&self) -> std::io::Result<Option<PathBuf>> {
        let mut candidates = Vec::new();
        for path in self.root_entries()? {
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().expect("entry has name").to_string_lossy();
            if path.join(MIGRATION_MARKER).is_file() && !Self::valid_generation(&name) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "malformed migration generation",
                ));
            }
            if Self::valid_generation(&name) {
                candidates.push(path);
            }
        }
        if candidates.len() > 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ambiguous migration generation",
            ));
        }
        Ok(candidates.pop())
    }

    fn migrate(&self, now: u64) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(&self.base)?;
        let directory = match self.migration_generation()? {
            Some(directory) => directory,
            None => {
                let (_, directory) = self.allocate(now)?;
                std::fs::write(directory.join(MIGRATION_MARKER), [])?;
                directory
            }
        };
        for source in self.root_entries()? {
            if source == directory {
                continue;
            }
            let target = directory.join(source.file_name().expect("entry has name"));
            std::fs::rename(source, target)?;
        }
        let generation = directory
            .file_name()
            .expect("generation has name")
            .to_string_lossy()
            .into_owned();
        self.write_pointer(&Pointer {
            generation,
            last_activity: now,
        })?;
        match std::fs::remove_file(directory.join(MIGRATION_MARKER)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(directory)
    }

    pub fn prepare(&self, idle_minutes: u64, now: u64) -> std::io::Result<Prepared> {
        match self.read_pointer()? {
            Some(pointer)
                if idle_minutes != 0
                    && now.saturating_sub(pointer.last_activity)
                        >= idle_minutes.saturating_mul(60) =>
            {
                Ok(Prepared {
                    directory: self.rotate(now)?,
                    expired: true,
                })
            }
            Some(pointer) => {
                self.write_pointer(&Pointer {
                    generation: pointer.generation.clone(),
                    last_activity: now,
                })?;
                Ok(Prepared {
                    directory: self.base.join(pointer.generation),
                    expired: false,
                })
            }
            None => Ok(Prepared {
                directory: self.migrate(now)?,
                expired: false,
            }),
        }
    }

    pub fn reset(&self, now: u64) -> std::io::Result<PathBuf> {
        if self.read_pointer()?.is_none() {
            self.migrate(now)?;
        }
        self.rotate(now)
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "irc-session-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn defaults_commands_and_authorization() {
        let config = SessionsConfig::default();
        assert_eq!(config.idle_minutes, 360);
        assert!(config.reset_users.is_empty());
        assert!(is_reset_command(" /new "));
        assert!(is_reset_command("/reset"));
        assert!(!is_reset_command("/reset now"));
        assert!(reset_authorized("any", &[]));
        assert!(reset_authorized("ALICE", &["Alice".into()]));
        assert!(!reset_authorized("bob", &["Alice".into()]));
    }

    #[test]
    fn ttl_boundary_and_zero() {
        let root = root();
        let store = Store::new(&root, "x");
        let first = store.prepare(360, 100).unwrap();
        assert_eq!(
            first.directory,
            store.prepare(360, 100 + 359 * 60).unwrap().directory
        );
        let expired = store.prepare(360, 100 + 719 * 60).unwrap();
        assert!(expired.expired);
        assert_ne!(first.directory, expired.directory);
        assert!(first.directory.exists());

        let zero = Store::new(&root, "zero");
        let first = zero.prepare(0, 0).unwrap();
        assert_eq!(
            first.directory,
            zero.prepare(0, u64::MAX).unwrap().directory
        );
    }

    #[test]
    fn ambient_passivity_does_not_create_store_or_refresh_ttl() {
        let root = root();
        let store = Store::new(&root, "passive");
        assert!(!store.base.exists());
        let first = store.prepare(1, 100).unwrap();
        assert!(store.prepare(1, 160).unwrap().expired);
        assert!(first.directory.exists());
    }

    #[test]
    fn legacy_migrates_and_rotation_is_append_only() {
        let root = root();
        let base = root.join("x");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("old.jsonl"), "context").unwrap();
        let store = Store::new(&root, "x");
        let legacy = store.prepare(360, 1).unwrap().directory;
        assert!(legacy.join("old.jsonl").exists());
        let fresh = store.reset(2).unwrap();
        assert_ne!(legacy, fresh);
        assert!(legacy.join("old.jsonl").exists());
    }

    #[test]
    fn first_reset_archives_legacy_before_fresh_generation() {
        let root = root();
        let base = root.join("x");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("old.jsonl"), "context").unwrap();
        let store = Store::new(&root, "x");
        let fresh = store.reset(1).unwrap();
        assert!(!fresh.join("old.jsonl").exists());
        assert!(std::fs::read_dir(base)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.path() != fresh && entry.path().join("old.jsonl").exists()));
    }

    #[test]
    fn allocation_skips_collision() {
        let root = root();
        let store = Store::new(&root, "x");
        std::fs::create_dir_all(&store.base).unwrap();
        let next = COUNTER.load(Ordering::Relaxed);
        let collision = store.base.join(format!("1-{next:06}"));
        std::fs::create_dir(&collision).unwrap();
        assert_ne!(store.rotate(1).unwrap(), collision);
    }

    #[test]
    fn malformed_traversal_and_missing_pointer_targets_error() {
        for value in [
            b"bad".as_slice(),
            br#"{"generation":"../x","last_activity":1}"#,
        ] {
            let root = root();
            let store = Store::new(&root, "x");
            std::fs::create_dir_all(&store.base).unwrap();
            std::fs::write(store.pointer_path(), value).unwrap();
            assert!(store.prepare(1, 2).is_err());
        }
        let root = root();
        let store = Store::new(&root, "missing");
        std::fs::create_dir_all(&store.base).unwrap();
        std::fs::write(
            store.pointer_path(),
            br#"{"generation":"1-000001","last_activity":1}"#,
        )
        .unwrap();
        assert_eq!(
            store.prepare(1, 2).err().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn interrupted_marked_migration_resumes_same_generation_without_nesting() {
        let root = root();
        let store = Store::new(&root, "x");
        std::fs::create_dir_all(&store.base).unwrap();
        let generation = store.base.join("1-000001");
        std::fs::create_dir(&generation).unwrap();
        std::fs::write(generation.join(MIGRATION_MARKER), []).unwrap();
        std::fs::write(generation.join("moved.jsonl"), "moved").unwrap();
        std::fs::write(store.base.join("remaining.jsonl"), "remaining").unwrap();

        let prepared = store.prepare(360, 2).unwrap();
        assert_eq!(prepared.directory, generation);
        assert!(generation.join("moved.jsonl").exists());
        assert!(generation.join("remaining.jsonl").exists());
        assert!(!generation.join(MIGRATION_MARKER).exists());
        assert!(!generation.join("1-000001").exists());
    }

    #[test]
    fn unmarked_migration_generation_recovers_but_ambiguous_candidates_error() {
        let root = root();
        let store = Store::new(&root, "recover");
        std::fs::create_dir_all(&store.base).unwrap();
        let generation = store.base.join("1-000001");
        std::fs::create_dir(&generation).unwrap();
        std::fs::write(store.base.join("remaining.jsonl"), "remaining").unwrap();
        assert_eq!(store.prepare(360, 2).unwrap().directory, generation);
        assert!(generation.join("remaining.jsonl").exists());

        let store = Store::new(&root, "ambiguous");
        std::fs::create_dir_all(store.base.join("1-000001")).unwrap();
        std::fs::create_dir_all(store.base.join("1-000002")).unwrap();
        let error = match store.prepare(360, 2) {
            Ok(_) => panic!("ambiguous generations must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn malformed_generation_names_are_rejected() {
        for name in ["--", "1-", "-1", "1--2"] {
            assert!(!Store::valid_generation(name), "accepted {name}");
        }
        assert!(Store::valid_generation("1-000001"));
    }
}
