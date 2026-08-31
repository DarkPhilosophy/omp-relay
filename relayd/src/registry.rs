use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use rand::{Rng, distr::Alphanumeric};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Client {
    pub uuid: Uuid,
    pub token_sha256: String,
    pub name: String,
    pub created: u64,
    pub last_seen: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PairingCode {
    pub code: String,
    pub expires_unix: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Registry {
    pub clients: Vec<Client>,
    pub codes: Vec<PairingCode>,
}

pub struct RegistryLock(File);

impl Drop for RegistryLock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to unlock relay registry");
        }
    }
}

pub fn registry_lock(path: &Path) -> io::Result<RegistryLock> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock_path = path.with_extension("lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(lock_path)?;
    file.lock()?;
    Ok(RegistryLock(file))
}

impl Registry {
    pub fn load(path: &Path) -> io::Result<Self> {
        match fs::read(path) {
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|error| io::Error::new(ErrorKind::InvalidData, error)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error),
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension(format!("json.tmp-{}", Uuid::new_v4()));
        let data = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary)?;
        use std::io::Write as _;
        file.write_all(&data)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    }

    pub fn purge_expired_codes(&mut self, now: u64) {
        self.codes.retain(|entry| entry.expires_unix > now);
    }

    pub fn create_pairing_code(&mut self, now: u64) -> String {
        self.purge_expired_codes(now);
        loop {
            let code: String = rand::rng()
                .sample_iter(Alphanumeric)
                .filter(|byte| byte.is_ascii_alphanumeric())
                .map(char::from)
                .map(|character| character.to_ascii_uppercase())
                .take(8)
                .collect();
            if !self.codes.iter().any(|entry| entry.code == code) {
                self.codes.push(PairingCode {
                    code: code.clone(),
                    expires_unix: now + 600,
                });
                return code;
            }
        }
    }

    pub fn consume_pairing_code(&mut self, code: &str, now: u64) -> bool {
        self.purge_expired_codes(now);
        let normalized = code.trim().to_ascii_uppercase();
        let Some(index) = self.codes.iter().position(|entry| entry.code == normalized) else {
            return false;
        };
        self.codes.swap_remove(index);
        true
    }

    pub fn add_client(&mut self, name: String, now: u64) -> (Uuid, String) {
        let uuid = Uuid::new_v4();
        let token_bytes: [u8; 32] = rand::random();
        let token = hex_lower(&token_bytes);
        self.clients.push(Client {
            uuid,
            token_sha256: token_hash(&token),
            name,
            created: now,
            last_seen: now,
        });
        (uuid, token)
    }

    pub fn revoke(&mut self, uuid: Uuid) -> bool {
        let original_length = self.clients.len();
        self.clients.retain(|client| client.uuid != uuid);
        original_length != self.clients.len()
    }
}

pub fn registry_path() -> io::Result<PathBuf> {
    dirs::config_dir()
        .map(|directory| directory.join("omp-relayd").join("registry.json"))
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                "could not locate the user configuration directory",
            )
        })
}

pub fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn token_hash(token: &str) -> String {
    hex_lower(&Sha256::digest(token.as_bytes()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_codes_are_one_time_and_case_insensitive() {
        let mut registry = Registry::default();
        let code = registry.create_pairing_code(1_000);

        assert_eq!(code.len(), 8);
        assert!(registry.consume_pairing_code(&code.to_ascii_lowercase(), 1_001));
        assert!(!registry.consume_pairing_code(&code, 1_002));
    }

    #[test]
    fn expired_pairing_codes_cannot_be_consumed() {
        let mut registry = Registry::default();
        let code = registry.create_pairing_code(1_000);

        assert!(!registry.consume_pairing_code(&code, 1_600));
        assert!(registry.codes.is_empty());
    }

    #[test]
    fn client_tokens_are_stored_as_hashes_and_can_be_revoked() {
        let mut registry = Registry::default();
        let (uuid, token) = registry.add_client("laptop".to_owned(), 1_000);
        let client = registry.clients.first().expect("client was added");

        assert_ne!(client.token_sha256, token);
        assert_eq!(client.token_sha256, token_hash(&token));
        assert!(registry.revoke(uuid));
        assert!(!registry.revoke(uuid));
    }

    #[test]
    fn registry_round_trips_through_disk() {
        let directory = std::env::temp_dir().join(format!("omp-relayd-test-{}", Uuid::new_v4()));
        let path = directory.join("registry.json");
        let mut registry = Registry::default();
        registry.add_client("atv".to_owned(), 1_000);

        let _lock = registry_lock(&path).expect("registry lock is acquired");
        registry.save(&path).expect("registry saves");
        let loaded = Registry::load(&path).expect("registry loads");

        assert_eq!(loaded.clients.len(), 1);
        assert_eq!(loaded.clients[0].name, "atv");
        drop(_lock);
        fs::remove_dir_all(directory).expect("temporary registry directory is removed");
    }

    #[cfg(unix)]
    #[test]
    fn registry_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!("omp-relayd-mode-test-{}", Uuid::new_v4()));
        let path = directory.join("registry.json");
        let registry = Registry::default();
        let lock = registry_lock(&path).expect("registry lock is acquired");
        registry.save(&path).expect("registry saves");

        assert_eq!(fs::metadata(&path).expect("registry metadata").permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::metadata(path.with_extension("lock")).expect("lock metadata").permissions().mode() & 0o777, 0o600);

        drop(lock);
        fs::remove_dir_all(directory).expect("temporary registry directory is removed");
    }
}
