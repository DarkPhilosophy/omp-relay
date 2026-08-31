mod forward;
mod registry;

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use registry::{Registry, registry_lock, registry_path, registry_read_lock, token_hash, unix_time};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Minimum seconds between durable `last_seen` writes per client.
const LAST_SEEN_FLUSH_SECS: u64 = 60;
const MAX_PINNED_CLIENTS: usize = 128;

#[derive(Parser)]
#[command(
    name = "omp-relayd",
    version,
    about = "Authenticated streaming relay for OMP AI provider traffic"
)]
struct Cli {
    #[arg(long, global = true)]
    registry: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "0.0.0.0:8787")]
        bind: SocketAddr,
    },
    Pair,
    List,
    Revoke {
        uuid: Uuid,
    },
    /// Install, remove, or inspect a systemd user service for the relay (Linux).
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Run deterministic local checks without starting a listener or changing the real registry.
    SelfTest,
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Write a systemd user unit, then enable and start it.
    Install {
        #[arg(long)]
        bind: SocketAddr,
        /// Write the unit file only; do not enable or start the service.
        #[arg(long)]
        no_enable: bool,
    },
    /// Stop, disable, and remove the systemd user unit.
    Uninstall,
    /// Show the systemd user service status.
    Status,
}

#[derive(Clone)]
pub struct AuthenticatedClient {
    pub uuid: Uuid,
    pub name: String,
}

pub struct AppState {
    registry_path: PathBuf,
    started: Instant,
    debug: AtomicBool,
    active_streams: AtomicUsize,
    pair_attempts: Mutex<HashMap<IpAddr, PairRateWindow>>,
    pinned_clients: Mutex<HashMap<String, PinnedHttpClient>>,
}
#[derive(Clone)]
pub struct PinnedHttpClient {
    client: reqwest::Client,
    addresses: Vec<SocketAddr>,
}

struct PairRateWindow {
    minute: u64,
    count: u8,
}

struct StreamGuard<'a>(&'a AtomicUsize);

impl Drop for StreamGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AppState {
    fn stream_guard(&self) -> StreamGuard<'_> {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
        StreamGuard(&self.active_streams)
    }
}

#[derive(Deserialize)]
struct PairRequest {
    code: String,
    name: String,
}

#[derive(Serialize)]
struct PairResponse {
    uuid: Uuid,
    token: String,
}

#[derive(Deserialize)]
struct DebugRequest {
    enabled: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let path = match cli.registry {
        Some(path) => path,
        None => registry_path()?,
    };

    match cli.command {
        Command::Serve { bind } => serve(path, bind).await?,
        Command::Pair => {
            let _lock = registry_lock(&path)?;
            let mut registry = Registry::load(&path)?;
            let code = registry.create_pairing_code(unix_time());
            registry.save(&path)?;
            println!("{code}");
        }
        Command::List => {
            let _lock = registry_read_lock(&path)?;
            let registry = Registry::load(&path)?;
            if registry.clients.is_empty() {
                println!("No paired clients.");
            } else {
                println!("UUID\tNAME\tCREATED\tLAST_SEEN");
                for client in registry.clients {
                    println!(
                        "{}\t{}\t{}\t{}",
                        client.uuid, client.name, client.created, client.last_seen
                    );
                }
            }
        }
        Command::Revoke { uuid } => {
            let _lock = registry_lock(&path)?;
            let mut registry = Registry::load(&path)?;
            if !registry.revoke(uuid) {
                return Err(format!("client {uuid} not found").into());
            }
            registry.save(&path)?;
            println!("Revoked {uuid}");
        }
        Command::Service { action } => service(&path, action)?,
        Command::SelfTest => self_test()?,
    }
    Ok(())
}

fn self_test() -> Result<(), Box<dyn std::error::Error>> {
    let now = 1_000;
    let mut registry = Registry::default();
    let code = registry.create_pairing_code(now);
    if !registry.consume_pairing_code(&code.to_ascii_lowercase(), now + 1) {
        return Err("pairing-code round trip failed".into());
    }
    if registry.consume_pairing_code(&code, now + 2) {
        return Err("pairing code was reusable".into());
    }
    let (uuid, token) = registry.add_client("self-test".to_owned(), now);
    let client = registry
        .clients
        .first()
        .ok_or("self-test client was not created")?;
    if client.uuid != uuid
        || client.token_sha256 != token_hash(&token)
        || client.token_sha256 == token
    {
        return Err("client credential validation failed".into());
    }
    if !registry.revoke(uuid) || !registry.clients.is_empty() {
        return Err("client revocation failed".into());
    }
    println!("omp-relayd self-test passed");
    Ok(())
}

const SERVICE_UNIT: &str = "omp-relayd.service";

fn service(registry: &Path, action: ServiceAction) -> Result<(), Box<dyn std::error::Error>> {
    if !cfg!(target_os = "linux") {
        return Err("service management is supported only on Linux with systemd".into());
    }
    let unit_path = service_unit_path()?;
    match action {
        ServiceAction::Install { bind, no_enable } => {
            let exe = std::env::current_exe()?;
            let registry = absolute_path(registry)?;
            prepare_registry_directory(&registry)?;
            let unit = render_service_unit(&exe, &registry, bind)?;
            if let Some(parent) = unit_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&unit_path, unit)?;
            println!("Wrote {}", unit_path.display());
            systemctl(["daemon-reload"])?;
            if no_enable {
                println!("Service unit written; not enabled (--no-enable).");
            } else {
                systemctl(["enable", "--now", SERVICE_UNIT])?;
                println!("Enabled and started {SERVICE_UNIT}.");
            }
        }
        ServiceAction::Uninstall => {
            let _ = systemctl(["disable", "--now", SERVICE_UNIT]);
            if unit_path.exists() {
                std::fs::remove_file(&unit_path)?;
                println!("Removed {}", unit_path.display());
            } else {
                println!("No unit file at {}", unit_path.display());
            }
            systemctl(["daemon-reload"])?;
        }
        ServiceAction::Status => {
            systemctl(["status", "--no-pager", SERVICE_UNIT])?;
        }
    }
    Ok(())
}

fn service_unit_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config = dirs::config_dir().ok_or("could not resolve user config directory")?;
    Ok(config.join("systemd").join("user").join(SERVICE_UNIT))
}

fn absolute_path(path: &Path) -> std::io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn prepare_registry_directory(registry: &Path) -> std::io::Result<()> {
    let Some(parent) = registry.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn render_service_unit(
    exe: &Path,
    registry: &Path,
    bind: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let exe = systemd_quote_path(exe)?;
    let registry_arg = systemd_quote_path(registry)?;
    let registry_directory =
        systemd_quote_path(registry.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(format!(
        "[Unit]\n\
Description=OMP authenticated AI provider relay\n\
After=network-online.target\n\
Wants=network-online.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart={exe} --registry {registry_arg} serve --bind {bind}\n\
Restart=on-failure\n\
RestartSec=2\n\
UMask=0077\n\
NoNewPrivileges=true\n\
PrivateTmp=true\n\
ProtectSystem=strict\n\
ProtectHome=read-only\n\
ReadWritePaths={registry_directory}\n\
\n\
[Install]\n\
WantedBy=default.target\n"
    ))
}

fn systemd_quote_path(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let value = path
        .to_str()
        .ok_or("systemd service paths must be valid UTF-8")?;
    if value.chars().any(char::is_control) {
        return Err("systemd service paths cannot contain control characters".into());
    }
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
        .replace('$', "$$");
    Ok(format!("\"{escaped}\""))
}

fn systemctl<const N: usize>(args: [&str; N]) -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .map_err(|error| format!("failed to run systemctl: {error}"))?;
    if !status.success() {
        return Err(format!("systemctl {} failed", args.join(" ")).into());
    }
    Ok(())
}

async fn serve(path: PathBuf, bind: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    {
        let _lock = registry_read_lock(&path)?;
        Registry::load(&path)?;
    }
    let state = Arc::new(AppState {
        registry_path: path,
        started: Instant::now(),
        debug: AtomicBool::new(false),
        active_streams: AtomicUsize::new(0),
        pair_attempts: Mutex::new(HashMap::new()),
        pinned_clients: Mutex::new(HashMap::new()),
    });
    let app = Router::new()
        .route("/pair", post(pair))
        .route("/status", get(status))
        .route("/debug", post(debug))
        .fallback(any(forward::forward_proxy))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, registry = %state.registry_path.display(), "omp-relayd listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

async fn pair(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(request): Json<PairRequest>,
) -> Response {
    if !allow_pair_attempt(&state, peer.ip()).await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "rate_limited" })),
        )
            .into_response();
    }
    let name = request.name.trim();
    if name.is_empty() || name.len() > 128 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_name" })),
        )
            .into_response();
    }
    let now = unix_time();
    let lock = match registry_lock(&state.registry_path) {
        Ok(lock) => lock,
        Err(error) => return internal_error(error),
    };
    let mut registry = match Registry::load(&state.registry_path) {
        Ok(registry) => registry,
        Err(error) => return internal_error(error),
    };
    if !registry.consume_pairing_code(&request.code, now) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "invalid_or_expired_code" })),
        )
            .into_response();
    }
    let (uuid, token) = registry.add_client(name.to_owned(), now);
    if let Err(error) = registry.save(&state.registry_path) {
        return internal_error(error);
    }
    drop(lock);
    (StatusCode::OK, Json(PairResponse { uuid, token })).into_response()
}

async fn status(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Err(response) = authenticate(&state, request.headers()).await {
        return response;
    }
    let registry_path = state.registry_path.clone();
    let clients = match tokio::task::spawn_blocking(move || {
        let _lock = registry_read_lock(&registry_path)?;
        Registry::load(&registry_path).map(|registry| registry.clients.len())
    })
    .await
    {
        Ok(Ok(clients)) => clients,
        Ok(Err(error)) => return internal_error(error),
        Err(error) => return internal_error(format!("registry status task failed: {error}")),
    };
    Json(json!({
        "version": VERSION,
        "uptime_s": state.started.elapsed().as_secs(),
        "clients": clients,
        "active_streams": state.active_streams.load(Ordering::Relaxed),
        "debug": state.debug.load(Ordering::Relaxed),
    }))
    .into_response()
}

async fn debug(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<DebugRequest>,
) -> Response {
    if let Err(response) = authenticate(&state, &headers).await {
        return response;
    }
    state.debug.store(request.enabled, Ordering::Relaxed);
    Json(json!({ "debug": request.enabled })).into_response()
}

#[allow(clippy::result_large_err)]
pub async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthenticatedClient, Response> {
    let Some(value) = headers
        .get("x-omp-relay-token")
        .and_then(|value| value.to_str().ok())
    else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response());
    };
    authenticate_token(state, value).await
}

#[allow(clippy::result_large_err)]
pub async fn authenticate_token(
    state: &AppState,
    value: &str,
) -> Result<AuthenticatedClient, Response> {
    let Some((uuid_text, token)) = value.split_once(':') else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response());
    };
    let Ok(uuid) = Uuid::parse_str(uuid_text) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response());
    };
    let presented_hash = token_hash(token);
    let registry_path = state.registry_path.clone();
    let now = unix_time();
    let (name, _disk_registry) = tokio::task::spawn_blocking(move || {
        authenticate_disk(&registry_path, uuid, &presented_hash, now)
    })
    .await
    .map_err(|error| internal_error(format!("registry auth task failed: {error}")))?
    .map_err(internal_error)?;
    let Some(name) = name else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response());
    };
    Ok(AuthenticatedClient { uuid, name })
}
fn authenticate_disk(
    path: &std::path::Path,
    uuid: Uuid,
    presented_hash: &str,
    now: u64,
) -> std::io::Result<(Option<String>, Registry)> {
    let read_lock = registry_read_lock(path)?;
    let registry = Registry::load(path)?;
    let Some(client) = registry.clients.iter().find(|client| {
        client.uuid == uuid
            && constant_time_equal(client.token_sha256.as_bytes(), presented_hash.as_bytes())
    }) else {
        return Ok((None, registry));
    };
    let name = client.name.clone();
    let should_flush = now.saturating_sub(client.last_seen) >= LAST_SEEN_FLUSH_SECS;
    drop(read_lock);
    if !should_flush {
        return Ok((Some(name), registry));
    }

    let _lock = registry_lock(path)?;
    let mut registry = Registry::load(path)?;
    let Some(client) = registry.clients.iter_mut().find(|client| {
        client.uuid == uuid
            && constant_time_equal(client.token_sha256.as_bytes(), presented_hash.as_bytes())
    }) else {
        return Ok((None, registry));
    };
    let name = client.name.clone();
    if now.saturating_sub(client.last_seen) >= LAST_SEEN_FLUSH_SECS {
        client.last_seen = now;
        registry.save(path)?;
    }
    Ok((Some(name), registry))
}

async fn allow_pair_attempt(state: &AppState, ip: IpAddr) -> bool {
    let minute = unix_time() / 60;
    let mut attempts = state.pair_attempts.lock().await;
    let window = attempts
        .entry(ip)
        .or_insert(PairRateWindow { minute, count: 0 });
    if window.minute != minute {
        window.minute = minute;
        window.count = 0;
    }
    if window.count >= 10 {
        return false;
    }
    window.count += 1;
    true
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "registry operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "internal" })),
    )
        .into_response()
}

async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_registry() -> PathBuf {
        std::env::temp_dir()
            .join(format!("omp-relayd-main-test-{}", Uuid::new_v4()))
            .join("registry.json")
    }

    #[test]
    fn authentication_persists_last_seen_without_reviving_revoked_clients() {
        let path = temporary_registry();
        let mut registry = Registry::default();
        let created = 1_000;
        let (uuid, token) = registry.add_client("test client".into(), created);
        registry.save(&path).expect("registry saves");

        let seen = created + LAST_SEEN_FLUSH_SECS;
        let (name, _) = authenticate_disk(&path, uuid, &token_hash(&token), seen)
            .expect("authentication succeeds");
        assert_eq!(name.as_deref(), Some("test client"));
        let persisted = Registry::load(&path).expect("registry reloads");
        assert_eq!(persisted.clients[0].last_seen, seen);

        {
            let _lock = registry_lock(&path).expect("registry locks");
            let mut registry = Registry::load(&path).expect("registry reloads for revocation");
            assert!(registry.revoke(uuid));
            registry.save(&path).expect("revocation saves");
        }
        let (name, registry) = authenticate_disk(&path, uuid, &token_hash(&token), seen + 1)
            .expect("revoked authentication is handled");
        assert!(name.is_none());
        assert!(registry.clients.is_empty());
        assert!(
            Registry::load(&path)
                .expect("registry reloads")
                .clients
                .is_empty()
        );

        std::fs::remove_dir_all(path.parent().expect("registry parent exists"))
            .expect("temporary registry is removed");
    }
    #[test]
    fn concurrent_authentication_never_revives_revoked_clients() {
        let path = temporary_registry();
        let mut registry = Registry::default();
        let created = 1_000;
        let (uuid, token) = registry.add_client("concurrent client".into(), created);
        registry.save(&path).expect("registry saves");
        let token_hash = token_hash(&token);
        let barrier = Arc::new(std::sync::Barrier::new(9));

        let mut workers = Vec::new();
        for index in 0..8 {
            let path = path.clone();
            let token_hash = token_hash.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                authenticate_disk(
                    &path,
                    uuid,
                    &token_hash,
                    created + LAST_SEEN_FLUSH_SECS + index,
                )
                .expect("concurrent authentication completes");
            }));
        }

        barrier.wait();
        {
            let _lock = registry_lock(&path).expect("registry locks for revocation");
            let mut registry = Registry::load(&path).expect("registry reloads for revocation");
            assert!(registry.revoke(uuid));
            registry.save(&path).expect("revocation saves");
        }
        for worker in workers {
            worker.join().expect("authentication worker joins");
        }

        let (name, registry) =
            authenticate_disk(&path, uuid, &token_hash, created + LAST_SEEN_FLUSH_SECS * 2)
                .expect("post-revocation authentication is handled");
        assert!(name.is_none());
        assert!(registry.clients.is_empty());

        std::fs::remove_dir_all(path.parent().expect("registry parent exists"))
            .expect("temporary registry is removed");
    }

    #[test]
    fn service_unit_quotes_paths_and_applies_sandboxing() {
        let unit = render_service_unit(
            Path::new("/opt/OMP Relay/omp-relayd"),
            Path::new("/home/alex/%relay/registry.json"),
            "10.90.0.2:43118".parse().expect("bind address parses"),
        )
        .expect("unit renders");
        assert!(unit.contains("ExecStart=\"/opt/OMP Relay/omp-relayd\""));
        assert!(unit.contains("--registry \"/home/alex/%%relay/registry.json\""));
        assert!(unit.contains("ReadWritePaths=\"/home/alex/%%relay\""));
        assert!(unit.contains("ProtectSystem=strict"));
        assert!(unit.contains("ProtectHome=read-only"));
        assert!(unit.contains("UMask=0077"));
    }

    #[test]
    fn service_actions_are_direct_subcommands_and_install_requires_bind() {
        assert!(Cli::try_parse_from(["omp-relayd", "service", "install"]).is_err());
        let install = Cli::try_parse_from([
            "omp-relayd",
            "service",
            "install",
            "--bind",
            "10.90.0.2:43118",
        ])
        .expect("service install with explicit bind parses");
        assert!(matches!(
            install.command,
            Command::Service {
                action: ServiceAction::Install { .. }
            }
        ));
        let status = Cli::try_parse_from(["omp-relayd", "service", "status"])
            .expect("service status parses");
        assert!(matches!(
            status.command,
            Command::Service {
                action: ServiceAction::Status
            }
        ));
    }
}
