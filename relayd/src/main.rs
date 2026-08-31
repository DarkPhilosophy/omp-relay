mod forward;
mod proxy;
mod registry;
mod ws;

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
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
use tokio::sync::{Mutex, RwLock};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use registry::{Registry, registry_lock, registry_path, registry_read_lock, token_hash, unix_time};

const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    /// Run deterministic local checks without starting a listener or changing the real registry.
    SelfTest,
}

#[derive(Clone)]
pub struct AuthenticatedClient {
    pub uuid: Uuid,
    pub name: String,
}

pub struct AppState {
    registry_path: PathBuf,
    registry: RwLock<Registry>,
    started: Instant,
    debug: AtomicBool,
    active_streams: AtomicUsize,
    pair_attempts: Mutex<HashMap<IpAddr, PairRateWindow>>,
    http_client: reqwest::Client,
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

async fn serve(path: PathBuf, bind: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let registry = {
        let _lock = registry_read_lock(&path)?;
        Registry::load(&path)?
    };
    let state = Arc::new(AppState {
        registry_path: path,
        registry: RwLock::new(registry),
        started: Instant::now(),
        debug: AtomicBool::new(false),
        active_streams: AtomicUsize::new(0),
        pair_attempts: Mutex::new(HashMap::new()),
        http_client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()?,
    });
    let app = Router::new()
        .route("/pair", post(pair))
        .route("/status", get(status))
        .route("/debug", post(debug))
        .route("/t/{uuid}/{host}/{*rest}", any(proxy::proxy))
        .route("/t/{uuid}/{host}", any(proxy::proxy_root))
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
    *state.registry.write().await = registry;
    (StatusCode::OK, Json(PairResponse { uuid, token })).into_response()
}

async fn status(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Err(response) = authenticate(&state, request.headers()).await {
        return response;
    }
    let clients = state.registry.read().await.clients.len();
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
    let disk_registry = tokio::task::spawn_blocking(move || {
        let _lock = registry_read_lock(&registry_path)?;
        Registry::load(&registry_path)
    })
    .await
    .map_err(|error| internal_error(format!("registry read task failed: {error}")))?
    .map_err(internal_error)?;
    let authenticated = disk_registry
        .clients
        .iter()
        .find(|client| {
            client.uuid == uuid
                && constant_time_equal(client.token_sha256.as_bytes(), presented_hash.as_bytes())
        })
        .map(|client| AuthenticatedClient {
            uuid,
            name: client.name.clone(),
        });
    *state.registry.write().await = disk_registry;
    let Some(authenticated) = authenticated else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response());
    };
    touch_last_seen(state, uuid).await;
    Ok(authenticated)
}

async fn touch_last_seen(state: &AppState, uuid: Uuid) {
    let now = unix_time();
    if let Some(client) = state
        .registry
        .write()
        .await
        .clients
        .iter_mut()
        .find(|client| client.uuid == uuid)
    {
        client.last_seen = now;
    }
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
