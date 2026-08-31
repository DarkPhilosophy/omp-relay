use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use axum::{
    Json,
    body::Body,
    extract::{
        ConnectInfo, FromRequestParts, Path, Request, State, WebSocketUpgrade, ws::WebSocket,
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::TryStreamExt;
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

use crate::{AppState, AuthenticatedClient, authenticate, ws};
pub async fn proxy_root(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((uuid_in_path, host)): Path<(String, String)>,
    request: Request,
) -> Response {
    proxy_request(state, peer, uuid_in_path, host, String::new(), request).await
}

pub async fn proxy(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((uuid_in_path, host, rest)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    proxy_request(state, peer, uuid_in_path, host, rest, request).await
}

async fn proxy_request(
    state: Arc<AppState>,
    peer: SocketAddr,
    uuid_in_path: String,
    host: String,
    rest: String,
    request: Request,
) -> Response {
    let authenticated = match authenticate(&state, request.headers()).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    if uuid_in_path != authenticated.uuid.to_string() {
        return json_error(
            StatusCode::FORBIDDEN,
            "uuid_mismatch",
            "path UUID does not match authenticated client",
        );
    }
    if !valid_host(&host) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_host",
            "host must be a DNS name or IP address with an optional port",
        );
    }

    let is_websocket = request
        .headers()
        .get(header::UPGRADE)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let scheme = upstream_scheme(request.headers(), is_websocket);
    let path_and_query = request
        .uri()
        .path_and_query()
        .and_then(|value| {
            value
                .as_str()
                .split_once(&format!("/{host}/"))
                .map(|(_, tail)| tail)
        })
        .unwrap_or(rest.as_str());
    let path_and_query = if path_and_query.is_empty() {
        "/".to_owned()
    } else {
        format!("/{path_and_query}")
    };
    let upstream_url = format!("{scheme}://{host}{path_and_query}");
    if is_websocket {
        return proxy_websocket_request(
            state,
            peer.ip(),
            authenticated,
            host,
            upstream_url,
            request,
        )
        .await;
    }
    proxy_http(state, peer.ip(), authenticated, host, upstream_url, request).await
}

async fn proxy_http(
    state: Arc<AppState>,
    peer: IpAddr,
    authenticated: AuthenticatedClient,
    host: String,
    upstream_url: String,
    request: Request,
) -> Response {
    let started = Instant::now();
    let _stream_guard = state.stream_guard();
    let (parts, body) = request.into_parts();
    let mut builder = state
        .http_client
        .request(parts.method.clone(), &upstream_url);
    for (name, value) in filtered_headers(&parts.headers, false) {
        builder = builder.header(name, value);
    }
    if let Ok(host_header) = HeaderValue::from_str(&host) {
        builder = builder.header(header::HOST, host_header);
    }
    let body_stream = TryStreamExt::map_err(body.into_data_stream(), std::io::Error::other);
    let upstream = match builder
        .body(reqwest::Body::wrap_stream(body_stream))
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            log_request(
                &state,
                peer,
                &authenticated,
                &parts.method,
                &host,
                None,
                0,
                started,
                &error.to_string(),
            );
            return json_error(
                StatusCode::BAD_GATEWAY,
                "upstream_connect",
                &error.to_string(),
            );
        }
    };

    let status = upstream.status();
    let headers = upstream.headers().clone();
    let stream = upstream.bytes_stream();
    let counted = CountingStream::new(stream, state.clone());
    let mut response = Response::new(Body::from_stream(counted));
    *response.status_mut() = status;
    for (name, value) in filtered_headers(&headers, false) {
        response.headers_mut().append(name, value);
    }
    if state.debug.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::info!(client = %authenticated.uuid, client_name = %authenticated.name, %peer, method = %parts.method, target = %host, status = status.as_u16(), duration_ms = started.elapsed().as_millis(), "proxied HTTP request; response bytes are logged when stream completes");
    }
    response
}

async fn proxy_websocket_request(
    state: Arc<AppState>,
    peer: IpAddr,
    authenticated: AuthenticatedClient,
    host: String,
    upstream_url: String,
    request: Request,
) -> Response {
    let (mut parts, _body) = request.into_parts();
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
        Ok(upgrade) => upgrade,
        Err(response) => return response.into_response(),
    };
    proxy_websocket(
        state,
        peer,
        authenticated,
        host,
        upstream_url,
        upgrade,
        &parts.headers,
    )
    .await
}

async fn proxy_websocket(
    state: Arc<AppState>,
    peer: IpAddr,
    authenticated: AuthenticatedClient,
    host: String,
    upstream_url: String,
    upgrade: WebSocketUpgrade,
    headers: &HeaderMap,
) -> Response {
    let started = Instant::now();
    let _stream_guard = state.stream_guard();
    let mut upstream_request = match upstream_url.clone().into_client_request() {
        Ok(request) => request,
        Err(error) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_upstream",
                &error.to_string(),
            );
        }
    };
    for (name, value) in filtered_headers(headers, true) {
        upstream_request.headers_mut().append(name, value);
    }
    if let Ok(host_header) = HeaderValue::from_str(&host) {
        upstream_request
            .headers_mut()
            .insert(header::HOST, host_header);
    }
    let (upstream, response) = match connect_async(upstream_request).await {
        Ok(value) => value,
        Err(error) => {
            log_request(
                &state,
                peer,
                &authenticated,
                &http::Method::GET,
                &host,
                None,
                0,
                started,
                &error.to_string(),
            );
            return json_error(
                StatusCode::BAD_GATEWAY,
                "upstream_connect",
                &error.to_string(),
            );
        }
    };
    let upstream_status = response.status();
    let state_for_upgrade = state.clone();
    let authenticated_for_upgrade = authenticated.clone();
    upgrade.on_upgrade(move |client: WebSocket| async move {
        ws::bridge(client, upstream).await;
        log_request(
            &state_for_upgrade,
            peer,
            &authenticated_for_upgrade,
            &http::Method::GET,
            &host,
            Some(upstream_status),
            0,
            started,
            "websocket_closed",
        );
    })
}

fn upstream_scheme(headers: &HeaderMap, websocket: bool) -> &'static str {
    let local_http = headers
        .get("x-omp-relay-scheme")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("http"));
    match (websocket, local_http) {
        (true, true) => "ws",
        (true, false) => "wss",
        (false, true) => "http",
        (false, false) => "https",
    }
}

fn filtered_headers(headers: &HeaderMap, websocket: bool) -> Vec<(HeaderName, HeaderValue)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !is_relay_header(name)
                && !is_hop_by_hop(name)
                && (!websocket || !is_websocket_handshake_header(name))
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn is_relay_header(name: &HeaderName) -> bool {
    name.as_str().eq_ignore_ascii_case("x-omp-relay-token")
        || name.as_str().eq_ignore_ascii_case("x-omp-relay-scheme")
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn is_websocket_handshake_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-accept"
            | "sec-websocket-extensions"
    )
}

fn valid_host(host: &str) -> bool {
    if host.is_empty()
        || host.contains(['/', '\\', '@', '#', '?'])
        || host.chars().any(char::is_whitespace)
    {
        return false;
    }
    let authority = match http::uri::Authority::from_str(host) {
        Ok(value) => value,
        Err(_) => return false,
    };
    if authority.host().is_empty() {
        return false;
    }
    authority.port_u16().is_some()
        || !host
            .rsplit_once(':')
            .is_some_and(|(_, suffix)| suffix.chars().all(|character| character.is_ascii_digit()))
}

fn json_error(status: StatusCode, error: &str, detail: &str) -> Response {
    (status, Json(json!({ "error": error, "detail": detail }))).into_response()
}

#[allow(clippy::too_many_arguments)]
fn log_request(
    state: &AppState,
    peer: IpAddr,
    authenticated: &AuthenticatedClient,
    method: &http::Method,
    host: &str,
    status: Option<StatusCode>,
    bytes: u64,
    started: Instant,
    outcome: &str,
) {
    if state.debug.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::info!(client = %authenticated.uuid, client_name = %authenticated.name, %peer, %method, target = %host, status = status.map(|value| value.as_u16()), bytes, duration_ms = started.elapsed().as_millis(), %outcome, "relay request");
    }
}

struct CountingStream<S> {
    inner: S,
    bytes: u64,
    state: Arc<AppState>,
}

impl<S> CountingStream<S> {
    fn new(inner: S, state: Arc<AppState>) -> Self {
        Self {
            inner,
            bytes: 0,
            state,
        }
    }
}

impl<S, E> futures_util::Stream for CountingStream<S>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
{
    type Item = Result<bytes::Bytes, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = std::pin::Pin::new(&mut self.inner).poll_next(context);
        if let std::task::Poll::Ready(Some(Ok(ref bytes))) = next {
            self.bytes += bytes.len() as u64;
        }
        next
    }
}

impl<S> Drop for CountingStream<S> {
    fn drop(&mut self) {
        if self.state.debug.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::info!(bytes = self.bytes, "HTTP response stream closed");
        }
    }
}
