use std::{net::SocketAddr, sync::Arc, time::Instant};

use axum::{
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::TryStreamExt;
use hyper_util::rt::TokioIo;
use tokio::io::copy_bidirectional;

use crate::{AppState, AuthenticatedClient, authenticate_token};

pub async fn forward_proxy(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let authenticated = match authenticate_proxy(&state, request.headers()).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    if request.method() == Method::CONNECT {
        return connect_tunnel(state, peer, authenticated, request).await;
    }
    forward_http(state, peer, authenticated, request).await
}

#[allow(clippy::result_large_err)]
async fn authenticate_proxy(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthenticatedClient, Response> {
    let Some(value) = headers
        .get(header::PROXY_AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(proxy_auth_required());
    };
    let Some(encoded) = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("basic "))
    else {
        return Err(proxy_auth_required());
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return Err(proxy_auth_required());
    };
    let Ok(credentials) = std::str::from_utf8(&decoded) else {
        return Err(proxy_auth_required());
    };
    authenticate_token(state, credentials)
        .await
        .map_err(|_| proxy_auth_required())
}

fn proxy_auth_required() -> Response {
    let mut response = (
        StatusCode::PROXY_AUTHENTICATION_REQUIRED,
        "proxy authentication required",
    )
        .into_response();
    response.headers_mut().insert(
        header::PROXY_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"omp-relayd\""),
    );
    response
}

async fn connect_tunnel(
    state: Arc<AppState>,
    peer: SocketAddr,
    authenticated: AuthenticatedClient,
    mut request: Request,
) -> Response {
    let Some(authority) = request.uri().authority().map(ToString::to_string) else {
        return (StatusCode::BAD_REQUEST, "CONNECT target missing").into_response();
    };
    let started = Instant::now();
    let state_for_upgrade = state.clone();
    let authority_for_upgrade = authority.clone();
    tokio::spawn(async move {
        let _guard = state_for_upgrade.stream_guard();
        match hyper::upgrade::on(&mut request).await {
            Ok(client) => match tokio::net::TcpStream::connect(&authority_for_upgrade).await {
                Ok(mut upstream) => {
                    let mut client = TokioIo::new(client);
                    if let Err(error) = copy_bidirectional(&mut client, &mut upstream).await {
                        tracing::debug!(%error, target = %authority_for_upgrade, "CONNECT tunnel closed with I/O error");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, target = %authority_for_upgrade, "CONNECT upstream failed")
                }
            },
            Err(error) => {
                tracing::warn!(%error, target = %authority_for_upgrade, "CONNECT upgrade failed")
            }
        }
    });
    if state.debug.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::info!(client = %authenticated.uuid, client_name = %authenticated.name, peer = %peer.ip(), target = %authority, duration_ms = started.elapsed().as_millis(), "opened CONNECT tunnel");
    }
    StatusCode::OK.into_response()
}

async fn forward_http(
    state: Arc<AppState>,
    peer: SocketAddr,
    authenticated: AuthenticatedClient,
    request: Request,
) -> Response {
    let started = Instant::now();
    let uri = request.uri().clone();
    if uri.scheme().is_none() || uri.authority().is_none() {
        return (StatusCode::BAD_REQUEST, "absolute proxy URI required").into_response();
    }
    let method = request.method().clone();
    let (parts, body) = request.into_parts();
    let mut builder = state.http_client.request(method.clone(), uri.to_string());
    for (name, value) in parts.headers {
        let Some(name) = name else { continue };
        if is_proxy_or_hop_header(&name) {
            continue;
        }
        builder = builder.header(name, value);
    }
    let body = TryStreamExt::map_err(body.into_data_stream(), std::io::Error::other);
    let upstream = match builder.body(reqwest::Body::wrap_stream(body)).send().await {
        Ok(response) => response,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("upstream connect failed: {error}"),
            )
                .into_response();
        }
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let stream = upstream.bytes_stream();
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    for (name, value) in headers {
        let Some(name) = name else { continue };
        if is_proxy_or_hop_header(&name) {
            continue;
        }
        response.headers_mut().append(name, value);
    }
    if state.debug.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::info!(client = %authenticated.uuid, client_name = %authenticated.name, peer = %peer.ip(), method = %method, target = %uri, status = %status, duration_ms = started.elapsed().as_millis(), "proxied absolute HTTP request");
    }
    response
}

fn is_proxy_or_hop_header(name: &axum::http::HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "proxy-authorization"
            | "proxy-authenticate"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[allow(dead_code)]
fn _assert_uri_send(_: Uri) {}
