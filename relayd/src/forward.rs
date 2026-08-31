use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

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

use crate::{
    AppState, AuthenticatedClient, MAX_PINNED_CLIENTS, PinnedHttpClient, authenticate_token,
};

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

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

async fn connect_tunnel(
    state: Arc<AppState>,
    peer: SocketAddr,
    authenticated: AuthenticatedClient,
    mut request: Request,
) -> Response {
    let Some(authority) = request.uri().authority().map(ToString::to_string) else {
        return (StatusCode::BAD_REQUEST, "CONNECT target missing").into_response();
    };
    let parsed = match authority.parse::<http::uri::Authority>() {
        Ok(value) => value,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid CONNECT target").into_response(),
    };
    let Some(port) = parsed.port_u16() else {
        return (StatusCode::BAD_REQUEST, "CONNECT target port missing").into_response();
    };
    let addresses = match resolve_public_target(parsed.host(), port).await {
        Ok(addresses) => addresses,
        Err(error) => return target_error_response(error),
    };
    let started = Instant::now();
    let upstream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_any(&addresses)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            tracing::warn!(%error, target = %authority, "CONNECT upstream failed");
            return (StatusCode::BAD_GATEWAY, "CONNECT upstream failed").into_response();
        }
        Err(_) => {
            tracing::warn!(target = %authority, "CONNECT upstream timed out");
            return (StatusCode::GATEWAY_TIMEOUT, "CONNECT upstream timed out").into_response();
        }
    };
    let state_for_upgrade = state.clone();
    let authority_for_upgrade = authority.clone();
    tokio::spawn(async move {
        let _guard = state_for_upgrade.stream_guard();
        match hyper::upgrade::on(&mut request).await {
            Ok(client) => {
                let mut client = TokioIo::new(client);
                let mut upstream = upstream;
                if let Err(error) = copy_bidirectional(&mut client, &mut upstream).await {
                    tracing::debug!(%error, target = %authority_for_upgrade, "CONNECT tunnel closed with I/O error");
                }
            }
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

struct DebugTarget<'a> {
    scheme: &'a str,
    host: &'a str,
    port: u16,
}

impl DebugTarget<'_> {
    fn from_uri(uri: &Uri) -> Option<DebugTarget<'_>> {
        let scheme = uri.scheme_str()?;
        let host = uri.host()?;
        let port = uri.port_u16().unwrap_or_else(|| {
            if scheme.eq_ignore_ascii_case("https") {
                443
            } else {
                80
            }
        });
        Some(DebugTarget { scheme, host, port })
    }
}

impl std::fmt::Display for DebugTarget<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "{}://[{}]:{}", self.scheme, self.host, self.port)
        } else {
            write!(formatter, "{}://{}:{}", self.scheme, self.host, self.port)
        }
    }
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
    let Some(host) = uri.host() else {
        return (StatusCode::BAD_REQUEST, "proxy target host missing").into_response();
    };
    let port = uri.port_u16().unwrap_or_else(|| {
        if uri
            .scheme_str()
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https"))
        {
            443
        } else {
            80
        }
    });
    let addresses = match resolve_public_target(host, port).await {
        Ok(addresses) => addresses,
        Err(error) => return target_error_response(error),
    };
    let client = match cached_pinned_client(&state, host, port, &addresses).await {
        Ok(client) => client,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to construct pinned HTTP client: {error}"),
            )
                .into_response();
        }
    };
    let method = request.method().clone();
    let (parts, body) = request.into_parts();
    let mut builder = client.request(method.clone(), uri.to_string());
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
        let target = DebugTarget::from_uri(&uri).expect("validated absolute proxy URI");
        tracing::info!(client = %authenticated.uuid, client_name = %authenticated.name, peer = %peer.ip(), method = %method, target = %target, status = %status, duration_ms = started.elapsed().as_millis(), "proxied absolute HTTP request");
    }
    response
}

fn pinned_http_client(
    host: &str,
    addresses: &[SocketAddr],
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .resolve_to_addrs(host, addresses)
        .build()
}

async fn cached_pinned_client(
    state: &AppState,
    host: &str,
    port: u16,
    addresses: &[SocketAddr],
) -> Result<reqwest::Client, reqwest::Error> {
    let key = format!("{}:{port}", host.to_ascii_lowercase());
    let mut validated_addresses = addresses.to_vec();
    validated_addresses.sort_unstable();
    validated_addresses.dedup();

    let mut cache = state.pinned_clients.lock().await;
    if let Some(entry) = cache.get(&key)
        && entry.addresses == validated_addresses
    {
        return Ok(entry.client.clone());
    }

    let client = pinned_http_client(host, &validated_addresses)?;
    if !cache.contains_key(&key)
        && cache.len() >= MAX_PINNED_CLIENTS
        && let Some(oldest_key) = cache.keys().next().cloned()
    {
        cache.remove(&oldest_key);
    }
    cache.insert(
        key,
        PinnedHttpClient {
            client: client.clone(),
            addresses: validated_addresses,
        },
    );
    Ok(client)
}

#[derive(Debug)]
enum TargetError {
    Forbidden,
    Resolution(String),
    Empty,
}

async fn resolve_public_target(host: &str, port: u16) -> Result<Vec<SocketAddr>, TargetError> {
    let normalized = host.trim_matches(['[', ']']).to_ascii_lowercase();
    if normalized == "localhost"
        || normalized.ends_with(".localhost")
        || normalized == "metadata.google.internal"
    {
        return Err(TargetError::Forbidden);
    }
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| TargetError::Resolution(error.to_string()))?
        .collect();
    if addresses.is_empty() {
        return Err(TargetError::Empty);
    }
    if addresses
        .iter()
        .any(|address| is_disallowed_ip(address.ip()))
    {
        return Err(TargetError::Forbidden);
    }
    Ok(addresses)
}

async fn connect_any(addresses: &[SocketAddr]) -> std::io::Result<tokio::net::TcpStream> {
    let mut last_error = None;
    for address in addresses {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| std::io::Error::other("target resolved to no addresses")))
}

fn target_error_response(error: TargetError) -> Response {
    match error {
        TargetError::Forbidden => (
            StatusCode::FORBIDDEN,
            "proxy target is not allowed: loopback, private, link-local, metadata, and other non-public destinations are blocked",
        )
            .into_response(),
        TargetError::Resolution(error) => (
            StatusCode::BAD_GATEWAY,
            format!("target resolution failed: {error}"),
        )
            .into_response(),
        TargetError::Empty => {
            (StatusCode::BAD_GATEWAY, "target resolved to no addresses").into_response()
        }
    }
}

fn is_disallowed_ip(ip: IpAddr) -> bool {
    !match ip {
        IpAddr::V4(ip) => is_globally_routable_ipv4(ip),
        IpAddr::V6(ip) => is_globally_routable_ipv6(ip),
    }
}

fn is_globally_routable_ipv4(ip: std::net::Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !matches!(
        (a, b, c),
        (0, _, _)
            | (10, _, _)
            | (127, _, _)
            | (100, 64..=127, _)
            | (169, 254, _)
            | (172, 16..=31, _)
            | (192, 168, _)
            | (192, 0, 0)
            | (192, 0, 2)
            | (192, 88, 99)
            | (198, 18..=19, _)
            | (198, 51, 100)
            | (203, 0, 113)
            | (224..=255, _, _)
    )
}

fn is_globally_routable_ipv6(ip: std::net::Ipv6Addr) -> bool {
    // IPv4-mapped destinations (::ffff:a.b.c.d) follow the IPv4 policy.
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_globally_routable_ipv4(mapped);
    }
    // IANA allocates global IPv6 unicast from 2000::/3. Everything outside it,
    // plus special-purpose blocks inside it, must stay unreachable.
    if ip.segments()[0] & 0xe000 != 0x2000
        || ip.is_multicast()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
    {
        return false;
    }
    let s = ip.segments();
    // Teredo (2001::/32), benchmarking (2001:2::/48), ORCHIDv2 (2001:20::/28),
    // documentation (2001:db8::/32, 3fff::/20), and deprecated 6to4 (2002::/16).
    !((s[0] == 0x2001 && s[1] == 0x0000)
        || (s[0] == 0x2001 && s[1] == 0x0002 && s[2] == 0x0000)
        || (s[0] == 0x2001 && (0x0020..=0x002f).contains(&s[1]))
        || (s[0] == 0x2001 && s[1] == 0x0db8)
        || s[0] == 0x2002
        || (s[0] == 0x3fff && s[1] & 0xf000 == 0x0000))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn rejects_non_public_ipv4_ranges() {
        for ip in [
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(172, 16, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            Ipv4Addr::new(169, 254, 169, 254),
            Ipv4Addr::new(100, 64, 0, 1),
        ] {
            assert!(is_disallowed_ip(IpAddr::V4(ip)), "{ip} should be blocked");
        }
        assert!(!is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn rejects_reserved_and_documentation_ipv4_ranges() {
        for ip in [
            Ipv4Addr::new(192, 0, 2, 1),
            Ipv4Addr::new(198, 51, 100, 1),
            Ipv4Addr::new(203, 0, 113, 1),
            Ipv4Addr::new(198, 18, 0, 1),
            Ipv4Addr::new(192, 88, 99, 1),
            Ipv4Addr::new(240, 0, 0, 1),
            Ipv4Addr::new(255, 255, 255, 255),
        ] {
            assert!(is_disallowed_ip(IpAddr::V4(ip)), "{ip} should be blocked");
        }
        assert!(!is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
    }

    #[test]
    fn rejects_special_purpose_ipv6_ranges() {
        for ip in [
            "2001:db8::1",
            "2002:c000:0204::1",
            "2001::1",
            "2001:2::1",
            "2001:20::1",
            "3fff::1",
            "0100::1",
        ] {
            let parsed: Ipv6Addr = ip.parse().expect("IPv6 parses");
            assert!(
                is_disallowed_ip(IpAddr::V6(parsed)),
                "{ip} should be blocked"
            );
        }
        assert!(!is_disallowed_ip(IpAddr::V6(
            "2001:4860:4860::8888".parse().expect("public IPv6 parses")
        )));
    }

    #[test]
    fn rejects_non_public_ipv6_ranges() {
        assert!(is_disallowed_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_disallowed_ip(IpAddr::V6(
            "fd00::1".parse().expect("ULA parses")
        )));
        assert!(is_disallowed_ip(IpAddr::V6(
            "fe80::1".parse().expect("link-local parses")
        )));
        assert!(!is_disallowed_ip(IpAddr::V6(
            "2606:4700:4700::1111".parse().expect("public IPv6 parses")
        )));
    }

    #[test]
    fn rejects_ipv4_mapped_ipv6_private_ranges() {
        for ip in [
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:172.16.0.1",
            "::ffff:192.168.1.1",
            "::ffff:169.254.169.254",
        ] {
            let ip: Ipv6Addr = ip.parse().expect("mapped IPv6 parses");
            assert!(is_disallowed_ip(IpAddr::V6(ip)), "{ip} should be blocked");
        }
        let public: Ipv6Addr = "::ffff:8.8.8.8".parse().expect("mapped IPv6 parses");
        assert!(!is_disallowed_ip(IpAddr::V6(public)));
    }

    #[test]
    fn debug_target_excludes_path_query_and_fragment_secrets() {
        let uri: Uri = "https://api.example.com:8443/v1/chat?api_key=secret-token#private"
            .parse()
            .expect("absolute URI parses");
        let rendered = DebugTarget::from_uri(&uri)
            .expect("debug target parses")
            .to_string();

        assert_eq!(rendered, "https://api.example.com:8443");
        assert!(!rendered.contains("/v1/chat"));
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("private"));
    }

    #[test]
    fn pinned_client_accepts_validated_addresses_for_http_and_https_hosts() {
        let addresses = [SocketAddr::from(([93, 184, 216, 34], 443))];
        assert!(pinned_http_client("example.com", &addresses).is_ok());
        assert!(pinned_http_client("api.example.com", &addresses).is_ok());
    }

    fn test_state() -> AppState {
        AppState {
            registry_path: std::path::PathBuf::new(),
            started: Instant::now(),
            debug: std::sync::atomic::AtomicBool::new(false),
            active_streams: std::sync::atomic::AtomicUsize::new(0),
            pair_attempts: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            pinned_clients: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    #[tokio::test]
    async fn pinned_cache_is_bounded_and_refreshes_changed_dns_answers() {
        let state = test_state();
        let first = [SocketAddr::from(([93, 184, 216, 34], 443))];
        cached_pinned_client(&state, "example.com", 443, &first)
            .await
            .expect("first client builds");
        cached_pinned_client(&state, "example.com", 443, &first)
            .await
            .expect("cached client is reused");
        assert_eq!(state.pinned_clients.lock().await.len(), 1);

        let changed = [SocketAddr::from(([93, 184, 216, 35], 443))];
        cached_pinned_client(&state, "example.com", 443, &changed)
            .await
            .expect("changed DNS answer refreshes the client");
        assert_eq!(
            state
                .pinned_clients
                .lock()
                .await
                .get("example.com:443")
                .expect("cache entry exists")
                .addresses,
            changed
        );

        for index in 0..=MAX_PINNED_CLIENTS {
            let host = format!("api-{index}.example.com");
            cached_pinned_client(&state, &host, 443, &first)
                .await
                .expect("client builds");
        }
        assert_eq!(state.pinned_clients.lock().await.len(), MAX_PINNED_CLIENTS);
    }
}
