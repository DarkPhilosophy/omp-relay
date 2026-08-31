# Changelog

All notable changes to omp-relay are documented here.

## 0.1.1 - 2026-09-01

### Added

- `omp-relayd service install`, `status`, and `uninstall` commands for managing a systemd user service on Linux.
- SSRF protection that only allows globally-routable IPv4 and IPv6 destinations, rejecting private, loopback, link-local, multicast, reserved, documentation, and metadata endpoints.
- Regression coverage for registry locking, durable `last_seen` updates, revocation under concurrency, reserved-range rejection, DNS-aware client reuse, and bounded proxy client caching.

### Changed

- Pin validated DNS addresses for forwarded requests to prevent DNS rebinding between validation and connection.
- Reuse pinned HTTP clients for stable upstream connections while refreshing them when DNS answers change.
- Move registry file access away from asynchronous worker threads and serialize registry updates with file locks.
- Persist registry updates through atomic replacement and keep registry and lock files private to the current user on Unix.
- Remove the obsolete host-in-path relay routes and WebSocket bridge, closing the legacy SSRF bypass and keeping one hardened forward-proxy transport.
- Require an explicit `--bind` address for `service install` instead of defaulting to a public all-interfaces bind.
- Authenticate on a shared registry read lock and escalate to an exclusive lock only when flushing `last_seen`, removing the per-request exclusive lock.
- Bound outbound CONNECT tunnel dials with a timeout so unreachable upstreams fail fast instead of hanging.
- Redact request paths, queries, and fragments from debug logs; diagnostics now record only the upstream scheme, host, and port.

### Fixed

- Revoked clients now lose access without being restored by a stale in-memory registry snapshot.
- `last_seen` values are written durably with bounded write frequency.
- `service install` only tightens permissions on a registry parent directory it creates; an existing directory keeps its mode.
- Unix shutdown handling now responds correctly to both SIGINT and SIGTERM.

## 0.1.0 - 2026-08-31

- Initial release of the OMP extension and authenticated relay server.
- Standard HTTP forward proxy support for OMP provider traffic, including HTTPS CONNECT and streaming responses.
- One-time pairing, client revocation, live relay status, debug controls, and deterministic self-test mode.
