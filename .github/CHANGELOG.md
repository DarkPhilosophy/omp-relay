# Changelog

All notable changes to omp-relay are documented here.

## 0.1.1 - 2026-09-01

### Added

- `omp-relayd service install`, `status`, and `uninstall` commands for managing a systemd user service on Linux.
- SSRF protection for HTTP and HTTPS proxy destinations, including private, loopback, link-local, reserved, and metadata endpoints.
- Regression coverage for registry locking, durable `last_seen` updates, revocation, DNS-aware client reuse, and bounded proxy client caching.

### Changed

- Pin validated DNS addresses for forwarded requests to prevent DNS rebinding between validation and connection.
- Reuse pinned HTTP clients for stable upstream connections while refreshing them when DNS answers change.
- Move registry file access away from asynchronous worker threads and serialize registry updates with file locks.
- Persist registry updates through atomic replacement and keep registry and lock files private to the current user on Unix.
- Remove the obsolete host-in-path relay routes and WebSocket bridge, closing the legacy SSRF bypass and keeping one hardened forward-proxy transport.

### Fixed

- Revoked clients now lose access without being restored by a stale in-memory registry snapshot.
- `last_seen` values are written durably with bounded write frequency.
- Unix shutdown handling now responds correctly to both SIGINT and SIGTERM.

## 0.1.0 - 2026-08-31

- Initial release of the OMP extension and authenticated relay server.
- Standard HTTP forward proxy support for OMP provider traffic, including HTTPS CONNECT and streaming responses.
- One-time pairing, client revocation, live relay status, debug controls, and deterministic self-test mode.
