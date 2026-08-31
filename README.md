# OMP Relay

Authenticated, streaming forward relay for [Oh My Pi](https://github.com/can1357/oh-my-pi).

OMP Relay routes provider traffic through a machine in another network without changing the provider destination selected by OMP. Its primary use case is reaching AI providers that restrict clients by GeoIP, region, ASN, or network policy. It also works as a simple private reroute when one network path is unreliable.

![OMP Relay status widget](.screenshot/Screenshot%20From%202026-08-31%2008-10-07.png)

## What it provides

- Standard authenticated HTTP forward proxy with HTTPS `CONNECT` tunneling.
- Exact per-request destinations: provider API calls, discovery, authentication, and streaming keep the URL chosen by OMP.
- Incremental HTTP/SSE streaming and WebSocket-compatible tunneling.
- One-time pairing codes and per-client revocation.
- Persistent OMP status widget with connected, degraded, unreachable, and offline states.
- Latency, active-stream count, and relay uptime in the OMP UI.
- Periodic health refresh so a failed relay is visible during the session.
- Legacy host-in-path relay routes retained for compatibility.

## Architecture

```text
OMP provider request
  -> PI_PROXY selected by the OMP extension
  -> authenticated HTTP proxy / CONNECT tunnel
  -> omp-relayd on the reroute host
  -> the exact upstream destination requested by OMP
```

The relay never maps a model to a hard-coded provider host. OMP remains the source of truth for the effective upstream URL.

## Requirements

- OMP 18 or newer.
- Bun 1.2 or newer for extension development.
- Rust stable for building `omp-relayd`.
- Private connectivity between the client and relay host, such as WireGuard, NetBird, Tailscale, or a protected LAN.
- A terminal font with Nerd Font glyphs for the full status presentation. Text labels remain readable without the icons.

## Install the extension

Install dependencies and link the repository as an OMP extension during development:

```bash
bun install
ln -s "$(pwd)" ~/.omp/agent/extensions/relay
```

The package manifest exposes `src/index.ts` through `omp.extensions`, so the repository root is the extension root.

## Build the relay server

```bash
cargo build --manifest-path relayd/Cargo.toml --release
```

The binary is written to `relayd/target/release/omp-relayd`.

Prebuilt static Linux binaries are attached to every [GitHub release](https://github.com/DarkPhilosophy/omp-relay/releases):

- `omp-relayd-linux-x86_64.tar.gz`
- `omp-relayd-linux-aarch64.tar.gz`
- matching `.sha256` checksum files

The OMP extension is published separately as [`omp-relay` on npm](https://www.npmjs.com/package/omp-relay).

For deployment, install the binary outside the source checkout and run it under a process supervisor. Example systemd user command:

```ini
ExecStart=%h/.local/bin/omp-relayd \
  --registry %h/.local/state/omp-relay/registry.json \
  serve --bind 10.90.0.2:43118
```

Bind to a private interface whenever possible. Do not expose the relay directly to the public internet without an additional network security layer.

## Pair and connect

Generate a one-time code on the relay host:

```bash
omp-relayd --registry ~/.local/state/omp-relay/registry.json pair
```

Then run these commands inside OMP:

```text
/relay pair http://10.90.0.2:43118 CODE atv
/relay start atv
/relay status
```

Available commands:

| Command | Purpose |
| --- | --- |
| `/relay pair <url> <code> [name]` | Pair this OMP client with a relay |
| `/relay list` | List paired relay servers |
| `/relay start [name]` | Route OMP provider traffic through the relay |
| `/relay stop` | Return to direct provider connections |
| `/relay status [name]` | Probe health and display latency |
| `/relay debug [on\|off]` | Toggle server-side request logging |

The extension sets `PI_PROXY` only for the current OMP process. Pairing credentials are stored locally in `~/.omp/agent/relay.json` with restrictive file permissions.

## Server administration

```bash
# List paired clients
omp-relayd --registry ~/.local/state/omp-relay/registry.json list

# Revoke one client
omp-relayd --registry ~/.local/state/omp-relay/registry.json revoke UUID

# Run deterministic local diagnostics without listening or touching the real registry
omp-relayd self-test
```

## Development

```bash
bun run format
bun run typecheck
bun run lint
bun run test
bun run test:rust
bun run test:mode
bun run scan
bun run package:check
bun run verify
```

`test:mode` runs the relay binary's deterministic self-test. It does not open a port, contact a provider, or modify the production registry.

## Repository layout

```text
src/                 OMP extension
relayd/              Rust relay server
  src/forward.rs     HTTP forward proxy and CONNECT tunnel
  src/proxy.rs       Legacy HTTP/SSE and WebSocket routes
  src/registry.rs    Pairing and client registry
  src/ws.rs          WebSocket bridge
tests/               Bun extension tests
.scripts/            Leak scan, package checks, and test-mode launcher
.github/workflows/   CI and release-build automation
```

## Security model

- Pairing codes expire after ten minutes and are single-use.
- Client tokens are random and stored hashed on the server.
- Every proxy request requires HTTP proxy authentication.
- Failed pairing attempts are rate-limited by client IP.
- Client revocation takes effect through the server registry.
- Debug logs contain routing metadata, not authorization headers or request bodies.
- The relay is a transport boundary, not a replacement for a private network or firewall.

## License

GNU General Public License v3.0. See [LICENSE](LICENSE).
