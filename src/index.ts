import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@oh-my-pi/pi-coding-agent";

interface RelayEntry {
	name: string;
	url: string;
	uuid: string;
	token: string;
}

interface RelayState {
	relays: RelayEntry[];
	active: string | null;
	debug: boolean;
}

interface PairResponse {
	uuid: string;
	token: string;
}

const statePath = join(homedir(), ".omp", "agent", "relay.json");
const emptyState = (): RelayState => ({ relays: [], active: null, debug: false });
const WIDGET_KEY = "omp-relay-status";
const HEALTH_REFRESH_MS = 15_000;

export function formatDuration(totalSeconds: number): string {
	const seconds = Math.max(0, Math.floor(totalSeconds));
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.floor(seconds / 60);
	if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
	const hours = Math.floor(minutes / 60);
	if (hours < 24) return `${hours}h ${minutes % 60}m`;
	return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

export function proxyUrl(relay: RelayEntry): string {
	const url = new URL(relay.url);
	url.username = relay.uuid;
	url.password = relay.token;
	return url.toString().replace(/\/$/, "");
}

export default function relayExtension(pi: ExtensionAPI): void {
	let healthTimer: Timer | undefined;
	let widgetContext: ExtensionContext | undefined;

	async function loadState(): Promise<RelayState> {
		try {
			const parsed = JSON.parse(await readFile(statePath, "utf8")) as Partial<RelayState>;
			return {
				relays: Array.isArray(parsed.relays) ? parsed.relays : [],
				active: typeof parsed.active === "string" ? parsed.active : null,

				debug: parsed.debug === true,
			};
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return emptyState();
			throw error;
		}
	}

	async function saveState(state: RelayState): Promise<void> {
		await mkdir(dirname(statePath), { recursive: true });
		const temporary = `${statePath}.tmp-${process.pid}-${Date.now()}`;
		await writeFile(temporary, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
		await rename(temporary, statePath);
	}

	function authHeaders(relay: RelayEntry): Record<string, string> {
		return { "X-OMP-Relay-Token": `${relay.uuid}:${relay.token}` };
	}

	async function relayFetch(relay: RelayEntry, path: string, init?: RequestInit): Promise<Response> {
		return fetch(`${relay.url}${path}`, {
			...init,
			headers: { ...authHeaders(relay), ...init?.headers },
			signal: AbortSignal.timeout(5000),
		});
	}

	function setRelayEnvironment(relay: RelayEntry): void {
		Bun.env.PI_PROXY = proxyUrl(relay);
	}

	function clearRelayEnvironment(): void {
		delete Bun.env.PI_PROXY;
	}

	async function updateWidget(ctx: ExtensionContext, requestedName?: string): Promise<void> {
		const state = await loadState();
		const relay = state.relays.find((entry) => entry.name === (requestedName ?? state.active));
		const theme = ctx.ui.theme;
		if (!relay || state.active !== relay.name) {
			ctx.ui.setWidget(
				WIDGET_KEY,
				[
					[
						theme.fg("error", "󰅙"),
						theme.fg("error", theme.bold(" RELAY OFFLINE")),
						theme.fg("dim", "  •  direct provider connection"),
					].join(""),
				],
				{ placement: "belowEditor" },
			);
			return;
		}
		const started = performance.now();
		try {
			const response = await relayFetch(relay, "/status");
			const latency = Math.round(performance.now() - started);
			if (!response.ok) {
				ctx.ui.setWidget(
					WIDGET_KEY,
					[
						[
							theme.fg("warning", "󰀦"),
							theme.fg("warning", theme.bold(" RELAY DEGRADED")),
							theme.fg("muted", `  ${relay.name}`),
							theme.fg("dim", `  •  HTTP ${response.status}  •  ${latency} ms`),
						].join(""),
					],
					{ placement: "belowEditor" },
				);
				return;
			}
			const status = (await response.json()) as { active_streams?: number; uptime_s?: number };
			const streams = status.active_streams ?? 0;
			const uptime = formatDuration(status.uptime_s ?? 0);
			ctx.ui.setWidget(
				WIDGET_KEY,
				[
					[
						theme.fg("success", "󰄬"),
						theme.fg("success", theme.bold(" RELAY CONNECTED")),
						theme.fg("accent", `  󰖟 ${relay.name}`),
						theme.fg("muted", `  •  󰔟 ${latency} ms`),
						theme.fg(streams > 0 ? "success" : "dim", `  •  󰓅 ${streams} ${streams === 1 ? "stream" : "streams"}`),
						theme.fg("dim", `  •  󱫑 ${uptime}`),
					].join(""),
				],
				{ placement: "belowEditor" },
			);
		} catch {
			const latency = Math.round(performance.now() - started);
			ctx.ui.setWidget(
				WIDGET_KEY,
				[
					[
						theme.fg("error", "󰅙"),
						theme.fg("error", theme.bold(" RELAY UNREACHABLE")),
						theme.fg("muted", `  ${relay.name}`),
						theme.fg("dim", `  •  󰔟 ${latency} ms  •  check server or network`),
					].join(""),
				],
				{ placement: "belowEditor" },
			);
		}
	}

	function startHealthRefresh(ctx: ExtensionContext): void {
		widgetContext = ctx;
		if (healthTimer) ctx.clearTimer(healthTimer);
		healthTimer = ctx.setInterval(async () => {
			if (widgetContext) await updateWidget(widgetContext);
		}, HEALTH_REFRESH_MS);
	}

	async function applyRelay(ctx: ExtensionContext, requestedName?: string, persist = true): Promise<void> {
		const state = await loadState();
		const relay = requestedName
			? state.relays.find((entry) => entry.name === requestedName)
			: (state.relays.find((entry) => entry.name === state.active) ?? state.relays[0]);
		if (!relay) throw new Error("no paired relay; run /relay pair <url> <code> [name]");
		setRelayEnvironment(relay);
		state.active = relay.name;
		if (persist) await saveState(state);
		await updateWidget(ctx, relay.name);
	}

	async function stopRelay(ctx: ExtensionContext): Promise<void> {
		clearRelayEnvironment();
		const state = await loadState();
		state.active = null;
		await saveState(state);
		await updateWidget(ctx);
	}

	async function command(args: string, ctx: ExtensionCommandContext): Promise<void> {
		const [subcommand, ...parameters] = args.trim().split(/\s+/).filter(Boolean);
		try {
			switch (subcommand) {
				case "pair": {
					const [urlText, code, requestedName] = parameters;
					if (!urlText || !code) throw new Error("usage: /relay pair <url> <code> [name]");
					const url = new URL(urlText);
					const relayUrl = url.toString().replace(/\/$/, "");
					const response = await fetch(`${relayUrl}/pair`, {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ code, name: requestedName ?? url.host }),
						signal: AbortSignal.timeout(5000),
					});
					if (!response.ok) throw new Error(`pair failed: HTTP ${response.status} ${await response.text()}`);
					const paired = (await response.json()) as PairResponse;
					const state = await loadState();
					const name = requestedName ?? url.host;
					const entry = { name, url: relayUrl, uuid: paired.uuid, token: paired.token };
					const existing = state.relays.findIndex((relay) => relay.name === name);
					if (existing >= 0) state.relays[existing] = entry;
					else state.relays.push(entry);
					await saveState(state);
					ctx.ui.notify(`relay: paired ${name}`, "info");
					break;
				}
				case "list": {
					const state = await loadState();
					ctx.ui.notify(
						state.relays.length
							? state.relays
									.map((relay) => `${relay.name}${relay.name === state.active ? " (active)" : ""}: ${relay.url}`)
									.join("\n")
							: "relay: no paired relays",
						"info",
					);
					break;
				}
				case "start": {
					await applyRelay(ctx, parameters[0]);
					const state = await loadState();
					ctx.ui.notify(`relay: forwarding OMP requests via ${state.active}`, "info");
					break;
				}
				case "stop": {
					await stopRelay(ctx);
					ctx.ui.notify("relay: forwarding disabled", "info");
					break;
				}
				case "status": {
					const state = await loadState();
					const relay = state.relays.find((entry) => entry.name === (parameters[0] ?? state.active));
					if (!relay) throw new Error("no active or named relay");
					const started = performance.now();
					const response = await relayFetch(relay, "/status");
					const latency = Math.round(performance.now() - started);
					if (!response.ok) throw new Error(`status failed: HTTP ${response.status} ${await response.text()}`);
					await updateWidget(ctx, relay.name);
					ctx.ui.notify(`relay ${relay.name}: ${latency} ms · ${JSON.stringify(await response.json())}`, "info");
					break;
				}
				case "debug": {
					const state = await loadState();
					const relay = state.relays.find((entry) => entry.name === state.active);
					if (!relay) throw new Error("no active relay");
					const enabled = parameters[0] !== "off";
					const response = await relayFetch(relay, "/debug", {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ enabled }),
					});
					if (!response.ok) throw new Error(`debug failed: HTTP ${response.status} ${await response.text()}`);
					state.debug = enabled;
					await saveState(state);
					ctx.ui.notify(`relay: debug ${enabled ? "on" : "off"}`, "info");
					break;
				}
				default:
					ctx.ui.notify(
						"usage: /relay pair <url> <code> [name] | list | start [name] | stop | status [name] | debug [on|off]",
						"info",
					);
			}
		} catch (error) {
			ctx.ui.notify(`relay: ${error instanceof Error ? error.message : String(error)}`, "error");
		}
	}

	pi.registerCommand("relay", {
		description: "Relay AI traffic through an authenticated forward proxy",
		getArgumentCompletions: (prefix) => {
			const normalized = prefix.trimStart().toLocaleLowerCase();
			return [
				{ label: "pair", description: "Pair this OMP client with a relay", value: "pair " },
				{ label: "list", description: "List paired relay servers", value: "list" },
				{ label: "start", description: "Route OMP provider traffic through a relay", value: "start " },
				{ label: "stop", description: "Disable relay forwarding", value: "stop" },
				{ label: "status", description: "Show connection health and latency", value: "status" },
				{ label: "debug", description: "Toggle relay request logging", value: "debug " },
			].filter((command) => command.value.startsWith(normalized));
		},
		handler: command,
	});

	pi.on("session_start", async (_event, ctx) => {
		widgetContext = ctx;
		try {
			const state = await loadState();
			if (state.active) await applyRelay(ctx, state.active, false);
			else await updateWidget(ctx);
			startHealthRefresh(ctx);
		} catch (error) {
			ctx.ui.notify(
				`relay: failed to restore active relay: ${error instanceof Error ? error.message : String(error)}`,
				"error",
			);
			await updateWidget(ctx);
			startHealthRefresh(ctx);
		}
	});
}
