import { describe, expect, test } from "bun:test";
import relayExtension, { formatDuration, proxyUrl } from "../src/index";

describe("relay extension", () => {
	test("registers the relay command and session hook", () => {
		const commands: Array<{ name: string; options: Record<string, unknown> }> = [];
		const events: string[] = [];
		const api = {
			registerCommand(name: string, options: Record<string, unknown>) {
				commands.push({ name, options });
			},
			on(event: string) {
				events.push(event);
			},
		} as never;

		relayExtension(api);

		expect(commands).toHaveLength(1);
		expect(commands[0]?.name).toBe("relay");
		expect(commands[0]?.options.handler).toBeFunction();
		expect(commands[0]?.options.getArgumentCompletions).toBeFunction();
		expect(events).toEqual(["session_start"]);
	});

	test("builds an authenticated forward-proxy URL", () => {
		expect(
			proxyUrl({
				name: "atv",
				url: "http://10.90.0.2:43118",
				uuid: "client-id",
				token: "secret/token",
			}),
		).toBe("http://client-id:secret%2Ftoken@10.90.0.2:43118");
	});

	test("formats relay uptime compactly", () => {
		expect(formatDuration(12)).toBe("12s");
		expect(formatDuration(125)).toBe("2m 5s");
		expect(formatDuration(7_500)).toBe("2h 5m");
		expect(formatDuration(180_000)).toBe("2d 2h");
	});
});
