import { readdir, readFile } from "node:fs/promises";
import { extname, join, relative } from "node:path";

const ROOT = new URL("..", import.meta.url).pathname;
const ROOTS = ["src", "tests", ".scripts", "relayd/src"];
const TEXT_EXTENSIONS = new Set([".ts", ".mjs", ".rs", ".json", ".toml", ".yml", ".yaml"]);
const PATTERNS = [
	[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/, "private key"],
	[/\bsk-[A-Za-z0-9_-]{20,}\b/, "API key"],
	[/\bgh[pousr]_[A-Za-z0-9]{30,}\b/, "GitHub token"],
	[/\b(?:token|secret|password)\s*[:=]\s*["'][^"']{16,}["']/i, "embedded credential"],
];

async function files(directory) {
	const output = [];
	for (const entry of await readdir(join(ROOT, directory), { withFileTypes: true })) {
		const path = join(directory, entry.name);
		if (entry.isDirectory()) output.push(...(await files(path)));
		else if (TEXT_EXTENSIONS.has(extname(entry.name))) output.push(path);
	}
	return output;
}

const findings = [];
for (const root of ROOTS) {
	for (const path of await files(root)) {
		const content = await readFile(join(ROOT, path), "utf8");
		for (const [pattern, label] of PATTERNS) {
			if (pattern.test(content)) findings.push(`${relative(ROOT, join(ROOT, path))}: possible ${label}`);
		}
	}
}

if (findings.length) {
	console.error(findings.join("\n"));
	process.exit(1);
}
console.log("No credential-shaped content found.");
