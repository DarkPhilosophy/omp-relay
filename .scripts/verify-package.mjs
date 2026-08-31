import { readFile, stat } from "node:fs/promises";
import { $ } from "bun";

const required = [
	"README.md",
	"LICENSE",
	".github/CHANGELOG.md",
	"src/index.ts",
	"relayd/Cargo.toml",
	"relayd/src/main.rs",
];
for (const path of required) {
	const metadata = await stat(path);
	if (!metadata.isFile() || metadata.size === 0) throw new Error(`required package file is empty: ${path}`);
}

const manifest = JSON.parse(await readFile("package.json", "utf8"));
if (manifest.exports !== "./src/index.ts") throw new Error("package export must point to src/index.ts");
if (!manifest.omp?.extensions?.includes("./src/index.ts"))
	throw new Error("OMP extension manifest is missing src/index.ts");
if (manifest.license !== "GPL-3.0-or-later") throw new Error("package license must match LICENSE");

const cargoManifest = await readFile("relayd/Cargo.toml", "utf8");
const cargoVersion = cargoManifest.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
const cargoLicense = cargoManifest.match(/^license\s*=\s*"([^"]+)"/m)?.[1];
if (cargoVersion !== manifest.version) throw new Error("npm and Cargo versions must match");
if (cargoLicense !== manifest.license) throw new Error("npm and Cargo licenses must match");

const packOutput = await $`npm pack --dry-run --json`.text();
const [{ files = [] }] = JSON.parse(packOutput);
const packedPaths = new Set(files.map(({ path }) => path));
for (const path of ["README.md", "LICENSE", ".github/CHANGELOG.md", "package.json", "src/index.ts"]) {
	if (!packedPaths.has(path)) throw new Error(`missing npm package asset: ${path}`);
}
for (const path of packedPaths) {
	const isForbidden = ["tests/", ".scripts/", "relayd/", "node_modules/"].some((prefix) => path.startsWith(prefix));
	const isUnexpectedGitHubFile = path.startsWith(".github/") && path !== ".github/CHANGELOG.md";
	if (isForbidden || isUnexpectedGitHubFile) throw new Error(`unexpected npm package asset: ${path}`);
}

console.log("Package structure and npm contents are complete.");
