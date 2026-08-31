import { spawn } from "node:child_process";

const command = process.platform === "win32" ? "cargo.exe" : "cargo";
const child = spawn(command, ["run", "--quiet", "--manifest-path", "relayd/Cargo.toml", "--", "self-test"], {
	stdio: "inherit",
});
child.on("exit", (code) => process.exit(code ?? 1));
