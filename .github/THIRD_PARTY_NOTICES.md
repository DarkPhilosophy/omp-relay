Third-Party Notices

OMP — Oh My Pi (https://github.com/can1357/oh-my-pi)
- Use: host application; this repository ships an extension loaded by OMP. No OMP code is bundled.

Rust crates
- Use: compiled into the `omp-relayd` release binary. Their licenses and source metadata are recorded by Cargo in `relayd/Cargo.lock`.

Bun, Biome, TypeScript, and OMP type definitions
- Use: development, testing, formatting, and extension type-checking. They are not bundled in the npm package.

The npm package does not bundle third-party source code or development dependencies. The standalone `omp-relayd` binaries statically link their Rust dependencies under the licenses recorded by the corresponding crates.
