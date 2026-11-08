# toko-tooling

The actual source distribution of Dragginzgame development tooling: scripts,
Make interfaces, CI actions, catalogs, rules, licenses and guides. Its empty
Rust library is dependency-free and `no_std`, with no build script.

Select an exact release once in repository-root Cargo.toml:

```toml
[workspace.dependencies]
toko-tooling = "=0.36.0"
```

Every real workspace package inherits it as a dev-dependency:

```toml
[dev-dependencies]
toko-tooling.workspace = true

[package.metadata.cargo-machete]
ignored = ["toko-tooling"]
```

Merge existing exemptions. A standalone package declares the exact version
in its own dev-dependencies instead. No helper packages or Rust imports are needed.
`cargo update -v` reports newer available releases; the exact pin keeps adoption
intentional. After changing the root pin, run `cargo update -p toko-tooling`, then
`make shared-tooling-update`. Cargo.lock supplies the selected version and checksum;
installation downloads and authenticates the actual crates.io source payload.

`make shared-tooling-install` installs the existing Cargo.lock selection.
Offline `make shared-tooling-check` validates every member's declaration, the root
pin, Cargo.lock and installed files. Common CI/release entrypoints require it;
Cargo builds never install tools. The producer qualifies its source archive
instead of depending on itself.

Follow the [migration guide](https://github.com/dragginzgame/toko-tooling/blob/main/docs/consuming-snapshots.md#migration-from-cargo-dependencies)
to remove the 0.34–0.35 metadata tables and `toko-tooling.lock`, plus any old
selection-only packages. Real tools and qualification packages remain.
Releases before 0.33 contain only a marker; 0.36 introduces this Cargo-visible
per-package selection contract.
