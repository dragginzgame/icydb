# Minimum supported Rust version

Apply the [common run contract](README.md) and the
[MSRV rules](../rules/cargo-dependencies.md#minimum-supported-rust-version-msrv).
Non-Rust repositories are `N/A`.

1. Inventory effective `package.rust-version` for each maintained package,
   inherited values, intentional package groups, editions and selected locks.
   Record the development toolchain separately; a toolchain pin is not an MSRV.
2. Trace minimum CI commands through Make/scripts. Check explicit `cargo +VERSION`
   or `RUSTUP_TOOLCHAIN` selection and actual compiler logs. Installing an old
   compiler without selecting it is insufficient. Check supported features,
   native/Wasm targets and public normal/build dependency paths, including macros.
3. Inspect matching CI runs for the selected source and graph. Report missing
   minimum coverage, misleading toolchain selection, and failures with precise
   ownership. Do not label a source review a successful old-compiler build.
4. When focused local execution is authorized in that repository, use its prepared
   MSRV target after inspecting its effects. For an individual supported package
   path, the command shape is:

   ```bash
   RUSTUP_AUTO_INSTALL=0 rustc +VERSION --version
   RUSTUP_AUTO_INSTALL=0 cargo +VERSION --version
   CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 \
     cargo +VERSION check --locked -p PACKAGE
   ```

   Replace placeholders with declared values and repeat for supported feature
   and target selections. A single host/default-feature check cannot prove all
   advertised paths. Keep higher internal test requirements separate where valid.
5. Review why each high floor exists. Identify the exact API, language, edition
   or dependency blocker and a simpler lower-floor alternative. Recommend a
   lower candidate only as a qualification task until it has passed on the actual
   compiler. Avoid dependency downgrades or `--ignore-rust-version` as proof.

Return package group, declared floor, actual checked compiler, source/run,
coverage, concrete blocker and next action. Missing declarations or proof remain
visible even when the current development compiler builds successfully.
