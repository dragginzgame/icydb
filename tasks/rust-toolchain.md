# Rust development toolchain freshness

Apply the [common run contract](README.md). This task compares development
compilers with current stable Rust; [msrv](msrv.md) owns minimum compatibility.
Non-Rust repositories are `N/A`.

1. Read the owning `rust-toolchain.toml` or documented selector, CI overrides,
   formatter/tool requirements and approved exceptions. Record mismatches and
   whether actual CI uses the intended version. Do not auto-install a missing
   toolchain just to print its version.
2. At run time, consult the [official Rust releases](https://blog.rust-lang.org/releases/)
   or the [stable channel manifest](https://static.rust-lang.org/dist/channel-rust-stable.toml).
   Record the URL, observation time, stable version and release date. Include
   stable patch releases; exclude beta/nightly. Network failure leaves freshness
   `BLOCKED`; a cached or remembered version cannot establish "latest".
3. Compare each development pin with that observation. Explain an intentional
   lag using current compatibility evidence; otherwise propose a focused upgrade
   issue. An available update is a finding, not proof of a broken build.
4. Identify the owner's needed qualification: formatting/Clippy changes,
   selected targets, build tools and relevant tests. Keep MSRV metadata unchanged
   unless independent evidence justifies changing the supported floor.

Return repository, selected development compiler, latest stable observation,
reason to retain/upgrade and owning issue. This check does not run `rustup update`,
change pins, update locks or claim an untested compiler upgrade works.
