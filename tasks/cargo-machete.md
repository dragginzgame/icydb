# Unused Cargo dependencies

Apply the [common run contract](README.md). Check every maintained Cargo workspace
in the selected repositories, including independently scoped tools and fixtures.
Non-Rust repositories are `N/A`.

Use the prepared consumer-qualified cargo-machete executable and record its
version. From each selected workspace root, the basic source scan is:

```bash
CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 cargo machete
```

Read existing targets before using them: a combined dependency target may also
fetch dependencies or run a security audit. The default scan needs neither.
Do not enable automatic fixes. `--with-metadata` can modify lockfiles and is
outside the central read-only pass; use independently obtained locked metadata
or request owning-repository qualification when needed.

Review findings against renamed crates, proc macros, generated code, build
scripts, target/feature-specific uses and documented ignore entries. Treat a
reported dependency as a candidate until that trace establishes it is unused.
Review stale ignores too, without changing suppression lists during the check.

Record package, declaration, candidate dependency, usage evidence and proposed
removal or justified retention. Preserve workspace catalog ownership when
proposing a fix. A clean scan is only unused-dependency evidence, not proof of
dependency security or complete feature coverage. Distinguish findings from
tool failures using the selected tool's documented exit statuses.

Reference: [cargo-machete usage and limitations](https://github.com/bnjbvr/cargo-machete#usage).
