# IcyDB Integration Guide

Use a pinned git tag for reproducible builds and immutable versions.

## Quick Start

Add IcyDB to your `Cargo.toml`:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
```

### Toolchain

- Rust 1.91.1 (edition 2024). Install with:
  - `rustup toolchain install 1.91.1`
  - Ensure CI and local dev use the same toolchain.

## Integration Methods

### 1) Git dependency with tag (recommended)

Use the Quick Start snippet above (pinned tag) for production.

**Pros:**
- Exact version pinning
- Reproducible builds
- No dependency on crates.io availability
- **Immutable tags** - code at `v0.21.0` will never change
- **Security** - prevents supply chain attacks

**Cons:**
- Manual updates required
- Larger download size

### 2) Git dependency with branch

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", branch = "main", features = [] }
```

**Pros:**
- Always latest changes
- Automatic updates

**Cons:**
- Unstable API
- Build reproducibility issues
- Not recommended for production

### 3) Git dependency with commit

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", rev = "abc123...", features = [] }
```

**Pros:**
- Exact commit pinning
- Reproducible builds

**Cons:**
- Hard to track updates
- Manual commit hash management

## Optional features

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1", features = [
  "serde",   # serde derive/support in types
] }
```

## Basic usage

```rust
use icydb::prelude::*;

#[entity(
    sk(field = "id"),
    fields(
        field(ident = "id", value(item(is = "types::Ulid"))),
        field(ident = "name", value(item(is = "text::Name"))),
        field(ident = "description", value(item(is = "text::Description"))),
    ),
)]
pub struct User {}

// Build a query and execute it via db()
let query = icydb::db::query::load()
    .filter(|f| f.contains("name", "ann"))
    .sort(|s| s.asc("name"))
    .limit(50);

let views: Vec<<User as icydb::core::traits::TypeView>::View> =
    db().load::<User>().execute(&query)?.views();
```

## Migration

- Check [CHANGELOG.md](CHANGELOG.md) between tags for any breaking notes.

## Troubleshooting

### Common Issues

#### 1. Compilation Errors

```bash
error: failed to select a version for `icydb`
```

**Solution:** Ensure the tag exists and is spelled correctly:
```bash
git ls-remote --tags https://github.com/dragginzgame/icydb.git
```

#### 2. Feature Not Found

```bash
error: feature `some_feature` is not available
```

**Solution:** Check available features in the [optional features](#optional-features) section above.

#### 3. Version Conflicts

```bash
error: failed to resolve dependencies
```

**Solution:** Use exact version pinning with tags (see Quick Start).

### Getting help

1. Check the [changelog](CHANGELOG.md) for version-specific notes
2. Review the [versioning guide](VERSIONING.md) for release information
3. Open an issue in this repo

## Security

### 🔒 Tag immutability

IcyDB enforces **tag immutability** - once a version is tagged and pushed, the code at that version will never change. This ensures:

- **Reproducible builds** - `v0.0.1` always contains the same code
- **Supply chain security** - prevents malicious code injection
- **Dependency stability** - your builds won't break unexpectedly

### Security Verification

```bash
# Check if a specific version exists and is immutable
git ls-remote --tags https://github.com/dragginzgame/icydb.git | grep v0.0

# Verify the commit hash hasn't changed
git ls-remote https://github.com/dragginzgame/icydb.git v0.0.1
```

## Best Practices

### 1. Version Pinning

Always use tag-based dependencies for production:

```toml
# ✅ Good - pinned version
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }

# ❌ Bad - floating version
icydb = { git = "https://github.com/dragginzgame/icydb.git", branch = "main", features = [] }
```

### 2. Feature Selection

Only enable features you need:

```toml
# ✅ Good - minimal features
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1", features = ["serde"] }

# ❌ Bad - unnecessary features
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1", features = ["serde"] }
```

### 3. Regular Updates

Keep your dependency updated:

```bash
# Check for new versions
git ls-remote --tags https://github.com/dragginzgame/icydb.git | grep "v0.0"

# Update to latest patch version
# Change tag from v0.0.0 to v0.0.1
```

### 4. Testing

Always test after version updates:

```bash
cargo test
cargo build --target wasm32-unknown-unknown
```

## Advanced Configuration

### Workspace Dependencies

For workspace projects, add IcyDB to the workspace dependencies:

```toml
[workspace.dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }

[workspace.members]
member1 = "crates/member1"
member2 = "crates/member2"

# In each member's Cargo.toml
[dependencies]
icydb = { workspace = true }
```

### Development Dependencies

For testing and development:

```toml
[dev-dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
```

## Version History

For a complete version history and detailed changelog, see [CHANGELOG.md](CHANGELOG.md).

### Recent Releases

See this repo’s Releases page for notes and tags.

## Support

- Source: `icydb/` (no crates.io/docs.rs)
- **Issues**: Open an issue in this repo
- **Discussions**: Use internal channels (e.g., Slack/Teams)
