# IcyDB Release Guide

This guide covers the complete workflow for developing, versioning, and releasing IcyDB, including how users can integrate it as a git dependency.

## 🚀 Quick Start for Users

### Integration as Git Dependency

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
```

### Check Available Versions

```bash
# From your project directory
git ls-remote --tags https://github.com/dragginzgame/icydb.git | grep "v0.0"

# Or use our helper script (if you have the repo cloned)
./scripts/app/check-git-versions.sh
```

### Prerequisites for Maintainers

- Rust toolchain (matches the workspace MSRV)
- `cargo-edit` so `cargo set-version` is available for release bumps:
  ```bash
  cargo install cargo-edit
  ```

## 📋 Development Workflow

### 1. Development Phase

```bash
# Run tests
make test

# Check code quality
make clippy
make fmt-check

```

### 2. Prepare for Release

```bash
# Update changelog with your changes
# Edit CHANGELOG.md and add entries under [Unreleased]

# Check current version
make version

# List available tags
make tags
```

### 3. Create Release

```bash
# Bump version (choose one; updates Cargo.toml/Cargo.lock and tags)
make patch    # 0.17.0 -> 0.17.1
make minor    # 0.17.0 -> 0.18.0
make major    # 0.17.0 -> 1.0.0

# The script stages manifest changes before committing and tagging.
```

### 4. Push Release

```bash
# Push changes and tags
git push --follow-tags
```

This triggers:
- ✅ Automated testing
- ✅ Building for all targets
- ✅ Creating GitHub release

## 🔧 Version Management Commands

### Show Information

```bash
make version          # Show current version
make tags             # List available git tags
make check-versioning # Verify system setup
```

### Bump Versions

```bash
make patch            # 0.17.0 -> 0.17.1
make minor            # 0.17.0 -> 0.18.0
make major            # 0.17.0 -> 1.0.0
```

### Create Releases

```bash
make release          # No-op; CI runs on tag push
```

## 📦 Release Workflow

### Automated Release

When you push a version tag, GitHub Actions automatically:

1. **Tests** the codebase
2. **Builds** for all targets
3. **Creates** a GitHub release with changelog notes

## 🏷️ Git Tag Management

### Creating Tags

Tags are automatically created by the version script:

```bash
# This creates both a commit and a tag
make patch
```

### Managing Tags

```bash
# List all tags
git tag --sort=-version:refname

# Delete a tag (if needed)
git tag -d v1.0.0
git push origin :refs/tags/v1.0.0

# Push specific tag
git push origin v1.0.0
```

## 📝 Changelog Management

### Adding Changes

Always update the changelog before releasing:

```markdown
## [Unreleased]
- Added new feature X
- Fixed bug in Y
- Breaking: Changed API for Z
```

### Changelog Format

Follow [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
## [1.0.0] - 2024-01-15
### Added
- New feature X

### Changed
- Updated API Y

### Fixed
- Bug in Z

### Breaking
- Removed deprecated function
```

## 🔍 Quality Assurance

### Pre-Release Checklist

- [ ] All tests pass: `make test`
- [ ] Code formatting: `make fmt-check`
- [ ] Linting: `make clippy`
- [ ] WASM build: `make build-wasm`
- [ ] Changelog updated
- [ ] Version bumped
- [ ] Working directory clean

### Post-Release Verification

- [ ] GitHub Actions passed
- [ ] GitHub release created
- [ ] Users can access new version

## 🛠️ Troubleshooting

### Common Issues

#### Version Already Exists

```bash
# Delete existing tag
git tag -d v1.0.0
git push origin :refs/tags/v1.0.0

# Create new version
make patch
```

#### Release Creation Fails

1. Check GitHub Actions workflow
2. Verify tag was pushed correctly
3. Ensure changelog format is correct

#### Git Dependency Issues

```bash
# Check if tag exists
git ls-remote --tags https://github.com/dragginzgame/icydb.git | grep v0.0

# Verify repository access
git ls-remote https://github.com/dragginzgame/icydb.git
```

## 📚 Integration Examples

### Basic Integration

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
```

### With Features

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1", features = ["serde"] }
```

### Development Version

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", branch = "main", features = [] }
```

### Workspace Integration

```toml
[workspace.dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }

[dependencies]
icydb = { workspace = true }
```

## 🎯 Best Practices

### For Maintainers

1. **Always test** before releasing
2. **Update changelog** with every change
3. **Use semantic versioning** correctly
4. **Tag releases** immediately after pushing
5. **Monitor CI/CD** pipeline

### For Users

1. **Pin versions** with tags, not branches
2. **Test updates** before deploying
3. **Check changelog** for breaking changes
4. **Use minimal features** for better performance

## 📖 Additional Resources

- [Versioning Guide](VERSIONING.md) - Detailed versioning information
- [Integration Guide](INTEGRATION.md) - Complete integration documentation
- [Changelog](CHANGELOG.md) - Version history and changes


## 🆘 Support

- **Issues**: Open an issue in this repo
- **Discussions**: Use internal channels (e.g., Slack/Teams)
- Source: `icydb/` (no crates.io/docs.rs)
