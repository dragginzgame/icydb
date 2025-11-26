# Versioning Guide

This document explains how versioning works in the IcyDB project and how to manage releases.

## Overview

IcyDB uses [Semantic Versioning](https://semver.org/) (SemVer) for all releases. The version format is `MAJOR.MINOR.PATCH` (e.g., `1.2.3`).

- **MAJOR**: Breaking changes that require migration
- **MINOR**: New features in a backward-compatible manner
- **PATCH**: Backward-compatible bug fixes

## Current Version

The current version is managed in the workspace `Cargo.toml` file and is shared across all crates in the workspace.

## Version Management Commands

IcyDB’s release workflow is driven entirely from the Makefile and Cargo’s
`set-version` subcommand.

### Show the current version

```bash
make version
```

### Bump the version

Each command runs `cargo set-version --workspace --bump …`, regenerates the
lockfile, commits the change, creates an annotated tag, and pushes to origin.

```bash
make patch    # 0.15.2 -> 0.15.3
make minor    # 0.15.2 -> 0.16.0
make major    # 0.15.2 -> 1.0.0
```

> **Tip:** Update `CHANGELOG.md` manually before running a bump command.

These targets require `cargo-edit` (for `cargo set-version`) and will abort if
the working tree is dirty or the resulting tag already exists.

## Release Process

### 1. Prepare for Release

Before creating a release:

1. Ensure all changes are committed
2. Update the changelog with your changes
3. Test thoroughly

### 2. Create Release

```bash
# For a patch release (bug fixes)
make patch

# For a minor release (new features)
make minor

# For a major release (breaking changes)
make major
```

The targets automatically create the release commit, tag, and push via
`git push --follow-tags`, triggering the GitHub Actions release workflow.

## Automated Release Workflow

When you push a version tag (e.g., `v1.0.0`), the following happens automatically:

1. **Testing**: All tests run on multiple targets
2. **Building**: Release builds are created
3. **GitHub Release**: A GitHub release is created with notes from the changelog

## Changelog Management

The changelog follows the [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
## [Unreleased]
- New features in development

## [1.0.0] - 2024-01-15
- Breaking changes
- New features
- Bug fixes
```

### Adding Changes

When making changes, add them to the `[Unreleased]` section:

```markdown
## [Unreleased]
- Added new feature X
- Fixed bug in Y
- Breaking: Changed API for Z
```

## Workspace Versioning

All crates in the workspace share the same version. When you bump the version:

- The workspace `Cargo.toml` is updated
- All crate dependencies are automatically updated
- All crates are built with the same version

## Pre-release Versions

The helper script currently supports strict patch/minor/major bumps. For pre-release identifiers (e.g., `1.0.0-beta.1`), update `Cargo.toml` manually, regenerate `Cargo.lock`, and create the annotated tag yourself.

## Troubleshooting

### Working Directory Not Clean

If you get an error about the working directory not being clean:

```bash
# Commit your changes first
git add .
git commit -m "Your commit message"

# Then run the version bump
make patch
```

### Tag Already Exists

If a tag already exists for the version you're trying to create:

1. Delete the local tag: `git tag -d v1.0.0`
2. Delete the remote tag: `git push origin :refs/tags/v1.0.0`
3. Run the version bump command again (`make patch`, `make minor`, or `make major`)

### Release Issues

If GitHub release creation fails:

1. Check that the GitHub Actions workflow is configured correctly
2. Ensure the tag was pushed to the remote repository
3. Verify that the changelog format is correct

## Security & Tag Immutability

### 🔒 Tag Immutability

**CRITICAL**: Once a version tag is pushed, the code at that version must NEVER change. This is essential for:

- **Reproducible builds** - Users can trust that `v1.0.0` always contains the same code
- **Security** - Prevents supply chain attacks through tag modification
- **Dependency integrity** - Git dependencies remain stable and predictable

### Security Checks

```bash
# Check tag immutability and version integrity
make security-check

# This will detect:
# - Modified tags
# - Unauthorized changes to tagged versions
# - Force-pushed tags
# - Broken or invalid tags
```

### What Happens When You Try to Modify a Tagged Version

```bash
# This will FAIL if v1.0.0 is already tagged
make patch  # Error: Current version is already tagged

# You must bump to a new version instead
make patch  # Creates v1.0.1
```

## Best Practices

1. **Always update the changelog** before creating a release
2. **Test thoroughly** before releasing
3. **Use semantic versioning** correctly
4. **Create meaningful commit messages** for version bumps
5. **Review the automated release** after pushing tags
6. **Never modify existing tags** - always bump to a new version
7. **Run security checks** regularly with `make security-check`

## Manual Version Management

If you need to manually manage versions:

```bash
# Update version in Cargo.toml
sed -i 's/^version = ".*"/version = "1.0.0"/' Cargo.toml

# Create git tag
git add Cargo.toml
git commit -m "Bump version to 1.0.0"
git tag -a v1.0.0 -m "Release version 1.0.0"
git push --follow-tags
```

However, it's recommended to use the version script for consistency and automation. 
