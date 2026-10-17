# Private IC Wire fetch

Every Cargo CI lane uses this action before compiling the locked workspace.
`actions/create-github-app-token` issues a short-lived token restricted to
`dragginzgame/ic-wire` with Contents: read; the action revokes it at job cleanup.
The token is available only during `cargo fetch --locked`. The credential helper
checks protocol, host and the exact repository path before returning it. Neither
the token nor App key is written to Git configuration, credentials files, Cargo
caches or `GITHUB_ENV`. The dependency source and exact commit remain unchanged.

An owner must create/install a read-only GitHub App for the private `ic-wire`
repository and provision `IC_WIRE_APP_ID` and `IC_WIRE_APP_PRIVATE_KEY` as Actions
secrets available to IcyDB. No secrets were configured by this source change.
The built-in IcyDB `GITHUB_TOKEN` cannot read that separate private repository.
Fork pull requests have no access to these secrets: qualify an explicitly trusted
branch instead of exposing private access through `pull_request_target`.

Acceptance still requires clean hosted Linux/MSRV and both macOS fetch/build
runs. A warm local cache or synthetic helper test cannot establish this access.
Tracking: [#370](https://github.com/dragginzgame/icydb/issues/370).
