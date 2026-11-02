# Tool store retention and maintenance
<!-- Shared companions: scripts/dev/tool-store-maintenance.pl scripts/dev/compiler-cache-maintenance.pl -->

Shared owns the existing tool store and its exported setup helpers. Rust setup
now discards successful `build/` and private `home/` scratch after executable and
receipt admission, under the installation lock. Selected binaries, crate source,
packaged lock, native observations and installation receipts remain available for
offline admission. This does not clean a consumer workspace or run `cargo clean`.

## Current requirements

Successful setup records one current requirement per physical checkout and
logical tool role. The default role is `package:kind:target:profile`; refreshing
its version, features, compiler or native inputs replaces that requirement.
`install-rust-tools.sh --selection-key NAME` distinguishes simultaneous uses of
different versions/features of the same selected tool. Common catalog setup owns
its separate package roles and does not accept this option.

Another checkout's older requirement remains live. The active complete common
bundle also remains live after a partial refresh, even when individual tool roles
have advanced. Historical `selected-*`, `rust-set.*` and inactive `common-set.*`
names do not themselves establish liveness. Checkers are read-only and do not
register or repair requirements; rerun explicit setup when requirements change.

Deleted temporary checkout directories no longer protect generations. For a
completed temporary checkout which will remain on disk, retire its requirements
explicitly with `forget` below. Do not forget a checkout still using tools. An
existing checkout remains declared until refreshed, forgotten or removed; age
alone does not establish that a consumer has stopped needing an older version.

## Preview and reclamation

Invoke the helper from the reviewed Shared snapshot. `STORE` below is the physical
store root, normally `$HOME/.local/share/shared-tooling/tools`; honor an existing
`SHARED_TOOLING_STORE` or `XDG_DATA_HOME` selection. Resolve ancestor aliases such
as macOS `/var` to their physical paths before invoking this interface.

```bash
perl scripts/dev/tool-store-maintenance.pl preview "$STORE"
# Stop all tools using this store and finish setup, builds and evidence capture.
perl scripts/dev/tool-store-maintenance.pl apply "$STORE" --quiescent
```

Preview changes nothing. Its JSON reports each Rust generation as protected or
reclaimable, with the reason. `physical_bytes` at the top level counts allocated
regular-file blocks once per device/inode across safely inspected Rust trees.
Per-entry totals deduplicate within that entry; **do not sum them** across entries.
`reclaimable_physical_bytes` counts only inodes whose every hardlink is inside
eligible generations, excluding receipts to be preserved. These are payload-byte
estimates, not a promise of net filesystem free space: directory blocks, new
receipt/journal allocation, filesystem compression/reflinks and unsafe trees are
not included. Hardlinks outside the admitted inventory never count as freed bytes.

Apply recomputes eligibility under `maintenance.lock`. It preserves immutable
receipts under `receipts/<generation-hash>/`, journals removal, deletes eligible
generations and retires matching generated checkout handles and obsolete common
bundles. It never follows payload symlinks or removes unrelated checkout files.
Active installation locks protect their entire slot; common-setup locks refuse
maintenance. Setup cooperates with the global maintenance guard. Interrupted
removal can resume from the journal without treating a half-removed generation as
newly unowned. Subsequent maintenance converges rather than retaining a fallback
for every refresh.

The existing interface returns executable paths directly; it has no execution
leases. Consequently **setup does not automatically reclaim old executables**.
`--quiescent` asserts that tools, builds and evidence readers have stopped across
the store's consumers. A background timer or concurrent setup is not a valid
substitute. The helper cannot prove that assertion by scanning processes. This
boundary protects running tools while allowing owner-operated maintenance without
a new per-version approval after adopting this policy.

After a killed maintenance process, inspect `maintenance.lock/owner.json` and its
process/operation. Only after establishing that it has stopped may the operator
remove that exact owner file and empty lock directory, then repeat preview/apply.
Never infer an abandoned lock solely from age. Same-user concurrent path mutation
outside these locks is unsupported; this is ownership admission, not a sandbox.

## Failure evidence and legacy migration

New interrupted or failed attempts retain compiler output for seven days from
creation. Logs and small original/observed identities survive reclamation as
receipts. Extend an actual recovery with a future Unix timestamp and reason:

```bash
perl scripts/dev/tool-store-maintenance.pl pin "$STORE" "$GENERATION" "$UNTIL" 'investigating build failure'
perl scripts/dev/tool-store-maintenance.pl forget "$STORE" "$FINISHED_CHECKOUT" --quiescent
```

Pins expire; their age is not silently renewed by inspection. Receipts larger
than 16 MiB protect a generation until its owner explicitly handles that evidence.
Preserve any additional recovery output with a pin before expiry. Successful
scratch trimming never removes source archives, binaries, receipts or failure
observations. Evidence capture archives the remaining admitted generation, not
scratch which has already been discarded.

Pre-policy generations are reported as `unregistered-generation` and protected.
Do not infer that an unregistered older consumer has disappeared. First inventory
every consumer and refresh its common and product-selected tools, or register its
still-needed legacy generation explicitly. Then enroll only individually reviewed
obsolete generations for normal reclamation:

```bash
perl scripts/dev/tool-store-maintenance.pl register "$STORE" "$CHECKOUT" "$ROLE" "$GENERATION"
perl scripts/dev/tool-store-maintenance.pl manage "$STORE" "$OBSOLETE_GENERATION" --quiescent
perl scripts/dev/tool-store-maintenance.pl preview "$STORE"
```

Registering is a declaration of current use, not proof of executable integrity;
normal setup/check admission remains required. `manage` admits ownership and the
physical tree, not obsolescence. Current requirements and recovery pins still win.
This explicit migration avoids silently collecting tools used by older snapshots.
Host/IC generations, Cargo's shared registry cache and old physical checkout-local
installations are outside this Rust-generation collector. In particular, archive
bytes needed for offline authentication are not redundant downloads.

## Consumer adoption and native build caches

Adopt the installer, build helper, native-package classifier and maintenance
helper through the canonical snapshot exporter; their companion declarations
close over the new files. Refresh the evidence selector in the same adoption.
Run setup and offline checks before maintenance. For IC Tooling, check the common
bundle and the release editor: its new role replaces obsolete editor requirements,
and quiescent maintenance removes only handles whose generation is now obsolete.

Profiler and Platform own their separate native integration roots. Their adoption
must declare the `.build`/compiler cache budget, serialize provider replacement
with assembly/build operations, stop accumulating pristine `retained-platform-*`
copies, and preserve selected provider inputs, final binaries and receipts. Large
incremental and dependency trees are regenerable cache, not required provenance.
Move new scratch beneath the owning `target/` unless the provider's technical
contract requires another root; existing output requires explicit migration.
Shared's Rust collector neither traverses those roots nor authorizes deleting
their existing contents. Product-owned cache implementation and native Linux
qualification remain separate from Shared's substitute installer fixtures and
macOS host qualification.

`scripts/dev/compiler-cache-maintenance.pl prepare|finish|preview ROOT OWNER LOCK
PID MAX_BYTES MAX_AGE_SECONDS` is the separate compiler-scratch primitive for
these adapters. It reuses the tool collector's owned-tree inspection/removal,
but never enters the Rust tool store. The caller must hold its existing exclusive
assembly lock containing `PID` and serialize every build and maintenance call.
`prepare` admits a new, absent root or verifies its exact owner, then retires an
idle/oversized `build/` subtree. `finish` retires oversized output after the child
compiler exits. `preview` changes nothing. Size is observed allocated bytes,
deduplicated within the tree; it is not a promise of physically freed space when
hardlinks exist elsewhere. Limits apply between commands, not during compilation.

Only `build/` is disposable. The owner marker and last-use record stay in ROOT;
selected executables, source, logs and immutable receipts must stay outside it.
Unknown contents, missing ownership, redirected roots and changed custody refuse.
Existing roots are never implicitly claimed. Interrupted creation without its
complete records requires explicit owner recovery. This helper does not establish
quiescence for unrelated writers; bypassing the product lock is unsupported.
