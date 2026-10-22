# CI and release integrity

Apply the [common audit contract](README.md). Review the affected automation
when workflow events, permissions, dependencies, credentials, tool setup,
artifact promotion or release/recovery adapters change, or when explicitly
asked for a baseline. This is inspection guidance, not a new release gate.
Consumers own release identities, product artifacts, supported hosts and effects.

## Trace authority and inputs

Trace each selected workflow or local release entrypoint from its trigger to
its first protected effect. Include reusable workflows, local/composite actions,
Make targets, shell/Rust adapters and external tools that participate.

| Entrypoint / trigger | Input and actor trust | Effective permissions / credentials | Protected effect / admission | Evidence or gap |
| --- | --- | --- | --- | --- |

- Inspect effective job/token permissions, inherited secrets, environment
  approval and branch/review protections. A top-level `permissions` block alone
  does not prove the authority of every job or reusable workflow.
- Trace untrusted PR content, expressions, downloaded artifacts and cache inputs
  through checkout, command construction and execution. Check whether privileged
  `pull_request_target`, `workflow_run` or equivalent paths can execute that input
  with write tokens, production credentials or publication authority.
- Review immutable action/dependency selections and authenticated tool downloads
  against the [pinning rules](../rules/dependency-pinning.md). A pinned caller
  does not authenticate a later download. Cache reuse must retain the selected
  tool/source admission; a hit is not proof of trusted bytes.
- Verify explicit setup and offline admission, relevant failure propagation,
  early fail-closed Clippy and the consumer's release/recovery ordering under
  the [release contract](../docs/releases.md). Preserve consumer gate selections;
  passing CI alone does not prove release adapters use those same inputs/checks.
- Review credential scope, command output, raw failure logs, retained archives
  and publication inputs. Record redaction or custody gaps without copying secret
  values into the audit. An absence of search matches is not a credential review.

Use existing declaration checks and action lint for their actual assertions;
manual trust/effect traces remain necessary. Inspect commands before execution.
Do not install tools, dispatch CI or exercise release/publication/deployment
merely to obtain an audit result. Focused substitute tests require the ordinary
validation authority and must be labelled as substitutes.

## Follow artifacts through promotion

For each selected promoted artifact class, record source/tree and lock identity,
toolchain, target/features, relevant configuration, builder/tool identities,
artifact digest, admission step and final destination. Follow the bytes actually
uploaded or deployed, including package assembly, signing and promotion adapters.

A checksum establishes byte identity, not producer trust or source attribution.
Verify the claimed binding and applicable signature/attestation at the consumer;
an artifact name, successful build or release tag alone does not establish it.
Recovery must retain original intent, artifact bytes, authority and failed
observations as required by the consumer's contract; an uncertain paid/upload
result is not permission to repeat the effect.

Record the declared native OS/architecture and product-target matrix alongside
matching source-bound local/hosted evidence. Missing, skipped, cancelled or
pending required cells remain qualification gaps. Linux execution and compiler
cross-checks do not qualify native macOS behavior. Follow the
[host policy](../docs/supported-hosts.md); do not invent support exclusions.
Build reproducibility, runtime correctness and measured footprint retain their
owning methods; release metadata or hash equality cannot substitute for them.

## Findings and proof

Return the authority/input trace, artifact-to-destination evidence, applicable
host coverage and any missing proof using common findings and verdicts. Keep one
owning finding per cause and reuse only evidence with matching source and scope.

Useful focused proof covers a valid admitted path and rejection of an applicable
untrusted input, failed prerequisite, unchecked artifact or interrupted recovery.
Name the assertion and its execution identity. A source-only conclusion, missing
required proof and a demonstrated bypass are different outcomes. Existing
credentials, live effects and a complete release are not audit prerequisites.
