# Hashing choices and ownership
<!-- Shared companions: ci/hash-dependencies.json -->

Shared owns the approved algorithm/purpose inventory; consumers own their bytes,
domain separation, budgets and versioned formats. A common import facade is not
required. Use root workspace dependency declarations and inherit them in members.

This replaces the earlier recommendation to route pure BLAKE3 hashing through
`ic-platform`. Centralize algorithm decisions and their enforcement in Shared
Tooling; keep pure implementation dependencies with their consumers. The purpose
is to prevent unnoticed algorithm proliferation without coupling unrelated
consumers to Platform releases. This policy does not require existing consumers
to switch algorithms.

| Algorithm | Approved purpose observed in the sibling audit |
| --- | --- |
| SHA-256 (`sha2`) | Existing content digests, artifact identities, schema/continuation identities and protocol commitments across Auth, Backup, Blob, Canic, Tooling and IcyDB. Default for new cryptographic content digests. |
| SHA-224 (`sha2`) | Established IC Principal/account derivation at Platform Types; not a second general application default. |
| BLAKE3 (`blake3`) | Existing IcyDB predicate fingerprints, SQL generator/reference and integration qualification data. Preserve these algorithms and bytes when changing imports. |
| XXH3 (`xxhash-rust`) | Existing IcyDB non-security value hashing. Never use it as a cryptographic integrity, authentication or adversarial-collision boundary. |
| HMAC-SHA-256 (`hmac`, `sha2`) | Existing keyed diagnostic fingerprints in Canic Host. Preserve key custody and domain separation. |
| CRC32 (`crc32fast`) | Existing external Principal/account checksum formats. Not cryptographic integrity or authentication. |

This is the 2026-11-07 source audit baseline, not permission to expand every
algorithm to arbitrary purposes. Before adding an algorithm, hash implementation
package, or new purpose, obtain explicit maintainer agreement and update this
shared inventory with the reason, owner and relevant tests. Prefer an existing
approved choice. Different crate versions of SHA-256 are not different algorithms;
do not require coordinated releases just to align harmless implementation versions.

Pure hashing may depend directly on the approved implementation. In particular,
IcyDB may replace Platform's BLAKE3 re-export with the same upstream version and
features. This exception does not move IC signatures, key custody, certification,
Candid/CBOR, external protocol adapters or runtime operations out of Platform.
Do not introduce a wrapper crate for each hashing dependency.

When retiring an existing facade, adopt this policy and its checker first,
migrate consumers while preserving outputs, then remove the unused facade.
Keep those source changes in their owning repositories; adopting this policy
alone does not mean the sibling migrations have been completed.

The existing Cargo boundary check with `--external-ic` loads
`ci/hash-dependencies.json`. It refuses known review-required direct hashing or
general crypto packages, including aliases, optional, target, build and dev
dependencies. Consumer allowlists cannot relax that common decision, including
inside Platform. Approved packages still obey stricter consumer ownership rules.
Transitive packages used by TLS, signature implementations or upstream protocols
are not automatically violations and do not require replacement with SHA-256.

The inventory is a known-package guard, not universal algorithm discovery. It
does not identify unknown package names, algorithms selected inside `sha2`,
browser/WebCrypto choices, copied code or generic hashing through `std::hash`.
Review dependency and source diffs for those cases, and explicitly select every
independent Cargo workspace when checking. Ordinary HashMap hashing is distinct
from a stored or transmitted identity. New direct general crypto dependencies
require review even when only one approved hash is intended.

Changing an algorithm, domain tag, seed, canonical input encoding or digest length
is a separate contract change. Preserve golden vectors and review persisted data,
signatures, cache keys and cross-language consumers before migrating it. Moving
an import must retain existing outputs and bounds; never silently rehash stored
or protocol identities to simplify the dependency graph.
