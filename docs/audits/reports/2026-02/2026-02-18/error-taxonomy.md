# Error Taxonomy Audit - 2026-02-18

Scope: classification integrity and upward error-domain preservation in `icydb-core`.
Rerun status: re-checked against current working tree; findings unchanged.

## STEP 0 - Semantic Domain Definitions Used

- `Corruption`: persisted bytes/state invalid or decode of trusted storage fails.
- `Unsupported`: intentionally unsupported value/feature/operation.
- `Invalid Input`: malformed cursor/query/user-supplied value.
- `Invariant Violation`: internal contract/state assumption break.
- `System Failure`: runtime/internal failure not attributable to user input or persisted corruption.

## STEP 1 - Full Error Enumeration

| Enum | Variant | Declared Meaning | Layer |
| ---- | ---- | ---- | ---- |
| `InternalError` (struct) | `class`, `origin`, `message`, `detail` | Runtime classified error carrier | cross-layer |
| `ErrorClass` | `Corruption`, `NotFound`, `Internal`, `Conflict`, `Unsupported`, `InvariantViolation` | Internal class taxonomy | global |
| `ErrorOrigin` | `Serialize`, `Store`, `Index`, `Query`, `Response`, `Executor`, `Interface` | Internal origin taxonomy | global |
| `StoreError` | `NotFound`, `Corrupt`, `InvariantViolation` | Store-specific detail payload | global/store |
| `PlanError` | `PredicateInvalid`, `UnknownOrderField`, `UnorderableField`, `IndexNotFound`, `IndexPrefixTooLong`, `IndexPrefixEmpty`, `IndexPrefixValueMismatch`, `PrimaryKeyNotKeyable`, `PrimaryKeyMismatch`, `InvalidKeyRange`, `EmptyOrderSpec`, `MissingPrimaryKeyTieBreak`, `DeletePlanWithPagination`, `LoadPlanWithDeleteLimit`, `DeleteLimitRequiresOrder`, `UnorderedPagination`, `CursorRequiresOrder`, `InvalidContinuationCursor`, `InvalidContinuationCursorPayload`, `ContinuationCursorVersionMismatch`, `ContinuationCursorSignatureMismatch`, `ContinuationCursorBoundaryArityMismatch`, `ContinuationCursorBoundaryTypeMismatch`, `ContinuationCursorPrimaryKeyTypeMismatch` | Plan validation/cursor semantics | query/plan |
| `QueryError` | `Validate`, `Plan`, `Intent`, `Response`, `Execute` | Public query boundary wrapper | query/intent |
| `CursorDecodeError` | `Empty`, `OddLength`, `InvalidHex` | Hex cursor token decode failures | cursor |
| `IdentityDecodeError` | `InvalidSize`, `InvalidLength`, `NonAscii`, `NonZeroPadding` | Identity decode/storage-shape failures | identity/store decode |
| `SerializeError` | `Serialize`, `Deserialize` | Generic CBOR format failures | serialize |
| `StoreRegistryError` | `StoreNotFound`, `StoreAlreadyRegistered` | Registry consistency errors | store registry |
| `StorageKeyEncodeError` | `AccountOwnerTooLarge`, `AccountLengthMismatch`, `UnsupportedValueKind`, `PrincipalTooLarge` | Storage-key encoding failures | data/serialize |
| `DataKeyEncodeError` | `KeyEncoding` | Data-key encode failure wrapper | data |
| `KeyDecodeError` | `InvalidEncoding` | Primary-key decode failure | data |
| `DataKeyDecodeError` | `Entity`, `Key` | Raw data-key decode failure | data/store |
| `RawRowError` | `TooLarge` | Raw row size policy failure | data/store |
| `RowDecodeError` | `Deserialize` | Row decode wrapper | data/serialize |
| `ExecutorError` | `Corruption`, `KeyExists` | Executor corruption/conflict surface | executor |
| `IndexEntryCorruption` | `TooLarge`, `MissingLength`, `TooManyKeys`, `LengthMismatch`, `InvalidKey`, `DuplicateKey`, `EmptyEntry`, `NonUniqueEntry`, `MissingKey`, `RowKeyMismatch` | Index-entry corruption detection | index |
| `IndexEntryEncodeError` | `TooManyKeys`, `KeyEncoding` | Index-entry encode failures | index |
| `OrderedValueEncodeError` | `NullNotIndexable`, `UnsupportedValueKind`, `SegmentTooLarge`, `InvalidSignedDecimal`, `InvalidUnsignedDecimal`, `DecimalExponentOverflow` | Canonical index component encode failures | index |
| `IndexRangeBoundEncodeError` | `Prefix`, `Lower`, `Upper` | Range-bound encoding failures | index/range |
| `RelationTargetRawKeyError` | `StorageKeyEncode`, `TargetEntityName` | Strong-relation raw-key normalization failures | relation |
| `PlannerError` | `Plan`, `Internal` | Planner wrapper surface | planner |
| `IntentError` | `PlanShape`, `ByIdsWithPredicate`, `OnlyWithPredicate`, `KeyAccessConflict`, `CursorRequiresOrder`, `CursorRequiresLimit`, `CursorWithOffsetUnsupported` | Intent/query composition failures | query/intent |
| `ResponseError` | `NotFound`, `NotUnique` | Cardinality expectation failures | response |
| `PlanPolicyError` | `EmptyOrderSpec`, `DeletePlanWithPagination`, `LoadPlanWithDeleteLimit`, `DeleteLimitRequiresOrder`, `UnorderedPagination` | Plan-shape policy failures | query/policy |
| `CursorPagingPolicyError` | `CursorRequiresOrder`, `CursorRequiresLimit`, `CursorWithOffsetUnsupported` | Cursor paging policy failures | query/policy |
| `CursorOrderPolicyError` | `CursorRequiresOrder` | Cursor-order precondition failure | query/policy |
| `SortLowerError` | `Validate`, `Plan` | Sort-lowering wrapper | query/expr |
| `ValidateError` | `InvalidEntityName`, `InvalidIndexName`, `UnknownField`, `NonQueryableFieldType`, `DuplicateField`, `UnsupportedQueryFeature`, `InvalidPrimaryKey`, `InvalidPrimaryKeyType`, `IndexFieldUnknown`, `IndexFieldNotQueryable`, `IndexFieldMapNotQueryable`, `IndexFieldDuplicate`, `DuplicateIndexName`, `InvalidOperator`, `InvalidCoercion`, `InvalidLiteral` | Predicate/schema validation failures | query/predicate |
| `ContinuationTokenError` | `Encode`, `Decode`, `UnsupportedVersion` | Continuation token wire encode/decode failures | query/plan/continuation |
| Commit marker errors | no dedicated enum; emitted as `InternalError` at `commit/store.rs`, `commit/decode.rs`, `commit/prepare.rs`, `commit/validate.rs` | Commit-marker corruption/shape enforcement | commit |
| Recovery errors | no dedicated enum; emitted as `InternalError` in `commit/recovery.rs` | Replay/rebuild failures preserving class+origin | commit/recovery |

## STEP 2 - Per-Variant Semantic Classification

| Variant | Semantic Domain | Justification |
| ---- | ---- | ---- |
| `CursorDecodeError::{Empty,OddLength,InvalidHex}` | Invalid Input | Direct malformed client token input (`db/cursor.rs:13`). |
| `PlanError::{InvalidContinuationCursor,InvalidContinuationCursorPayload,ContinuationCursorVersionMismatch,ContinuationCursorSignatureMismatch,ContinuationCursorBoundaryArityMismatch,ContinuationCursorBoundaryTypeMismatch,ContinuationCursorPrimaryKeyTypeMismatch,CursorRequiresOrder}` | Invalid Input | Cursor payload/shape/order compatibility failures from client-supplied continuation token (`query/plan/validate/mod.rs:126`). |
| `PlanError::{UnknownOrderField,UnorderableField,IndexNotFound,IndexPrefixTooLong,IndexPrefixEmpty,IndexPrefixValueMismatch,PrimaryKeyNotKeyable,PrimaryKeyMismatch,InvalidKeyRange,EmptyOrderSpec,MissingPrimaryKeyTieBreak,DeletePlanWithPagination,LoadPlanWithDeleteLimit,DeleteLimitRequiresOrder,UnorderedPagination}` | Invalid Input | Invalid query/plan shape or invalid order/index/query constraints. |
| `PlanError::PredicateInvalid` | Mixed (`Invalid Input` + `Invariant Violation`) | Wraps `ValidateError`, which includes user predicate errors and model-contract failures (`predicate/validate/schema.rs:138`). |
| `ValidateError::{UnknownField,NonQueryableFieldType,DuplicateField,InvalidOperator,InvalidCoercion,InvalidLiteral}` | Invalid Input | Query predicate mismatches schema or literal types. |
| `ValidateError::{InvalidEntityName,InvalidIndexName,InvalidPrimaryKey,InvalidPrimaryKeyType,IndexFieldUnknown,IndexFieldNotQueryable,IndexFieldMapNotQueryable,IndexFieldDuplicate,DuplicateIndexName}` | Invariant Violation | Model/schema contract invalid at planning boundary. |
| `ValidateError::UnsupportedQueryFeature` + `UnsupportedQueryFeature::MapPredicate` | Unsupported | Explicit policy fence for map predicates (`predicate/ast.rs:247`). |
| `IdentityDecodeError::{InvalidSize,InvalidLength,NonAscii,NonZeroPadding}` | Corruption | Storage decode boundary for fixed-format identity bytes (`identity.rs:26`). |
| `SerializeError::{Serialize,Deserialize}` | System Failure (generic layer) | Raw format-level failure; caller decides corruption vs internal (`serialize/mod.rs:19`). |
| `StoreRegistryError::StoreNotFound` | Invariant Violation | Missing registered store is a runtime configuration contract failure (`registry.rs:22`). |
| `StoreRegistryError::StoreAlreadyRegistered` | Invariant Violation | Duplicate registration breaks registry invariant (`registry.rs:25`). |
| `StorageKeyEncodeError::*` / `DataKeyEncodeError::KeyEncoding` / `RawRowError::TooLarge` / `OrderedValueEncodeError::*` / `IndexRangeBoundEncodeError::*` / `RelationTargetRawKeyError::StorageKeyEncode` | Unsupported | Value cannot be represented by storage/index encoding policy. |
| `KeyDecodeError::InvalidEncoding` / `DataKeyDecodeError::{Entity,Key}` / `RowDecodeError::Deserialize` / `IndexEntryCorruption::*` / `ExecutorError::Corruption` | Corruption | Persisted bytes or store/index relationships fail trusted decode/invariant checks. |
| `ExecutorError::KeyExists` | Invalid Input | Mutation conflicts with existing state (`executor/mod.rs:45`) and maps to conflict class. |
| `ErrorClass::Internal` | System Failure | Generic internal/runtime failures. |
| `ErrorClass::{NotFound,Conflict}` | Invalid Input (operation-level) | User operation/expectation mismatch with current state. |
| `IntentError::{PlanShape,ByIdsWithPredicate,OnlyWithPredicate,KeyAccessConflict,CursorRequiresOrder,CursorRequiresLimit,CursorWithOffsetUnsupported}` | Invalid Input | Query construction/policy failures (`intent/mod.rs:593`). |
| `ResponseError::{NotFound,NotUnique}` | Invalid Input | Caller requested cardinality unmet (`response/mod.rs:27`). |
| `ContinuationTokenError::{Decode,UnsupportedVersion}` | Invalid Input | Malformed/incompatible token wire format (`plan/continuation.rs:283`). |
| `ContinuationTokenError::Encode` | System Failure | Server-side encode failure while creating cursor token. |
| `Commit marker/recovery InternalError` (majority) | Corruption / Invariant Violation / Unsupported / System Failure | Domain chosen by callsite class+origin (`commit/*.rs`, `commit/recovery.rs`). |

Flagged mixed-domain pressure:
- `PlanError` mixes invalid input and schema/model invariant categories (`query/plan/validate/mod.rs:56`).
- `ValidateError` mixes user-input validation and schema-contract failures (`predicate/validate/schema.rs:138`).

## STEP 3 - Upward Mapping Verification

| Source Variant | Mapped To | Domain Preserved? | Escalation? | Downgrade? | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `CursorDecodeError::*` | `PlanError::InvalidContinuationCursor` -> `QueryError::Plan` (`db/mod.rs:434`, `query/plan/validate/mod.rs:127`) | Yes | No | No | Low |
| `ContinuationTokenError::{Decode,Encode}` | `PlanError::InvalidContinuationCursorPayload` (`plan/continuation.rs:390`) -> `QueryError::Plan` | Yes | No | No | Low |
| `ContinuationTokenError::UnsupportedVersion` | `PlanError::ContinuationCursorVersionMismatch` (`plan/continuation.rs:394`) -> `QueryError::Plan` | Yes | No | No | Low |
| `ValidateError::*` | `PlanError::PredicateInvalid` (`plan/validate/mod.rs:57`) -> `QueryError::Plan` (`intent/mod.rs:583`) | Partially | No | No | Medium |
| `StorageKeyEncodeError` | `DataKeyEncodeError::KeyEncoding` -> `InternalError(Unsupported,Serialize)` (`data/key.rs:31`) -> `QueryError::Execute` | Yes | No | No | Low |
| `DataKeyDecodeError` on trusted bytes | `InternalError(Corruption,Store)` at executor/commit/recovery (`executor/context.rs:412`, `commit/prepare.rs:41`, `commit/recovery.rs:171`) | Yes | No | No | Low |
| `StoreRegistryError::*` | `InternalError(class(), Store)` (`registry.rs:30`) -> `QueryError::Execute` | Yes | No | No | Low |
| `ExecutorError::KeyExists` | `InternalError(Conflict,Store)` (`executor/mod.rs:49`) -> `QueryError::Execute` | Yes | No | No | Low |
| `SerializeError` in DB row decode | remapped to `InternalError(Corruption,Serialize)` (`db/codec.rs:37`) | Yes (trusted-persisted context) | No | No | Low |
| Recovery-prepared op errors | `InternalError` class+origin preserved unchanged (`commit/recovery.rs:190-193`) | Yes | No | No | Low |

## STEP 4 - Corruption Containment Audit

| Corruption Variant | Public Classification | Origin | Correct? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Commit marker index key/entry decode failures (`commit/decode.rs:15`, `commit/decode.rs:37`) | `InternalError(Corruption)` | `Index` | Yes | Low |
| Commit marker data key decode failures (`commit/decode.rs:59`) | `InternalError(Corruption)` | `Store` | Yes | Low |
| Commit row-op malformed shape (`commit/validate.rs:14`) | `InternalError(Corruption)` | `Store` | Yes | Low |
| Startup rebuild invalid data key (`commit/recovery.rs:171`) | `InternalError(Corruption)` | `Store` | Yes | Low |
| Trusted row deserialize failure (`db/codec.rs:37`) | `InternalError(Corruption)` | `Serialize` | Yes | Low |
| Index-key corruption during lookup (`index/store/lookup.rs:201`) | `InternalError(Corruption)` | `Index` | Yes | Low |
| Data-key decode in executor context (`executor/context.rs:412`) | `InternalError(Corruption)` | `Store` | Yes | Low |
| Persisted commit marker exceeds max bytes on load (`commit/store.rs:57`) | `InternalError(InvariantViolation)` | `Store` | No (domain ambiguity) | Medium |

Containment result:
- No observed path where malformed cursor/user token is mislabeled as corruption.
- Corruption is generally preserved and not downgraded in query execution wrappers.

## STEP 5 - Invalid Input Containment Audit

| Invalid Input Variant | Final Classification | Correct? | Risk |
| ---- | ---- | ---- | ---- |
| `CursorDecodeError::*` -> `PlanError::InvalidContinuationCursor` (`db/mod.rs:434`) | `QueryError::Plan` (invalid continuation cursor) | Yes | Low |
| Cursor paging precondition failures (`session/load.rs:272`) -> `IntentError::{CursorRequiresOrder,CursorRequiresLimit,CursorWithOffsetUnsupported}` | `QueryError::Intent` | Yes | Low |
| Query composition conflicts (`IntentError::{ByIdsWithPredicate,OnlyWithPredicate,KeyAccessConflict}`) | `QueryError::Intent` | Yes | Low |
| Query shape violations (`PlanError::{UnorderedPagination,DeleteLimitRequiresOrder,...}`) | `QueryError::Plan` | Yes | Low |
| `ResponseError::{NotFound,NotUnique}` | `QueryError::Response` | Yes (operation-level invalid expectation) | Low |
| Identity decode from untrusted source | No active untrusted-source path observed; identity decode is storage boundary in current code | N/A | Low |

## STEP 6 - Invariant Violation Audit

| Invariant Variant | Propagation Path | Classification Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| Executor defensive plan validation failures (`validate_executor_plan`) | `InternalError(InvariantViolation,Query)` (`plan/validate/mod.rs:221`) -> `QueryError::Execute` | Yes | Low |
| Registry duplicate store registration | `StoreRegistryError::StoreAlreadyRegistered` -> `InternalError(InvariantViolation,Store)` (`registry.rs:25`) | Yes | Low |
| Duplicate runtime hooks for entity | `InternalError(InvariantViolation,Store)` (`db/mod.rs:223`) | Yes | Low |
| Commit marker oversize-on-load | `InternalError(InvariantViolation,Store)` (`commit/store.rs:57`) | Preserved, but domain likely closer to corruption | Medium |

## STEP 7 - Origin Fidelity Audit

| Variant | True Origin | Reported Origin | Match? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Row deserialize failure in load path | Serialize | Serialize (`executor/context.rs:427`) | Yes | Low |
| Data-key decode failure in load path | Store | Store (`executor/context.rs:412`) | Yes | Low |
| Index-key decode failure in lookup | Index | Index (`index/store/lookup.rs:201`) | Yes | Low |
| Cursor malformed hex | Cursor/query-input boundary | PlanError (no `ErrorOrigin`) | Partial (origin not carried) | Medium |
| Cursor payload mismatch (anchor/index-id/arity) | Query/cursor boundary | PlanError (no `ErrorOrigin`) | Partial (origin not carried) | Medium |
| Merge patch failures | Interface | Interface (`error.rs:113`) | Yes | Low |
| Recovery rewrapped prepare failure | Preserved source | Preserved (`commit/recovery.rs:190-193`) | Yes | Low |

## STEP 8 - Layer Violation Detection

| Violation | Location | Classification Impact | Risk |
| ---- | ---- | ---- | ---- |
| Planner wrapper can carry lower-layer `InternalError` directly (`PlannerError::Internal`) | `query/plan/planner.rs:27` | No immediate domain break; broadens planner boundary surface | Medium |
| Cursor/plan errors do not carry `ErrorOrigin` | `PlanError` family (`query/plan/validate/mod.rs:56`) | Domain preserved, origin fidelity reduced at public plan boundary | Medium |
| Serialize decode failures can map to different classes by callsite | generic `SerializeError` (`serialize/mod.rs:27`) vs DB persisted-row wrapper (`db/codec.rs:37`) | Context-dependent mapping is intentional; requires discipline | Low |
| Unknown entity name/path in persisted data paths labeled `Unsupported` | `db/mod.rs:135`, `db/mod.rs:233` | Domain ambiguity between unsupported-compatibility vs corruption | Medium |

## STEP 9 - Cross-Path Consistency

| Scenario | Normal Classification | Replay Classification | Consistent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Malformed cursor token hex | `QueryError::Plan(InvalidContinuationCursor)` (`db/mod.rs:434`) | N/A | Yes (single path) | Low |
| Commit marker malformed data key | `InternalError(Corruption,Store)` in prepare (`commit/prepare.rs:41`) | `InternalError(Corruption,Store)` in replay (`commit/recovery.rs:171`) | Yes | Low |
| Persisted row deserialize failure | `InternalError(Corruption,Serialize)` in load (`executor/context.rs:427`) | `InternalError(Corruption,Serialize)` via commit prepare decode path (`commit/prepare.rs:56`) | Yes | Low |
| Unique/index conflict in save vs replace | `InternalError(Conflict,Index)` (`index/plan/mod.rs:45`) | N/A (replay reuses prepared ops; preserves class/origin when errors surface) | Yes | Low |
| Unknown entity in commit row op | `InternalError(Unsupported,Store)` (`db/mod.rs:135`) | `InternalError(Unsupported,Store)` through runtime hook lookup (`db/mod.rs:233`) | Yes | Medium |

## STEP 10 - Mixed-Domain Enum Detection

| Enum | Mixed Domains? | Risk |
| ---- | ---- | ---- |
| `ErrorClass` | Yes (`Corruption`, `Unsupported`, `InvariantViolation`, `Internal`, `Conflict`, `NotFound`) | Medium (expected root taxonomy) |
| `PlanError` | Yes (invalid input + unsupported/model-contract pressure through `PredicateInvalid`) | Medium |
| `ValidateError` | Yes (invalid input + model-contract invariant issues + unsupported feature) | Medium |
| `QueryError` | Yes (wrapper over validate/plan/intent/response/internal execute domains) | Medium |
| `IntentError` | No (invalid input/policy domain) | Low |
| `CursorDecodeError` | No (invalid input only) | Low |

## STEP 11 - Incorrect Classification List

1. Commit marker oversize at decode/load boundary is classified as `InvariantViolation` instead of corruption-like persisted-state invalidity (`commit/store.rs:57`). Risk: Medium.
2. Unknown entity path/name during commit/recovery hook lookup is classified as `Unsupported`; this is defensible for compatibility but overlaps with persisted-state invalidity semantics (`db/mod.rs:135`, `db/mod.rs:233`). Risk: Medium.
3. Cursor-origin fidelity is not expressible in `ErrorOrigin` because cursor failures are represented as `PlanError` without origin channel (`query/plan/validate/mod.rs:126`). Risk: Medium-Low.

No direct corruption downgrades to invalid input were found.
No invalid-input escalations to corruption were found in cursor/query paths.

## STEP 12 - Error Classification Matrix

| Variant | Layer | Domain | Origin | Final Public Classification | Correct? |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `CursorDecodeError::InvalidHex` | cursor | Invalid Input | N/A (PlanError path) | `QueryError::Plan(InvalidContinuationCursor)` | Yes |
| `PlanError::InvalidContinuationCursorPayload` | query/plan | Invalid Input | N/A | `QueryError::Plan` | Yes |
| `PlanError::ContinuationCursorVersionMismatch` | query/plan | Invalid Input | N/A | `QueryError::Plan` | Yes |
| `PlanError::UnorderedPagination` | query/plan | Invalid Input | N/A | `QueryError::Plan` | Yes |
| `ValidateError::InvalidLiteral` | query/predicate | Invalid Input | N/A | `QueryError::Validate` or wrapped in `PlanError::PredicateInvalid` | Yes |
| `ValidateError::InvalidPrimaryKeyType` | query/predicate | Invariant Violation | N/A | wrapped in `PlanError::PredicateInvalid` -> `QueryError::Plan` | Mostly |
| `IntentError::CursorWithOffsetUnsupported` | query/intent | Invalid Input | N/A | `QueryError::Intent` | Yes |
| `ResponseError::NotUnique` | response | Invalid Input | `Response` layer | `QueryError::Response` | Yes |
| `StoreRegistryError::StoreAlreadyRegistered` | store | Invariant Violation | Store | `InternalError(InvariantViolation,Store)` | Yes |
| `StoreRegistryError::StoreNotFound` | store | Invariant Violation | Store | `InternalError(Internal,Store)` | Mostly |
| `StorageKeyEncodeError::UnsupportedValueKind` | data/serialize | Unsupported | Serialize | `InternalError(Unsupported,Serialize)` | Yes |
| `DataKeyDecodeError::Entity` | data/store | Corruption | Store | `InternalError(Corruption,Store)` at callsites | Yes |
| `RawRowError::TooLarge` | data/store | Unsupported | Store | `InternalError(Unsupported,Store)` | Yes |
| `ExecutorError::KeyExists` | executor | Invalid Input | Store | `InternalError(Conflict,Store)` -> `QueryError::Execute` | Yes |
| `ExecutorError::Corruption` | executor | Corruption | caller-supplied | `InternalError(Corruption,origin)` | Yes |
| `IndexEntryCorruption::DuplicateKey` | index | Corruption | Index | `InternalError(Corruption,Index)` at wrappers | Yes |
| `IndexEntryEncodeError::TooManyKeys` | index | Unsupported | Index/Serialize boundary | usually `InternalError(Unsupported,Index/Serialize)` | Yes |
| `OrderedValueEncodeError::NullNotIndexable` | index | Unsupported | Index | `InternalError(Unsupported,Index)` | Yes |
| `IndexRangeBoundEncodeError::Lower` | index/range | Unsupported | Query/Index context | `PlanError::InvalidContinuationCursorPayload` or `InternalError(Unsupported,Index)` | Yes |
| Commit marker bad index key length | commit/decode | Corruption | Index | `InternalError(Corruption,Index)` | Yes |
| Commit marker bad row-op shape | commit/validate | Corruption | Store | `InternalError(Corruption,Store)` | Yes |
| Commit marker oversized payload on load | commit/store | Corruption-like | Store | `InternalError(InvariantViolation,Store)` | No (domain ambiguity) |
| Recovery rebuild `DataKey` decode failure | commit/recovery | Corruption | Store | `InternalError(Corruption,Store)` | Yes |
| Recovery wrapped prepare failure | commit/recovery | preserved source domain | preserved source | `InternalError(err.class, err.origin, ..)` | Yes |

## STEP 13 - Overall Taxonomy Risk Index

Taxonomy Risk Index (1–10, lower is better): **4/10**

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

Rationale:
- Domain preservation is strong across cursor, query, execute, commit, and recovery mapping paths.
- Corruption containment is mostly consistent and not downgraded to invalid input.
- Main pressure points are domain ambiguity in a small number of commit/store compatibility cases and limited origin fidelity for plan-level cursor failures.
