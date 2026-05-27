# 0.165 Execution Handoff Residual Naming

## Status

Complete.

## Accepted Renames

### Private Execution Handoff Unpackers

Role proof:

- Owning modules: executor pipeline contracts, grouped fold runtime, index
  access scanning, SQL compile-cache context, structural delete projection, and
  structural query intent
- Payload: private handoff values whose consumers need named runtime fields,
  cache inputs, cursor rows, scan anchors, or query state
- Main consumers: scalar entrypoints, grouped fold reducers, DISTINCT stream
  decoration, physical index scans, SQL compile-cache lookup, delete projection
  materialization, and structural query transformations
- Chosen family: explicit role-and-field vocabulary
- Rejected alternatives:
  - `into_parts` / `from_parts`: too weak because these helpers expose specific
    runtime fields or cache/query inputs rather than arbitrary decomposition
  - `parts_mut`: too weak because grouped fold callers borrow exactly the fold
    inputs required by the reducer loops
  - `*Components`: still does not name the execution/cache/cursor role
- Public-surface impact: none; public response DTO `into_parts` methods are
  intentionally left for a separate public-surface decision
- Hard-cut rule: remove the old private helper names from live execution,
  cache, scan, delete, and structural query code

Accepted renames:

```text
ResolvedExecutionKeyStream::into_parts() -> into_stream_resolution_fields()
StructuralCursorPage::into_parts() -> into_data_rows_and_cursor()
GroupedStreamStage::parts_mut() -> fold_inputs_mut()
```

Additional accepted private helper renames:

```text
IndexDecodedKeyScanChunk::into_parts() -> into_decoded_keys_and_resume_anchor()
SqlCompiledCommandCacheContext::into_parts() -> into_cache_inputs()
DeleteProjection::into_parts() -> into_rows_and_count()
```

## Kept Names

### Public Response `into_parts`

Public response DTO methods such as `Row::into_parts`,
`ProjectedRow::into_parts`, and paged/grouped response `into_parts` methods are
kept in this slice. They are public facade helpers and require a separate
public-surface rename decision before any hard cut.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "resolved\\.into_parts\\(|page\\.into_parts\\(|stream\\.parts_mut\\(|chunk\\.into_parts\\(|context\\.into_parts\\(|Self::from_parts\\(|row\\.into_parts\\(\\?\\)|deleted\\.into_parts\\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/session crates/icydb-core/src/db/query
rg -n "into_stream_resolution_fields|into_data_rows_and_cursor|fold_inputs_mut|into_decoded_keys_and_resume_anchor|into_cache_inputs|from_intent_and_access_requirements|into_data_row_and_slots|into_rows_and_count" crates/icydb-core/src/db
```

Remaining old-name hits are allowed only in this family note, changelog
history, or public response DTOs explicitly kept for a separate public-surface
decision.
