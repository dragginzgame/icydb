//! Module: db::executor::projection::materialize::distinct
//! Responsibility: projected-row DISTINCT key storage and bounded windowing.
//! Does not own: row decoding, scalar expression evaluation, or page dispatch.
//! Boundary: consumes already-projected row views only.

use crate::{
    db::{
        executor::group::{GroupKey, KeyCanonicalError, StableHash, stable_hash_from_digest},
        executor::projection::materialize::row_view::RowView,
        query::plan::PageSpec,
    },
    error::InternalError,
    value::{Value, ValueHashWriter},
};
use std::collections::HashMap;

///
/// ProjectionDistinctWindow
///
/// ProjectionDistinctWindow carries projected-row DISTINCT paging after
/// structural projection. It lets the row projector skip OFFSET rows and stop
/// at the LIMIT horizon while preserving the existing projected-row DISTINCT
/// equality contract.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProjectionDistinctWindow {
    offset: usize,
    limit: Option<usize>,
}

#[cfg(feature = "sql")]
impl ProjectionDistinctWindow {
    pub(super) fn from_page(page: Option<&PageSpec>) -> Self {
        Self {
            offset: page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX)),
            limit: page.and_then(|page| {
                page.limit
                    .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
            }),
        }
    }

    const fn output_is_empty(self) -> bool {
        matches!(self.limit, Some(0))
    }

    fn output_capacity(self) -> usize {
        self.limit.unwrap_or(0)
    }

    fn stop_after_distinct_count(self) -> Option<usize> {
        self.limit.map(|limit| self.offset.saturating_add(limit))
    }
}

///
/// DistinctProjectionRowSet
///
/// DistinctProjectionRowSet stores canonical projected-row keys for SQL
/// `DISTINCT` without cloning every candidate row before duplicate rejection.
/// Common non-map projected rows are hashed and compared through borrowed
/// values; map-valued rows keep the owned canonicalization path so malformed-map
/// errors and canonical map semantics remain identical.
///

#[cfg(feature = "sql")]
struct DistinctProjectionRowSet {
    buckets: HashMap<StableHash, Vec<Value>>,
}

#[cfg(feature = "sql")]
impl DistinctProjectionRowSet {
    // Build one empty distinct key set for a single SQL projection page pass.
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    // Insert one projected row into the canonical distinct-key set. This keeps
    // the row borrowed through the duplicate check and only materializes an owned
    // canonical key when the row is actually new.
    fn insert_row(&mut self, row: &RowView<'_>) -> Result<bool, KeyCanonicalError> {
        if projected_row_requires_owned_canonical_lookup(row.values()) {
            return self.insert_row_with_owned_canonicalization(row);
        }

        let hash = stable_hash_projected_row(row)?;
        if self
            .buckets
            .get(&hash)
            .is_some_and(|bucket| bucket.iter().any(|key| projected_row_matches_key(row, key)))
        {
            return Ok(false);
        }

        self.buckets
            .entry(hash)
            .or_default()
            .push(canonical_projected_row_value(row)?);

        Ok(true)
    }

    // Preserve the previous fully-owned canonicalization path for map-valued
    // rows, where malformed duplicate map keys must still surface through the
    // existing grouped-key canonicalization error path.
    fn insert_row_with_owned_canonicalization(
        &mut self,
        row: &RowView<'_>,
    ) -> Result<bool, KeyCanonicalError> {
        let key = GroupKey::from_group_values(row.values().to_vec())?;
        let hash = key.hash();
        let canonical = key.into_canonical_value();
        let bucket = self.buckets.entry(hash).or_default();
        if bucket.iter().any(|existing| existing == &canonical) {
            return Ok(false);
        }

        bucket.push(canonical);

        Ok(true)
    }
}

// Materialize the owned canonical key stored for one newly accepted projected
// row. The caller keeps the original output row, so response values are not
// normalized or reordered by DISTINCT key storage.
#[cfg(feature = "sql")]
fn canonical_projected_row_value(row: &RowView<'_>) -> Result<Value, KeyCanonicalError> {
    GroupKey::from_group_values(row.values().to_vec()).map(GroupKey::into_canonical_value)
}

// Hash one projected row under the same virtual-list framing used by grouped
// keys without first allocating `Value::List(row.clone())`.
#[cfg(feature = "sql")]
fn stable_hash_projected_row(row: &RowView<'_>) -> Result<StableHash, KeyCanonicalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(row.values().len());
    for value in row.values() {
        hash_writer
            .write_list_value(value)
            .map_err(|err| KeyCanonicalError::HashingFailed {
                reason: err.display_with_class(),
            })?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

// Map values need the owned canonicalization fallback because map validation can
// reject malformed duplicate-key payloads and that error behavior is part of the
// existing DISTINCT contract.
#[cfg(feature = "sql")]
fn projected_row_requires_owned_canonical_lookup(row: &[Value]) -> bool {
    row.iter().any(value_requires_owned_canonical_lookup)
}

// Detect map values recursively so nested list payloads keep borrowed lookup
// unless they contain a map that requires owned validation.
#[cfg(feature = "sql")]
fn value_requires_owned_canonical_lookup(value: &Value) -> bool {
    match value {
        Value::Map(_) => true,
        Value::List(items) => items.iter().any(value_requires_owned_canonical_lookup),
        _ => false,
    }
}

// Compare one borrowed projected row against an owned canonical key already
// stored in the set. The stored key always has `Value::List` framing.
#[cfg(feature = "sql")]
fn projected_row_matches_key(row: &RowView<'_>, key: &Value) -> bool {
    let Value::List(key_values) = key else {
        return false;
    };
    if row.values().len() != key_values.len() {
        return false;
    }

    key_values
        .iter()
        .enumerate()
        .all(|(index, canonical)| value_matches_canonical_key(row.get(index), canonical))
}

// Compare a borrowed projected value against its canonical stored value without
// allocating a second full row key. Decimal values normalize for key equality;
// map values are intentionally routed through the owned fallback above.
#[cfg(feature = "sql")]
fn value_matches_canonical_key(value: &Value, canonical: &Value) -> bool {
    match (value, canonical) {
        (Value::Decimal(value), Value::Decimal(canonical)) => value.normalize() == *canonical,
        (Value::List(values), Value::List(canonical_values)) => {
            values.len() == canonical_values.len()
                && values
                    .iter()
                    .zip(canonical_values)
                    .all(|(value, canonical)| value_matches_canonical_key(value, canonical))
        }
        (Value::Map(_), _) => false,
        _ => value == canonical,
    }
}

///
/// DistinctProjectionAccumulator
///
/// DistinctProjectionAccumulator owns the projected-row DISTINCT set and
/// post-DISTINCT window state for one materialization pass. Callers feed rows
/// in final execution order and stop when `consider_row` returns false.
///

#[cfg(feature = "sql")]
struct DistinctProjectionAccumulator {
    distinct_rows: DistinctProjectionRowSet,
    output_rows: Vec<RowView<'static>>,
    window: ProjectionDistinctWindow,
    distinct_seen: usize,
}

#[cfg(feature = "sql")]
impl DistinctProjectionAccumulator {
    fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            distinct_rows: DistinctProjectionRowSet::new(),
            output_rows: Vec::with_capacity(window.output_capacity()),
            window,
            distinct_seen: 0,
        }
    }

    fn consider_row(
        &mut self,
        row: RowView<'static>,
        mut record_bounded_stop: impl FnMut(),
    ) -> Result<bool, InternalError> {
        let inserted = self
            .distinct_rows
            .insert_row(&row)
            .map_err(KeyCanonicalError::into_internal_error)?;
        if !inserted {
            return Ok(true);
        }

        let distinct_index = self.distinct_seen;
        self.distinct_seen = self.distinct_seen.saturating_add(1);
        if distinct_index >= self.window.offset {
            self.output_rows.push(row);
        }

        let Some(stop_after) = self.window.stop_after_distinct_count() else {
            return Ok(true);
        };
        if self.distinct_seen >= stop_after {
            record_bounded_stop();

            return Ok(false);
        }

        Ok(true)
    }

    fn into_rows(self) -> Vec<RowView<'static>> {
        self.output_rows
    }
}

#[cfg(feature = "sql")]
pub(super) fn collect_bounded_distinct_projected_rows<I>(
    window: ProjectionDistinctWindow,
    rows: impl IntoIterator<Item = I>,
    mut record_candidate_row: impl FnMut(),
    mut record_bounded_stop: impl FnMut(),
    mut project_row: impl FnMut(I) -> Result<RowView<'static>, InternalError>,
) -> Result<Vec<RowView<'static>>, InternalError> {
    if window.output_is_empty() {
        return Ok(Vec::new());
    }

    let mut accumulator = DistinctProjectionAccumulator::new(window);

    // Phase 1: project rows in final execution order and feed each projected
    // tuple into the DISTINCT/window accumulator. A bounded LIMIT can stop the
    // projector before later structural rows are decoded.
    for row in rows {
        let projected = project_row(row)?;
        record_candidate_row();

        if !accumulator.consider_row(projected, &mut record_bounded_stop)? {
            break;
        }
    }

    Ok(accumulator.into_rows())
}
