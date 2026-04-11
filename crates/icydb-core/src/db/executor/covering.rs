//! Module: db::executor::covering
//! Responsibility: shared covering-index decode helpers for executor fast paths.
//! Does not own: index scan selection, terminal semantics, or aggregate orchestration.
//! Boundary: executor lanes import covering component decode from this root instead of duplicating payload logic.

use crate::{
    db::{
        access::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec},
        cursor::IndexScanContinuationInput,
        data::DataKey,
        direction::Direction,
        executor::{
            read_row_presence_with_consistency_from_data_store,
            record_row_check_covering_candidate_seen, record_row_check_row_emitted,
        },
        index::IndexEntryExistenceWitness,
        predicate::MissingRowPolicy,
        query::plan::{CoveringExistingRowMode, CoveringProjectionOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
    types::Ulid,
    value::{Value, ValueTag},
};

const COVERING_BOOL_PAYLOAD_LEN: usize = 1;
const COVERING_U64_PAYLOAD_LEN: usize = 8;
const COVERING_ULID_PAYLOAD_LEN: usize = 16;
const COVERING_TEXT_ESCAPE_PREFIX: u8 = 0x00;
const COVERING_TEXT_TERMINATOR: u8 = 0x00;
const COVERING_TEXT_ESCAPED_ZERO: u8 = 0xFF;
const COVERING_I64_SIGN_BIT_BIAS: u64 = 1u64 << 63;

type CoveringComponentValues = Vec<Vec<u8>>;

pub(in crate::db::executor) type CoveringProjectionComponentRows =
    Vec<(DataKey, IndexEntryExistenceWitness, CoveringComponentValues)>;

// Build the canonical executor-owned covering mode for fast paths that still
// must verify row presence before trusting secondary/index-backed payloads.
pub(in crate::db::executor) const fn covering_requires_row_presence_check()
-> CoveringExistingRowMode {
    CoveringExistingRowMode::RequiresRowPresenceCheck
}

// Resolve one canonical scan direction for covering projections. Any contract
// that still owes primary-key reordering must consume the underlying index in
// ascending storage order before post-access reordering.
pub(in crate::db::executor) const fn covering_projection_scan_direction(
    order_contract: CoveringProjectionOrder,
) -> Direction {
    match order_contract {
        CoveringProjectionOrder::IndexOrder(direction) => direction,
        CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
    }
}

// Reapply the logical covering projection order after component decoding.
pub(in crate::db::executor) fn reorder_covering_projection_pairs<T>(
    order_contract: CoveringProjectionOrder,
    projected_pairs: &mut [(DataKey, T)],
) {
    match order_contract {
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
            projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
        }
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
            projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
        }
        CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
    }
}

// Resolve one covering projection component stream from one lowered
// index-prefix or index-range contract.
pub(in crate::db::executor) fn resolve_covering_projection_components_from_lowered_specs<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    index_range_specs: &[LoweredIndexRangeSpec],
    direction: Direction,
    limit: usize,
    component_indices: &[usize],
    mut resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&IndexModel) -> Result<StoreHandle, InternalError>,
{
    let continuation = IndexScanContinuationInput::new(None, direction);

    if let [spec] = index_prefix_specs {
        return resolve_covering_projection_components_for_index_bounds(
            resolve_store_for_index(spec.index())?,
            entity_tag,
            spec.index(),
            (spec.lower(), spec.upper()),
            continuation,
            limit,
            component_indices,
        );
    }
    if !index_prefix_specs.is_empty() {
        return Err(InternalError::query_executor_invariant(
            "covering projection index-prefix path requires one lowered prefix spec",
        ));
    }

    if let [spec] = index_range_specs {
        return resolve_covering_projection_components_for_index_bounds(
            resolve_store_for_index(spec.index())?,
            entity_tag,
            spec.index(),
            (spec.lower(), spec.upper()),
            continuation,
            limit,
            component_indices,
        );
    }
    if !index_range_specs.is_empty() {
        return Err(InternalError::query_executor_invariant(
            "covering projection index-range path requires one lowered range spec",
        ));
    }

    Err(InternalError::query_executor_invariant(
        "covering projection component scans require index-backed access paths",
    ))
}

// Resolve one bounded component stream from one lowered index-bounds contract.
fn resolve_covering_projection_components_for_index_bounds(
    store: StoreHandle,
    entity_tag: EntityTag,
    index: &IndexModel,
    bounds: (
        &std::ops::Bound<crate::db::index::RawIndexKey>,
        &std::ops::Bound<crate::db::index::RawIndexKey>,
    ),
    continuation: IndexScanContinuationInput<'_>,
    limit: usize,
    component_indices: &[usize],
) -> Result<CoveringProjectionComponentRows, InternalError> {
    store.with_index(|index_store| {
        index_store.resolve_data_values_with_components_in_raw_range_limited(
            entity_tag,
            index,
            bounds,
            continuation,
            limit,
            component_indices,
            None,
        )
    })
}

// Map one raw covering projection stream under the existing-row contract and
// let the caller decide how the admitted component bytes become terminal
// payloads.
pub(in crate::db::executor) fn map_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut map_components: F,
) -> Result<Option<Vec<(DataKey, T)>>, InternalError>
where
    F: FnMut(CoveringComponentValues) -> Result<Option<T>, InternalError>,
{
    store.with_data(|data| {
        let mut projected_pairs = Vec::with_capacity(raw_pairs.len());
        for (data_key, existence_witness, components) in raw_pairs {
            if existing_row_mode.uses_storage_existence_witness() {
                record_row_check_covering_candidate_seen();

                if existence_witness == IndexEntryExistenceWitness::Missing {
                    continue;
                }
            } else if existing_row_mode.requires_row_presence_check() {
                record_row_check_covering_candidate_seen();

                if !read_row_presence_with_consistency_from_data_store(
                    data,
                    &data_key,
                    consistency,
                )? {
                    continue;
                }
            }

            let Some(projected) = map_components(components)? else {
                return Ok(None);
            };
            projected_pairs.push((data_key, projected));
            if existing_row_mode.requires_row_presence_check()
                || existing_row_mode.uses_storage_existence_witness()
            {
                record_row_check_row_emitted();
            }
        }

        Ok(Some(projected_pairs))
    })
}

// Decode one canonical covering-index component payload into one runtime
// `Value`. Returning `Ok(None)` keeps unsupported component kinds fail-closed
// at the caller boundary instead of guessing a lossy decode here.
pub(in crate::db::executor) fn decode_covering_projection_component(
    component: &[u8],
) -> Result<Option<Value>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::bytes_covering_component_payload_empty());
    };

    if tag == ValueTag::Bool.to_u8() {
        return decode_covering_bool(payload);
    }
    if tag == ValueTag::Int.to_u8() {
        return decode_covering_i64(payload);
    }
    if tag == ValueTag::Uint.to_u8() {
        return decode_covering_u64(payload);
    }
    if tag == ValueTag::Text.to_u8() {
        return decode_covering_text(payload);
    }
    if tag == ValueTag::Ulid.to_u8() {
        return decode_covering_ulid(payload);
    }
    if tag == ValueTag::Unit.to_u8() {
        return Ok(Some(Value::Unit));
    }

    Ok(None)
}

// Decode one ordered component vector into runtime values while keeping
// unsupported component kinds fail-closed at the caller boundary.
fn decode_covering_projection_components(
    components: Vec<Vec<u8>>,
) -> Result<Option<Vec<Value>>, InternalError> {
    let mut decoded = Vec::with_capacity(components.len());
    for component in components {
        let Some(value) = decode_covering_projection_component(&component)? else {
            return Ok(None);
        };
        decoded.push(value);
    }

    Ok(Some(decoded))
}

// Decode one covering projection stream under the existing-row contract and
// let the caller map the decoded value vector into its terminal payload.
pub(in crate::db::executor) fn decode_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut map_decoded: F,
) -> Result<Option<Vec<(DataKey, T)>>, InternalError>
where
    F: FnMut(Vec<Value>) -> Result<T, InternalError>,
{
    map_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        |components| {
            let Some(decoded) = decode_covering_projection_components(components)? else {
                return Ok(None);
            };

            Ok(Some(map_decoded(decoded)?))
        },
    )
}

// Decode one single-component covering projection stream under the existing-row
// contract and let the caller map the decoded runtime value.
pub(in crate::db::executor) fn decode_single_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    invariant_message: &'static str,
    mut map_decoded: F,
) -> Result<Option<Vec<(DataKey, T)>>, InternalError>
where
    F: FnMut(Value) -> Result<T, InternalError>,
{
    decode_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        |decoded| {
            let mut decoded = decoded.into_iter();
            let Some(value) = decoded.next() else {
                return Err(InternalError::query_executor_invariant(invariant_message));
            };
            if decoded.next().is_some() {
                return Err(InternalError::query_executor_invariant(invariant_message));
            }

            map_decoded(value)
        },
    )
}

fn decode_covering_bool(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some(value) = payload.first() else {
        return Err(InternalError::bytes_covering_bool_payload_truncated());
    };
    if payload.len() != COVERING_BOOL_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length("bool"));
    }

    match *value {
        0 => Ok(Some(Value::Bool(false))),
        1 => Ok(Some(Value::Bool(true))),
        _ => Err(InternalError::bytes_covering_bool_payload_invalid_value()),
    }
}

fn decode_covering_i64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_U64_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length("int"));
    }

    let mut bytes = [0u8; COVERING_U64_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);
    let biased = u64::from_be_bytes(bytes);
    let unsigned = biased ^ COVERING_I64_SIGN_BIT_BIAS;
    let value = i64::from_be_bytes(unsigned.to_be_bytes());

    Ok(Some(Value::Int(value)))
}

fn decode_covering_u64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_U64_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length("uint"));
    }

    let mut bytes = [0u8; COVERING_U64_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Uint(u64::from_be_bytes(bytes))))
}

fn decode_covering_text(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let mut bytes = Vec::new();
    let mut i = 0usize;

    while i < payload.len() {
        let byte = payload[i];
        if byte != COVERING_TEXT_ESCAPE_PREFIX {
            bytes.push(byte);
            i = i.saturating_add(1);
            continue;
        }

        let Some(next) = payload.get(i.saturating_add(1)).copied() else {
            return Err(InternalError::bytes_covering_text_payload_invalid_terminator());
        };
        match next {
            COVERING_TEXT_TERMINATOR => {
                i = i.saturating_add(2);
                if i != payload.len() {
                    return Err(InternalError::bytes_covering_text_payload_trailing_bytes());
                }

                let text = String::from_utf8(bytes)
                    .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

                return Ok(Some(Value::Text(text)));
            }
            COVERING_TEXT_ESCAPED_ZERO => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::bytes_covering_text_payload_invalid_escape_byte());
            }
        }
    }

    Err(InternalError::bytes_covering_text_payload_missing_terminator())
}

fn decode_covering_ulid(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_ULID_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length("ulid"));
    }

    let mut bytes = [0u8; COVERING_ULID_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Ulid(Ulid::from_bytes(bytes))))
}
