use crate::{
    db::data::structural_field::{
        decode_list_field_items, decode_list_item, decode_map_entry, decode_map_field_entries,
        encode_list_field_owned_items, encode_map_field_owned_entries, encode_owned_list_item,
        encode_owned_map_entry,
    },
    error::InternalError,
    model::field::FieldKind,
};
use std::collections::BTreeMap;

// Resolve list/set by-kind metadata once so encode and decode traversal helpers
// do not each own parallel FieldKind validation.
fn resolve_collection_item_kind(
    kind: FieldKind,
    field_name: &'static str,
    encode: bool,
) -> Result<FieldKind, InternalError> {
    collection_item_kind(kind).ok_or_else(|| {
        let message = format!("field kind {kind:?} does not accept collection payloads");
        if encode {
            InternalError::persisted_row_field_encode_failed(field_name, message)
        } else {
            InternalError::persisted_row_field_decode_failed(field_name, message)
        }
    })
}

// Resolve map by-kind metadata once so encode and decode traversal helpers do
// not each own parallel FieldKind validation.
fn resolve_map_entry_kinds(
    kind: FieldKind,
    field_name: &'static str,
    encode: bool,
) -> Result<(FieldKind, FieldKind), InternalError> {
    map_entry_kinds(kind).ok_or_else(|| {
        let message = format!("field kind {kind:?} does not accept map payloads");
        if encode {
            InternalError::persisted_row_field_encode_failed(field_name, message)
        } else {
            InternalError::persisted_row_field_decode_failed(field_name, message)
        }
    })
}

// Encode by-kind list-like containers. The caller provides the concrete field
// kind and field name, so item callbacks no longer need fake strategy/optional
// field metadata.
pub(in crate::db::data::persisted_row::codec) fn encode_collection<'a, I, T>(
    kind: FieldKind,
    items: I,
    field_name: &'static str,
    mut encode_item: impl FnMut(FieldKind, &T, &'static str) -> Result<Vec<u8>, InternalError>,
) -> Result<Vec<u8>, InternalError>
where
    I: IntoIterator<Item = &'a T>,
    T: 'a,
{
    let item_kind = resolve_collection_item_kind(kind, field_name, true)?;

    let iter = items.into_iter();
    let mut item_payloads = Vec::with_capacity(iter.size_hint().0);
    for item in iter {
        item_payloads.push(encode_item(item_kind, item, field_name)?);
    }

    encode_list_field_owned_items(item_payloads.as_slice(), kind, field_name)
}

// Encode structured list-like containers without forcing structured callers
// through the by-kind callback shape.
pub(in crate::db::data::persisted_row::codec) fn encode_structured_collection<'a, I, T>(
    items: I,
    mut encode_item: impl FnMut(&T) -> Result<Vec<u8>, InternalError>,
) -> Result<Vec<u8>, InternalError>
where
    I: IntoIterator<Item = &'a T>,
    T: 'a,
{
    let iter = items.into_iter();
    let mut item_payloads = Vec::with_capacity(iter.size_hint().0);
    for item in iter {
        item_payloads.push(encode_item(item)?);
    }

    Ok(encode_owned_list_item(item_payloads.as_slice()))
}

// Decode by-kind list-like containers. Callers choose whether the result
// remains ordered or is later validated as a set.
pub(in crate::db::data::persisted_row::codec) fn decode_collection<T>(
    kind: FieldKind,
    bytes: &[u8],
    field_name: &'static str,
    mut decode_item: impl FnMut(
        FieldKind,
        &[u8],
        &'static str,
        &'static str,
    ) -> Result<T, InternalError>,
) -> Result<Vec<T>, InternalError> {
    let item_kind = resolve_collection_item_kind(kind, field_name, false)?;
    let item_bytes = decode_list_field_items(bytes, kind)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?;

    item_bytes
        .iter()
        .map(|item| decode_item(item_kind, item.as_slice(), field_name, "item"))
        .collect()
}

// Decode structured list-like containers without exposing by-kind metadata to
// the structured leaf decoder.
pub(in crate::db::data::persisted_row::codec) fn decode_structured_collection<T>(
    bytes: &[u8],
    decode_item: impl FnMut(&[u8]) -> Result<T, InternalError>,
) -> Result<Vec<T>, InternalError> {
    let item_bytes = decode_list_item(bytes).map_err(InternalError::persisted_row_decode_failed)?;

    item_bytes.into_iter().map(decode_item).collect()
}

// Encode by-kind maps. The field envelope stays by-kind-specific, while
// callbacks receive concrete key/value kinds directly.
pub(in crate::db::data::persisted_row::codec) fn encode_map<K, V>(
    kind: FieldKind,
    entries: &BTreeMap<K, V>,
    field_name: &'static str,
    mut encode_key: impl FnMut(FieldKind, &K, &'static str) -> Result<Vec<u8>, InternalError>,
    mut encode_value: impl FnMut(FieldKind, &V, &'static str) -> Result<Vec<u8>, InternalError>,
) -> Result<Vec<u8>, InternalError>
where
    K: Ord,
{
    let (key_kind, value_kind) = resolve_map_entry_kinds(kind, field_name, true)?;

    let mut entry_payloads = Vec::with_capacity(entries.len());
    for (entry_key, entry_value) in entries {
        entry_payloads.push((
            encode_key(key_kind, entry_key, field_name)?,
            encode_value(value_kind, entry_value, field_name)?,
        ));
    }

    encode_map_field_owned_entries(entry_payloads.as_slice(), kind, field_name)
}

// Encode structured maps without carrying by-kind field metadata through every
// key/value callback.
pub(in crate::db::data::persisted_row::codec) fn encode_structured_map<K, V>(
    entries: &BTreeMap<K, V>,
    mut encode_key: impl FnMut(&K) -> Result<Vec<u8>, InternalError>,
    mut encode_value: impl FnMut(&V) -> Result<Vec<u8>, InternalError>,
) -> Result<Vec<u8>, InternalError>
where
    K: Ord,
{
    let mut entry_payloads = Vec::with_capacity(entries.len());
    for (entry_key, entry_value) in entries {
        entry_payloads.push((encode_key(entry_key)?, encode_value(entry_value)?));
    }

    Ok(encode_owned_map_entry(entry_payloads.as_slice()))
}

// Decode by-kind maps. Valid writers emit canonical key order through `BTreeMap`,
// so duplicate or unordered decoded keys are malformed payloads.
pub(in crate::db::data::persisted_row::codec) fn decode_map<K, V>(
    kind: FieldKind,
    bytes: &[u8],
    field_name: &'static str,
    decode_key: impl FnMut(FieldKind, &[u8], &'static str, &'static str) -> Result<K, InternalError>,
    decode_value: impl FnMut(FieldKind, &[u8], &'static str, &'static str) -> Result<V, InternalError>,
) -> Result<BTreeMap<K, V>, InternalError>
where
    K: Ord,
{
    let (key_kind, value_kind) = resolve_map_entry_kinds(kind, field_name, false)?;
    let entry_bytes = decode_map_field_entries(bytes, kind)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?;

    decode_map_entries(
        entry_bytes
            .iter()
            .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice())),
        key_kind,
        value_kind,
        field_name,
        true,
        || by_kind_map_decode_failed::<K, V>(field_name),
        decode_key,
        decode_value,
    )
}

// Decode structured maps while preserving canonical ordering and duplicate
// checks without requiring structured callers to accept by-kind callback
// arguments.
pub(in crate::db::data::persisted_row::codec) fn decode_structured_map<K, V>(
    bytes: &[u8],
    mut decode_key: impl FnMut(&[u8]) -> Result<K, InternalError>,
    mut decode_value: impl FnMut(&[u8]) -> Result<V, InternalError>,
) -> Result<BTreeMap<K, V>, InternalError>
where
    K: Ord,
{
    let entry_bytes =
        decode_map_entry(bytes).map_err(InternalError::persisted_row_decode_failed)?;

    decode_entries(
        entry_bytes,
        true,
        structured_map_decode_failed::<K, V>,
        |key_bytes, value_bytes| Ok((decode_key(key_bytes)?, decode_value(value_bytes)?)),
    )
}

// Decode already-framed by-kind map entries into a map.
#[expect(
    clippy::too_many_arguments,
    reason = "by-kind map traversal keeps key/value kind, field label, ordering, and decode callbacks explicit"
)]
fn decode_map_entries<'a, K, V>(
    entries: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    key_kind: FieldKind,
    value_kind: FieldKind,
    field_name: &'static str,
    enforce_order: bool,
    violation_error: impl FnMut() -> InternalError,
    mut decode_key: impl FnMut(FieldKind, &[u8], &'static str, &'static str) -> Result<K, InternalError>,
    mut decode_value: impl FnMut(
        FieldKind,
        &[u8],
        &'static str,
        &'static str,
    ) -> Result<V, InternalError>,
) -> Result<BTreeMap<K, V>, InternalError>
where
    K: 'a + Ord,
    V: 'a,
{
    decode_entries(
        entries,
        enforce_order,
        violation_error,
        |key_bytes, value_bytes| {
            let key = decode_key(key_kind, key_bytes, field_name, "map key")?;
            let value = decode_value(value_kind, value_bytes, field_name, "map value")?;

            Ok((key, value))
        },
    )
}

// Decode entry payload pairs into a map and optionally enforce structured-map
// ordering. This keeps by-kind and structured map decoding on one insertion path
// while allowing each caller to own its callback shape and error taxonomy.
fn decode_entries<'a, K, V>(
    entries: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    enforce_order: bool,
    mut violation_error: impl FnMut() -> InternalError,
    mut decode_entry: impl FnMut(&'a [u8], &'a [u8]) -> Result<(K, V), InternalError>,
) -> Result<BTreeMap<K, V>, InternalError>
where
    K: 'a + Ord,
    V: 'a,
{
    let mut out = BTreeMap::new();
    for (key_bytes, value_bytes) in entries {
        let (key, value) = decode_entry(key_bytes, value_bytes)?;
        if enforce_order
            && let Some((previous_key, _)) = out.last_key_value()
            && key <= *previous_key
        {
            return Err(violation_error());
        }
        if out.insert(key, value).is_some() && enforce_order {
            return Err(violation_error());
        }
    }

    Ok(out)
}

// Build a by-kind field-level map shape error for malformed duplicate or
// unordered decoded keys. This mirrors the structured invariant while preserving
// by-kind field error taxonomy.
fn by_kind_map_decode_failed<K, V>(field_name: &'static str) -> InternalError {
    InternalError::persisted_row_field_decode_failed(
        field_name,
        format!(
            "by-kind map payload contains duplicate or unordered keys for BTreeMap<{}, {}>",
            std::any::type_name::<K>(),
            std::any::type_name::<V>()
        ),
    )
}

// Build the canonical structured-map shape error from one place so both
// ordering and duplicate checks preserve exactly the same message.
fn structured_map_decode_failed<K, V>() -> InternalError {
    structured_container_decode_failed(&format!(
        "BTreeMap<{}, {}>",
        std::any::type_name::<K>(),
        std::any::type_name::<V>()
    ))
}

// Build the shared structured container mismatch error while keeping each
// caller responsible for its precise rendered type name.
pub(in crate::db::data::persisted_row::codec) fn structured_container_decode_failed(
    type_name: &str,
) -> InternalError {
    InternalError::persisted_row_decode_failed(format!("value payload does not match {type_name}"))
}

// Select the item kind shared by list and set wrappers. Keeping this decision
// here prevents encode and decode from carrying parallel FieldKind checks.
const fn collection_item_kind(kind: FieldKind) -> Option<FieldKind> {
    let (FieldKind::List(inner) | FieldKind::Set(inner)) = kind else {
        return None;
    };

    Some(*inner)
}

// Select the key/value kinds for a map wrapper. This is the single shape gate
// used by both map encode and map decode.
const fn map_entry_kinds(kind: FieldKind) -> Option<(FieldKind, FieldKind)> {
    let FieldKind::Map { key, value } = kind else {
        return None;
    };

    Some((*key, *value))
}
