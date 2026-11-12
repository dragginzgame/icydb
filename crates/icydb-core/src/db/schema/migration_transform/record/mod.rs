//! Rewrite stored record labels through stable accepted member identities.
//! Values are already admitted under the predecessor; candidate admission owns
//! the final shape and byte budget after this bounded catalog-guided walk.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        data::{CanonicalSlotReader, StructuralRowContract, StructuralSlotReader},
        schema::{
            AcceptedFieldKind, AcceptedValueCatalogHandle, MAX_ACCEPTED_RECURSIVE_DEPTH,
            SchemaFieldSlot,
            composite_catalog::{AcceptedCompositeShape, CompositeTypeId},
            enum_catalog::{AcceptedEnumVariantBody, MAX_ACCEPTED_VALUE_BYTES},
        },
    },
    error::InternalError,
    value::{CanonicalEnumBody, Value, ValueEnum},
};

/// Read one predecessor slot and map record labels to the candidate catalog.
pub(in crate::db::schema::migration_transform) fn migrated_source_value(
    before: &StructuralSlotReader<'_>,
    candidate: &StructuralRowContract,
    slot: SchemaFieldSlot,
) -> Result<Value, InternalError> {
    let slot = usize::from(slot.get());
    let value = before.required_value_by_contract_cow(slot)?.into_owned();
    rewrite_value(
        value,
        before
            .contract()
            .required_accepted_field_decode_contract(slot)?
            .kind(),
        before.contract().accepted_value_catalog_handle(),
        candidate.accepted_value_catalog_handle(),
        0,
        &mut (MAX_ACCEPTED_VALUE_BYTES as usize),
    )
}

// The predecessor supplies traversal shape; the candidate supplies only the
// current label for the same stable member ID. Never infer identity by position
// in the candidate, whose alphabetical member order may have changed.
fn rewrite_value(
    value: Value,
    kind: &AcceptedFieldKind,
    before: &AcceptedValueCatalogHandle,
    after: &AcceptedValueCatalogHandle,
    depth: usize,
    label_bytes: &mut usize,
) -> Result<Value, InternalError> {
    if depth > MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(InternalError::store_invariant());
    }
    if matches!(value, Value::Null) {
        return Ok(value);
    }
    let next = depth.saturating_add(1);
    match (kind, value) {
        (AcceptedFieldKind::Composite { type_id }, value) => {
            rewrite_composite(value, *type_id, before, after, next, label_bytes)
        }
        (AcceptedFieldKind::Enum { type_id }, Value::Enum(value)) => {
            let variant_id = value.variant_id();
            let variant = before
                .enum_catalog()
                .enum_type(*type_id)
                .and_then(|definition| definition.variant(variant_id))
                .ok_or_else(InternalError::store_invariant)?;
            let body = match (variant.body(), value.into_body()) {
                (AcceptedEnumVariantBody::Unit, CanonicalEnumBody::Unit) => CanonicalEnumBody::Unit,
                (
                    AcceptedEnumVariantBody::Payload { contract },
                    CanonicalEnumBody::Payload(value),
                ) => CanonicalEnumBody::Payload(Box::new(rewrite_value(
                    *value,
                    contract.kind(),
                    before,
                    after,
                    next,
                    label_bytes,
                )?)),
                _ => return Err(InternalError::store_invariant()),
            };
            Ok(Value::Enum(ValueEnum::new(*type_id, variant_id, body)))
        }
        (AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner), Value::List(values)) => {
            let mut values = values
                .into_iter()
                .map(|value| rewrite_value(value, inner, before, after, next, label_bytes))
                .collect::<Result<Vec<_>, _>>()?;
            if matches!(kind, AcceptedFieldKind::Set(_)) {
                values.sort_unstable_by(Value::canonical_cmp);
            }
            Ok(Value::List(values))
        }
        (
            AcceptedFieldKind::Map {
                key,
                value: value_kind,
            },
            Value::Map(entries),
        ) => {
            let mut entries = entries
                .into_iter()
                .map(|(entry_key, value)| {
                    Ok((
                        rewrite_value(entry_key, key, before, after, next, label_bytes)?,
                        rewrite_value(value, value_kind, before, after, next, label_bytes)?,
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            entries.sort_unstable_by(|left, right| Value::canonical_cmp(&left.0, &right.0));
            Ok(Value::Map(entries))
        }
        (AcceptedFieldKind::Relation { key_kind, .. }, value) => {
            rewrite_value(value, key_kind, before, after, next, label_bytes)
        }
        (_, value) => Ok(value),
    }
}

fn rewrite_composite(
    value: Value,
    type_id: CompositeTypeId,
    before: &AcceptedValueCatalogHandle,
    after: &AcceptedValueCatalogHandle,
    next: usize,
    label_bytes: &mut usize,
) -> Result<Value, InternalError> {
    let old = before
        .composite_catalog()
        .composite_type(type_id)
        .ok_or_else(InternalError::store_invariant)?;
    let new = after
        .composite_catalog()
        .composite_type(type_id)
        .ok_or_else(InternalError::store_invariant)?;
    match (old.shape(), new.shape(), value) {
        (
            AcceptedCompositeShape::Record(old_fields),
            AcceptedCompositeShape::Record(new_fields),
            Value::Map(entries),
        ) => {
            if entries.len() != old_fields.len() || old_fields.len() != new_fields.len() {
                return Err(InternalError::store_invariant());
            }
            let mut rewritten = Vec::with_capacity(entries.len());
            for ((key, value), field) in entries.into_iter().zip(old_fields) {
                if !matches!(&key, Value::Text(name) if name == field.name()) {
                    return Err(InternalError::store_invariant());
                }
                let target = new_fields
                    .iter()
                    .find(|target| target.id() == field.id())
                    .ok_or_else(InternalError::store_invariant)?;
                // Bound newly allocated labels before cloning them. Any value
                // admitted by the candidate fits this existing per-value limit;
                // final admission still charges the complete rewritten value.
                *label_bytes = label_bytes
                    .checked_sub(target.name().len())
                    .ok_or_else(InternalError::store_invariant)?;
                rewritten.push((
                    Value::Text(target.name().to_string()),
                    rewrite_value(
                        value,
                        field.contract().kind(),
                        before,
                        after,
                        next,
                        label_bytes,
                    )?,
                ));
            }
            rewritten.sort_unstable_by(|left, right| Value::canonical_cmp(&left.0, &right.0));
            Ok(Value::Map(rewritten))
        }
        (
            AcceptedCompositeShape::Tuple(fields),
            AcceptedCompositeShape::Tuple(_),
            Value::List(values),
        ) => {
            if fields.len() != values.len() {
                return Err(InternalError::store_invariant());
            }
            values
                .into_iter()
                .zip(fields)
                .map(|(value, field)| {
                    rewrite_value(value, field.kind(), before, after, next, label_bytes)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List)
        }
        (AcceptedCompositeShape::Newtype(inner), AcceptedCompositeShape::Newtype(_), value) => {
            rewrite_value(value, inner.kind(), before, after, next, label_bytes)
        }
        _ => Err(InternalError::store_invariant()),
    }
}
