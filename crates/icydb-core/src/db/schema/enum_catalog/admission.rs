//! Catalog-backed normalization and strict validation for canonical values.
use super::{
    AcceptedEnumCatalog, AcceptedEnumVariantBody, AcceptedSchemaAuthority, AcceptedSchemaRevision,
    AcceptedValueCatalogHandle, AcceptedValueContract, EnumTypeId, EnumValueResolutionError,
};
use crate::{
    db::schema::{
        AcceptedFieldKind, MAX_ACCEPTED_RECURSIVE_DEPTH_U16,
        composite_catalog::{
            AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeShape,
            CompositeTypeId,
        },
    },
    model::field::FieldStorageDecode,
    types::Decimal,
    value::{CanonicalEnumBody, CanonicalEnumValue, InputValue, InputValueEnum, Value, ValueEnum},
};
use std::cmp::Ordering;

const MAX_ACCEPTED_VALUE_DEPTH: u16 = MAX_ACCEPTED_RECURSIVE_DEPTH_U16;
pub(in crate::db) const MAX_ACCEPTED_VALUE_BYTES: u32 = 4 * 1024 * 1024;

/// Runtime `Value` is the canonical accepted-value domain.
pub(in crate::db) type CanonicalValue = Value;

type AdmittedEnumValue = ValueEnum;

/// Typed accepted-value admission rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ValueAdmissionError {
    DepthExceeded,
    SizeExceeded,
    TypeMismatch,
    ScalarConstraint,
    EnumPathMismatch,
    EnumTypeMismatch,
    UnknownEnumType,
    UnknownEnumVariant,
    EnumBodyMismatch,
    UnknownCompositeType,
    CompositeShapeMismatch,
    CompositeFieldMismatch,
    DuplicateSetItem,
    DuplicateMapKey,
    InvalidAcceptedContract,
    MissingSchemaRevision,
}

#[derive(Clone, Copy)]
struct AdmissionCatalogs<'a> {
    enums: &'a AcceptedEnumCatalog,
    composites: &'a AcceptedCompositeCatalog,
}

impl<'a> AdmissionCatalogs<'a> {
    fn from_handle(handle: &'a AcceptedValueCatalogHandle) -> Self {
        Self {
            enums: handle.enum_catalog(),
            composites: handle.composite_catalog(),
        }
    }
}

/// Shared recursion and encoded-size budget for one admission operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ValueAdmissionBudget {
    max_depth: u16,
    remaining_bytes: u32,
}

impl ValueAdmissionBudget {
    #[must_use]
    pub(in crate::db) const fn standard() -> Self {
        Self {
            max_depth: MAX_ACCEPTED_VALUE_DEPTH,
            remaining_bytes: MAX_ACCEPTED_VALUE_BYTES,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn with_limits(max_depth: u16, max_bytes: u32) -> Self {
        Self {
            max_depth,
            remaining_bytes: max_bytes,
        }
    }

    const fn enter(self, depth: u16) -> Result<(), ValueAdmissionError> {
        if depth >= self.max_depth {
            return Err(ValueAdmissionError::DepthExceeded);
        }
        Ok(())
    }

    fn consume(&mut self, bytes: usize) -> Result<(), ValueAdmissionError> {
        let bytes = u32::try_from(bytes).map_err(|_| ValueAdmissionError::SizeExceeded)?;
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or(ValueAdmissionError::SizeExceeded)?;
        Ok(())
    }
}

/// Owned canonical value pinned to the accepted revision that admitted it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AdmittedOwnedValue {
    authority: AcceptedSchemaAuthority,
    value: CanonicalValue,
}

impl AdmittedOwnedValue {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.authority.revision()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn authority(&self) -> &AcceptedSchemaAuthority {
        &self.authority
    }

    #[must_use]
    pub(in crate::db) const fn value(&self) -> &CanonicalValue {
        &self.value
    }
}

/// Borrowed proof that one canonical value matches one accepted contract.
pub(in crate::db) struct AcceptedValueRef<'a> {
    catalog: &'a AcceptedValueCatalogHandle,
    contract: &'a AcceptedValueContract,
    value: &'a CanonicalValue,
}

impl<'a> AcceptedValueRef<'a> {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.catalog.revision()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn authority(&self) -> &'a AcceptedSchemaAuthority {
        self.catalog.authority()
    }

    #[must_use]
    pub(in crate::db) fn catalog(&self) -> &'a AcceptedEnumCatalog {
        self.catalog.enum_catalog()
    }

    #[must_use]
    pub(in crate::db) const fn contract(&self) -> &'a AcceptedValueContract {
        self.contract
    }

    #[must_use]
    pub(in crate::db) const fn value(&self) -> &'a CanonicalValue {
        self.value
    }
}

pub(in crate::db) fn normalize_and_admit_value(
    catalog: &AcceptedValueCatalogHandle,
    contract: &AcceptedValueContract,
    input: InputValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<AdmittedOwnedValue, ValueAdmissionError> {
    let value = normalize_nullable_value(catalog, contract, false, input, budget)?;
    Ok(AdmittedOwnedValue {
        authority: catalog.authority().clone(),
        value,
    })
}

pub(in crate::db::schema) fn normalize_and_admit_nullable_value(
    catalog: &AcceptedValueCatalogHandle,
    contract: &AcceptedValueContract,
    nullable: bool,
    input: InputValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<AdmittedOwnedValue, ValueAdmissionError> {
    if !matches!(&input, InputValue::Null) {
        return normalize_and_admit_value(catalog, contract, input, budget);
    }

    let value = normalize_nullable_value(catalog, contract, nullable, input, budget)?;
    Ok(AdmittedOwnedValue {
        authority: catalog.authority().clone(),
        value,
    })
}

/// Normalize one authored value and expose its short-lived accepted proof.
pub(in crate::db::schema) fn with_normalized_accepted_value<R>(
    catalog: &AcceptedValueCatalogHandle,
    contract: &AcceptedValueContract,
    nullable: bool,
    input: InputValue,
    budget: &mut ValueAdmissionBudget,
    use_value: impl for<'value> FnOnce(AcceptedValueRef<'value>) -> R,
) -> Result<R, ValueAdmissionError> {
    let value = normalize_nullable_value(catalog, contract, nullable, input, budget)?;
    Ok(use_value(AcceptedValueRef {
        catalog,
        contract,
        value: &value,
    }))
}

fn normalize_nullable_value(
    catalog: &AcceptedValueCatalogHandle,
    contract: &AcceptedValueContract,
    nullable: bool,
    input: InputValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    if catalog.revision() == AcceptedSchemaRevision::NONE {
        return Err(ValueAdmissionError::MissingSchemaRevision);
    }
    if matches!(&input, InputValue::Null) {
        if !nullable {
            return Err(ValueAdmissionError::TypeMismatch);
        }
        budget.enter(0)?;
        budget.consume(1)?;
        return Ok(CanonicalValue::Null);
    }

    normalize_contract(
        AdmissionCatalogs::from_handle(catalog),
        contract,
        input,
        0,
        budget,
    )
}

/// Resolve and encode one generated unit-enum default through the candidate catalog.
pub(in crate::db) fn encode_unit_enum_default_in_catalog(
    catalog: &AcceptedEnumCatalog,
    enum_path: &str,
    variant_name: &str,
) -> Result<Vec<u8>, ValueAdmissionError> {
    let type_id = catalog
        .type_id(enum_path)
        .ok_or(ValueAdmissionError::UnknownEnumType)?;
    let definition = catalog
        .enum_type(type_id)
        .ok_or(ValueAdmissionError::UnknownEnumType)?;
    let variant_id = definition
        .variant_id(variant_name)
        .ok_or(ValueAdmissionError::UnknownEnumVariant)?;
    let variant = definition
        .variant(variant_id)
        .ok_or(ValueAdmissionError::UnknownEnumVariant)?;
    if !matches!(variant.body(), AcceptedEnumVariantBody::Unit) {
        return Err(ValueAdmissionError::EnumBodyMismatch);
    }

    let value = CanonicalEnumValue::<()>::new(type_id, variant_id, CanonicalEnumBody::Unit);
    super::encode_canonical_enum_value(&value, |(), _| {
        Err(super::CanonicalEnumWireError::PayloadCodec)
    })
    .map_err(|_| ValueAdmissionError::InvalidAcceptedContract)
}

pub(in crate::db::schema) fn admit_canonical_value(
    catalog: &AcceptedValueCatalogHandle,
    contract: &AcceptedValueContract,
    nullable: bool,
    value: CanonicalValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<AdmittedOwnedValue, ValueAdmissionError> {
    let _ = validate_nullable_canonical_value(catalog, contract, nullable, &value, budget)?;
    Ok(AdmittedOwnedValue {
        authority: catalog.authority().clone(),
        value,
    })
}

/// Validate decoded bytes against catalog definitions without assigning
/// published-revision provenance to the value.
pub(in crate::db) fn validate_decoded_persisted_field_value_in_catalog(
    catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    kind: &AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    nullable: bool,
    value: &CanonicalValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    validate_persisted_field_value_in_catalog(
        catalog,
        composite_catalog,
        kind,
        storage_decode,
        nullable,
        value,
        budget,
    )
}

fn validate_persisted_field_value_in_catalog(
    catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    kind: &AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    nullable: bool,
    value: &CanonicalValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    if matches!(value, CanonicalValue::Null) {
        if !nullable {
            return Err(ValueAdmissionError::TypeMismatch);
        }
        budget.enter(0)?;
        return budget.consume(1);
    }
    let contract = AcceptedValueContract::from_candidate_catalogs(
        catalog,
        composite_catalog,
        kind,
        storage_decode,
    )
    .map_err(|_| ValueAdmissionError::InvalidAcceptedContract)?;
    validate_contract(
        AdmissionCatalogs {
            enums: catalog,
            composites: composite_catalog,
        },
        &contract,
        value,
        0,
        budget,
    )
}

#[cfg(test)]
pub(in crate::db) fn validate_canonical_value<'a>(
    catalog: &'a AcceptedValueCatalogHandle,
    contract: &'a AcceptedValueContract,
    value: &'a CanonicalValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<AcceptedValueRef<'a>, ValueAdmissionError> {
    validate_nullable_canonical_value(catalog, contract, false, value, budget)
}

/// Strictly validate one canonical value with its accepted nullability rule.
pub(in crate::db::schema) fn validate_nullable_canonical_value<'a>(
    catalog: &'a AcceptedValueCatalogHandle,
    contract: &'a AcceptedValueContract,
    nullable: bool,
    value: &'a CanonicalValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<AcceptedValueRef<'a>, ValueAdmissionError> {
    if catalog.authority().revision() == AcceptedSchemaRevision::NONE {
        return Err(ValueAdmissionError::MissingSchemaRevision);
    }
    if matches!(value, CanonicalValue::Null) {
        if !nullable {
            return Err(ValueAdmissionError::TypeMismatch);
        }
        budget.enter(0)?;
        budget.consume(1)?;
    } else {
        validate_contract(
            AdmissionCatalogs::from_handle(catalog),
            contract,
            value,
            0,
            budget,
        )?;
    }
    Ok(AcceptedValueRef {
        catalog,
        contract,
        value,
    })
}

fn normalize_contract(
    catalogs: AdmissionCatalogs<'_>,
    contract: &AcceptedValueContract,
    input: InputValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    normalize_kind(catalogs, contract.kind(), input, depth, budget)
}

#[expect(
    clippy::too_many_lines,
    reason = "accepted kind normalization remains one exhaustive auditable match across every scalar and recursive kind"
)]
fn normalize_kind(
    catalogs: AdmissionCatalogs<'_>,
    kind: &AcceptedFieldKind,
    input: InputValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    budget.enter(depth)?;
    match (kind, input) {
        (AcceptedFieldKind::Account, InputValue::Account(value)) => {
            budget.consume(64)?;
            Ok(CanonicalValue::Account(value))
        }
        (AcceptedFieldKind::Blob { max_len }, InputValue::Blob(value)) => {
            ensure_max_len(value.len(), *max_len)?;
            budget.consume(5_usize.saturating_add(value.len()))?;
            Ok(CanonicalValue::Blob(value))
        }
        (AcceptedFieldKind::Bool, InputValue::Bool(value)) => {
            budget.consume(2)?;
            Ok(CanonicalValue::Bool(value))
        }
        (AcceptedFieldKind::Date, InputValue::Date(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Date(value))
        }
        (AcceptedFieldKind::Decimal { scale }, InputValue::Decimal(value)) => {
            budget.consume(21)?;
            normalize_decimal(value, *scale).map(CanonicalValue::Decimal)
        }
        (AcceptedFieldKind::Duration, InputValue::Duration(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Duration(value))
        }
        (AcceptedFieldKind::Enum { type_id }, InputValue::Enum(value)) => {
            normalize_enum(catalogs, *type_id, value, depth, budget).map(CanonicalValue::Enum)
        }
        (AcceptedFieldKind::Float32, InputValue::Float32(value)) => {
            budget.consume(5)?;
            Ok(CanonicalValue::Float32(value))
        }
        (AcceptedFieldKind::Float64, InputValue::Float64(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Float64(value))
        }
        (AcceptedFieldKind::Int8, InputValue::Int64(value)) if i8::try_from(value).is_ok() => {
            budget.consume(2)?;
            Ok(CanonicalValue::Int64(value))
        }
        (AcceptedFieldKind::Int16, InputValue::Int64(value)) if i16::try_from(value).is_ok() => {
            budget.consume(3)?;
            Ok(CanonicalValue::Int64(value))
        }
        (AcceptedFieldKind::Int32, InputValue::Int64(value)) if i32::try_from(value).is_ok() => {
            budget.consume(5)?;
            Ok(CanonicalValue::Int64(value))
        }
        (AcceptedFieldKind::Int64, InputValue::Int64(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Int64(value))
        }
        (AcceptedFieldKind::Int128, InputValue::Int128(value)) => {
            budget.consume(17)?;
            Ok(CanonicalValue::Int128(value))
        }
        (AcceptedFieldKind::IntBig { max_bytes }, InputValue::IntBig(value)) => {
            let bytes = value.to_leb128().len();
            ensure_max_len(bytes, Some(*max_bytes))?;
            budget.consume(5_usize.saturating_add(bytes))?;
            Ok(CanonicalValue::IntBig(value))
        }
        (AcceptedFieldKind::Principal, InputValue::Principal(value)) => {
            budget.consume(32)?;
            Ok(CanonicalValue::Principal(value))
        }
        (AcceptedFieldKind::Subaccount, InputValue::Subaccount(value)) => {
            budget.consume(33)?;
            Ok(CanonicalValue::Subaccount(value))
        }
        (AcceptedFieldKind::Text { max_len }, InputValue::Text(value)) => {
            ensure_text_max_len(value.as_str(), *max_len)?;
            budget.consume(5_usize.saturating_add(value.len()))?;
            Ok(CanonicalValue::Text(value))
        }
        (AcceptedFieldKind::Timestamp, InputValue::Timestamp(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Timestamp(value))
        }
        (AcceptedFieldKind::Nat8, InputValue::Nat64(value)) if u8::try_from(value).is_ok() => {
            budget.consume(2)?;
            Ok(CanonicalValue::Nat64(value))
        }
        (AcceptedFieldKind::Nat16, InputValue::Nat64(value)) if u16::try_from(value).is_ok() => {
            budget.consume(3)?;
            Ok(CanonicalValue::Nat64(value))
        }
        (AcceptedFieldKind::Nat32, InputValue::Nat64(value)) if u32::try_from(value).is_ok() => {
            budget.consume(5)?;
            Ok(CanonicalValue::Nat64(value))
        }
        (AcceptedFieldKind::Nat64, InputValue::Nat64(value)) => {
            budget.consume(9)?;
            Ok(CanonicalValue::Nat64(value))
        }
        (AcceptedFieldKind::Nat128, InputValue::Nat128(value)) => {
            budget.consume(17)?;
            Ok(CanonicalValue::Nat128(value))
        }
        (AcceptedFieldKind::NatBig { max_bytes }, InputValue::NatBig(value)) => {
            let bytes = value.to_leb128().len();
            ensure_max_len(bytes, Some(*max_bytes))?;
            budget.consume(5_usize.saturating_add(bytes))?;
            Ok(CanonicalValue::NatBig(value))
        }
        (AcceptedFieldKind::Ulid, InputValue::Ulid(value)) => {
            budget.consume(17)?;
            Ok(CanonicalValue::Ulid(value))
        }
        (AcceptedFieldKind::Unit, InputValue::Unit) => {
            budget.consume(1)?;
            Ok(CanonicalValue::Unit)
        }
        (AcceptedFieldKind::Relation { key_kind, .. }, input) => {
            normalize_kind(catalogs, key_kind, input, depth.saturating_add(1), budget)
        }
        (AcceptedFieldKind::List(inner), InputValue::List(items)) => {
            budget.consume(5)?;
            normalize_list(catalogs, inner, items, depth, budget, false)
        }
        (AcceptedFieldKind::Set(inner), InputValue::List(items)) => {
            budget.consume(5)?;
            normalize_list(catalogs, inner, items, depth, budget, true)
        }
        (AcceptedFieldKind::Map { key, value }, InputValue::Map(entries)) => {
            budget.consume(5)?;
            normalize_map(catalogs, key, value, entries, depth, budget)
        }
        (AcceptedFieldKind::Composite { type_id }, input) => {
            normalize_composite(catalogs, *type_id, input, depth, budget)
        }
        _ => Err(ValueAdmissionError::TypeMismatch),
    }
}

fn normalize_list(
    catalogs: AdmissionCatalogs<'_>,
    kind: &AcceptedFieldKind,
    items: Vec<InputValue>,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
    is_set: bool,
) -> Result<CanonicalValue, ValueAdmissionError> {
    budget.consume(items.len())?;
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        values.push(normalize_kind(
            catalogs,
            kind,
            item,
            depth.saturating_add(1),
            budget,
        )?);
    }
    if is_set {
        values.sort_unstable_by(Value::canonical_cmp);
        if values.windows(2).any(|items| items[0] == items[1]) {
            return Err(ValueAdmissionError::DuplicateSetItem);
        }
    }
    Ok(CanonicalValue::List(values))
}

fn normalize_map(
    catalogs: AdmissionCatalogs<'_>,
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
    entries: Vec<(InputValue, InputValue)>,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    budget.consume(entries.len().saturating_mul(2))?;
    let mut values = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        values.push((
            normalize_kind(catalogs, key_kind, key, depth.saturating_add(1), budget)?,
            normalize_kind(catalogs, value_kind, value, depth.saturating_add(1), budget)?,
        ));
    }
    values.sort_unstable_by(|left, right| Value::canonical_cmp(&left.0, &right.0));
    if values
        .windows(2)
        .any(|entries| entries[0].0 == entries[1].0)
    {
        return Err(ValueAdmissionError::DuplicateMapKey);
    }
    Ok(CanonicalValue::Map(values))
}

fn normalize_enum(
    catalogs: AdmissionCatalogs<'_>,
    expected_type_id: EnumTypeId,
    input: InputValueEnum,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<AdmittedEnumValue, ValueAdmissionError> {
    budget.consume(13)?;
    let (variant_name, path, payload) = input.into_parts();
    if let Some(path) = path.as_deref() {
        let resolved = catalogs
            .enums
            .type_id(path)
            .ok_or(ValueAdmissionError::UnknownEnumType)?;
        if resolved != expected_type_id {
            return Err(ValueAdmissionError::EnumPathMismatch);
        }
    }
    let definition = catalogs
        .enums
        .enum_type(expected_type_id)
        .ok_or(ValueAdmissionError::UnknownEnumType)?;
    let variant_id = definition
        .variant_id(variant_name.as_str())
        .ok_or(ValueAdmissionError::UnknownEnumVariant)?;
    let variant = definition
        .variant(variant_id)
        .ok_or(ValueAdmissionError::UnknownEnumVariant)?;
    let body = match (variant.body(), payload) {
        (AcceptedEnumVariantBody::Unit, None) => CanonicalEnumBody::Unit,
        (AcceptedEnumVariantBody::Payload { contract }, Some(payload)) => {
            CanonicalEnumBody::Payload(Box::new(normalize_contract(
                catalogs,
                contract,
                payload,
                depth.saturating_add(1),
                budget,
            )?))
        }
        _ => return Err(ValueAdmissionError::EnumBodyMismatch),
    };
    Ok(ValueEnum::new(expected_type_id, variant_id, body))
}

fn normalize_composite(
    catalogs: AdmissionCatalogs<'_>,
    type_id: CompositeTypeId,
    input: InputValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    let definition = catalogs
        .composites
        .composite_type(type_id)
        .ok_or(ValueAdmissionError::UnknownCompositeType)?;
    match (definition.shape(), input) {
        (AcceptedCompositeShape::Record(fields), InputValue::Map(entries)) => {
            budget.consume(5)?;
            budget.consume(entries.len().saturating_mul(2))?;
            if entries.len() != fields.len() {
                return Err(ValueAdmissionError::CompositeShapeMismatch);
            }
            let mut authored = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let InputValue::Text(name) = key else {
                    return Err(ValueAdmissionError::CompositeFieldMismatch);
                };
                budget.consume(5_usize.saturating_add(name.len()))?;
                authored.push((name, value));
            }
            authored.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            if authored.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(ValueAdmissionError::CompositeFieldMismatch);
            }

            let mut values = Vec::with_capacity(fields.len());
            for ((name, input), field) in authored.into_iter().zip(fields) {
                if name != field.name() {
                    return Err(ValueAdmissionError::CompositeFieldMismatch);
                }
                values.push((
                    CanonicalValue::Text(name),
                    normalize_composite_element(
                        catalogs,
                        field.contract(),
                        input,
                        depth.saturating_add(1),
                        budget,
                    )?,
                ));
            }
            Ok(CanonicalValue::Map(values))
        }
        (AcceptedCompositeShape::Tuple(elements), InputValue::List(values)) => {
            budget.consume(5)?;
            budget.consume(values.len())?;
            if values.len() != elements.len() {
                return Err(ValueAdmissionError::CompositeShapeMismatch);
            }
            let values = values
                .into_iter()
                .zip(elements)
                .map(|(input, element)| {
                    normalize_composite_element(
                        catalogs,
                        element,
                        input,
                        depth.saturating_add(1),
                        budget,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CanonicalValue::List(values))
        }
        (AcceptedCompositeShape::Newtype(inner), input) => {
            normalize_composite_element(catalogs, inner, input, depth.saturating_add(1), budget)
        }
        _ => Err(ValueAdmissionError::CompositeShapeMismatch),
    }
}

fn normalize_composite_element(
    catalogs: AdmissionCatalogs<'_>,
    element: &AcceptedCompositeElement,
    input: InputValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<CanonicalValue, ValueAdmissionError> {
    if matches!(input, InputValue::Null) {
        if !element.nullable() {
            return Err(ValueAdmissionError::TypeMismatch);
        }
        budget.enter(depth)?;
        budget.consume(1)?;
        return Ok(CanonicalValue::Null);
    }
    normalize_kind(catalogs, element.kind(), input, depth, budget)
}

fn validate_contract(
    catalogs: AdmissionCatalogs<'_>,
    contract: &AcceptedValueContract,
    value: &CanonicalValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    validate_kind(catalogs, contract.kind(), value, depth, budget)
}

fn validate_kind(
    catalogs: AdmissionCatalogs<'_>,
    kind: &AcceptedFieldKind,
    value: &CanonicalValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    budget.enter(depth)?;
    match (kind, value) {
        (AcceptedFieldKind::Account, CanonicalValue::Account(_)) => budget.consume(64),
        (AcceptedFieldKind::Blob { max_len }, CanonicalValue::Blob(value)) => {
            ensure_max_len(value.len(), *max_len)?;
            budget.consume(5_usize.saturating_add(value.len()))
        }
        (AcceptedFieldKind::Bool, CanonicalValue::Bool(_)) => budget.consume(2),
        (AcceptedFieldKind::Date, CanonicalValue::Date(_))
        | (AcceptedFieldKind::Duration, CanonicalValue::Duration(_))
        | (AcceptedFieldKind::Float64, CanonicalValue::Float64(_))
        | (AcceptedFieldKind::Int64, CanonicalValue::Int64(_))
        | (AcceptedFieldKind::Timestamp, CanonicalValue::Timestamp(_))
        | (AcceptedFieldKind::Nat64, CanonicalValue::Nat64(_)) => budget.consume(9),
        (AcceptedFieldKind::Decimal { scale }, CanonicalValue::Decimal(value)) => {
            if value.scale() != *scale {
                return Err(ValueAdmissionError::ScalarConstraint);
            }
            budget.consume(21)
        }
        (AcceptedFieldKind::Enum { type_id }, CanonicalValue::Enum(value)) => {
            validate_enum(catalogs, *type_id, value, depth, budget)
        }
        (AcceptedFieldKind::Float32, CanonicalValue::Float32(_)) => budget.consume(5),
        (AcceptedFieldKind::Int8, CanonicalValue::Int64(value)) if i8::try_from(*value).is_ok() => {
            budget.consume(2)
        }
        (AcceptedFieldKind::Int16, CanonicalValue::Int64(value))
            if i16::try_from(*value).is_ok() =>
        {
            budget.consume(3)
        }
        (AcceptedFieldKind::Int32, CanonicalValue::Int64(value))
            if i32::try_from(*value).is_ok() =>
        {
            budget.consume(5)
        }
        (AcceptedFieldKind::Int128, CanonicalValue::Int128(_))
        | (AcceptedFieldKind::Nat128, CanonicalValue::Nat128(_))
        | (AcceptedFieldKind::Ulid, CanonicalValue::Ulid(_)) => budget.consume(17),
        (AcceptedFieldKind::IntBig { max_bytes }, CanonicalValue::IntBig(value)) => {
            let bytes = value.to_leb128().len();
            ensure_max_len(bytes, Some(*max_bytes))?;
            budget.consume(5_usize.saturating_add(bytes))
        }
        (AcceptedFieldKind::Principal, CanonicalValue::Principal(_)) => budget.consume(32),
        (AcceptedFieldKind::Subaccount, CanonicalValue::Subaccount(_)) => budget.consume(33),
        (AcceptedFieldKind::Text { max_len }, CanonicalValue::Text(value)) => {
            ensure_text_max_len(value, *max_len)?;
            budget.consume(5_usize.saturating_add(value.len()))
        }
        (AcceptedFieldKind::Nat8, CanonicalValue::Nat64(value)) if u8::try_from(*value).is_ok() => {
            budget.consume(2)
        }
        (AcceptedFieldKind::Nat16, CanonicalValue::Nat64(value))
            if u16::try_from(*value).is_ok() =>
        {
            budget.consume(3)
        }
        (AcceptedFieldKind::Nat32, CanonicalValue::Nat64(value))
            if u32::try_from(*value).is_ok() =>
        {
            budget.consume(5)
        }
        (AcceptedFieldKind::NatBig { max_bytes }, CanonicalValue::NatBig(value)) => {
            let bytes = value.to_leb128().len();
            ensure_max_len(bytes, Some(*max_bytes))?;
            budget.consume(5_usize.saturating_add(bytes))
        }
        (AcceptedFieldKind::Unit, CanonicalValue::Unit) => budget.consume(1),
        (AcceptedFieldKind::Relation { key_kind, .. }, value) => {
            validate_kind(catalogs, key_kind, value, depth.saturating_add(1), budget)
        }
        (AcceptedFieldKind::List(inner), CanonicalValue::List(items)) => {
            budget.consume(5)?;
            validate_list(catalogs, inner, items, depth, budget, false)
        }
        (AcceptedFieldKind::Set(inner), CanonicalValue::List(items)) => {
            budget.consume(5)?;
            validate_list(catalogs, inner, items, depth, budget, true)
        }
        (AcceptedFieldKind::Map { key, value }, CanonicalValue::Map(entries)) => {
            budget.consume(5)?;
            validate_map(catalogs, key, value, entries, depth, budget)
        }
        (AcceptedFieldKind::Composite { type_id }, value) => {
            validate_composite(catalogs, *type_id, value, depth, budget)
        }
        _ => Err(ValueAdmissionError::TypeMismatch),
    }
}

fn validate_list(
    catalogs: AdmissionCatalogs<'_>,
    kind: &AcceptedFieldKind,
    items: &[CanonicalValue],
    depth: u16,
    budget: &mut ValueAdmissionBudget,
    is_set: bool,
) -> Result<(), ValueAdmissionError> {
    if is_set
        && items
            .windows(2)
            .any(|items| Value::canonical_cmp(&items[0], &items[1]) != Ordering::Less)
    {
        return Err(ValueAdmissionError::DuplicateSetItem);
    }
    for item in items {
        validate_kind(catalogs, kind, item, depth.saturating_add(1), budget)?;
    }
    Ok(())
}

fn validate_map(
    catalogs: AdmissionCatalogs<'_>,
    key_kind: &AcceptedFieldKind,
    value_kind: &AcceptedFieldKind,
    entries: &[(CanonicalValue, CanonicalValue)],
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    if entries
        .windows(2)
        .any(|entries| Value::canonical_cmp(&entries[0].0, &entries[1].0) != Ordering::Less)
    {
        return Err(ValueAdmissionError::DuplicateMapKey);
    }
    for (key, value) in entries {
        validate_kind(catalogs, key_kind, key, depth.saturating_add(1), budget)?;
        validate_kind(catalogs, value_kind, value, depth.saturating_add(1), budget)?;
    }
    Ok(())
}

fn validate_enum(
    catalogs: AdmissionCatalogs<'_>,
    expected_type_id: EnumTypeId,
    value: &AdmittedEnumValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    budget.consume(13)?;
    if value.type_id() != expected_type_id {
        return Err(ValueAdmissionError::EnumTypeMismatch);
    }
    let selection =
        catalogs
            .enums
            .resolve_value(value.canonical())
            .map_err(|error| match error {
                EnumValueResolutionError::UnknownType => ValueAdmissionError::UnknownEnumType,
                EnumValueResolutionError::UnknownVariant => ValueAdmissionError::UnknownEnumVariant,
            })?;
    match (selection.accepted_body(), selection.value_body()) {
        (AcceptedEnumVariantBody::Unit, CanonicalEnumBody::Unit) => Ok(()),
        (AcceptedEnumVariantBody::Payload { contract }, CanonicalEnumBody::Payload(payload)) => {
            validate_contract(catalogs, contract, payload, depth.saturating_add(1), budget)
        }
        _ => Err(ValueAdmissionError::EnumBodyMismatch),
    }
}

fn validate_composite(
    catalogs: AdmissionCatalogs<'_>,
    type_id: CompositeTypeId,
    value: &CanonicalValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    let definition = catalogs
        .composites
        .composite_type(type_id)
        .ok_or(ValueAdmissionError::UnknownCompositeType)?;
    match (definition.shape(), value) {
        (AcceptedCompositeShape::Record(fields), CanonicalValue::Map(entries)) => {
            budget.consume(5)?;
            budget.consume(entries.len().saturating_mul(2))?;
            if entries.len() != fields.len() {
                return Err(ValueAdmissionError::CompositeShapeMismatch);
            }
            for ((key, value), field) in entries.iter().zip(fields) {
                let CanonicalValue::Text(name) = key else {
                    return Err(ValueAdmissionError::CompositeFieldMismatch);
                };
                if name != field.name() {
                    return Err(ValueAdmissionError::CompositeFieldMismatch);
                }
                budget.consume(5_usize.saturating_add(name.len()))?;
                validate_composite_element(
                    catalogs,
                    field.contract(),
                    value,
                    depth.saturating_add(1),
                    budget,
                )?;
            }
            Ok(())
        }
        (AcceptedCompositeShape::Tuple(elements), CanonicalValue::List(values)) => {
            budget.consume(5)?;
            budget.consume(values.len())?;
            if values.len() != elements.len() {
                return Err(ValueAdmissionError::CompositeShapeMismatch);
            }
            for (value, element) in values.iter().zip(elements) {
                validate_composite_element(
                    catalogs,
                    element,
                    value,
                    depth.saturating_add(1),
                    budget,
                )?;
            }
            Ok(())
        }
        (AcceptedCompositeShape::Newtype(inner), value) => {
            validate_composite_element(catalogs, inner, value, depth.saturating_add(1), budget)
        }
        _ => Err(ValueAdmissionError::CompositeShapeMismatch),
    }
}

fn validate_composite_element(
    catalogs: AdmissionCatalogs<'_>,
    element: &AcceptedCompositeElement,
    value: &CanonicalValue,
    depth: u16,
    budget: &mut ValueAdmissionBudget,
) -> Result<(), ValueAdmissionError> {
    if matches!(value, CanonicalValue::Null) {
        if !element.nullable() {
            return Err(ValueAdmissionError::TypeMismatch);
        }
        budget.enter(depth)?;
        return budget.consume(1);
    }
    validate_kind(catalogs, element.kind(), value, depth, budget)
}

fn ensure_max_len(len: usize, max_len: Option<u32>) -> Result<(), ValueAdmissionError> {
    if max_len.is_some_and(|max_len| len > max_len as usize) {
        return Err(ValueAdmissionError::ScalarConstraint);
    }
    Ok(())
}

fn ensure_text_max_len(value: &str, max_len: Option<u32>) -> Result<(), ValueAdmissionError> {
    if max_len.is_some_and(|max_len| value.chars().count() > max_len as usize) {
        return Err(ValueAdmissionError::ScalarConstraint);
    }
    Ok(())
}

fn normalize_decimal(value: Decimal, scale: u32) -> Result<Decimal, ValueAdmissionError> {
    if scale > Decimal::max_supported_scale() {
        return Err(ValueAdmissionError::ScalarConstraint);
    }
    match value.scale().cmp(&scale) {
        Ordering::Equal => Ok(value),
        Ordering::Less => value
            .scale_to_integer(scale)
            .and_then(|mantissa| Decimal::try_from_i128_with_scale(mantissa, scale))
            .ok_or(ValueAdmissionError::ScalarConstraint),
        Ordering::Greater => Ok(value.round_dp(scale)),
    }
}
