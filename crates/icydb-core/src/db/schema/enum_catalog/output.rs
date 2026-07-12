//! Catalog-backed output materialization for admitted canonical values.
use super::{AcceptedEnumCatalog, EnumValueResolutionError, admission::CanonicalValue};
use crate::value::{CanonicalEnumBody, OutputValue, OutputValueEnum, Value};

/// Resolve one runtime value into its public representation through the
/// immutable accepted catalog that owns its enum IDs.
pub(in crate::db) fn output_value_from_runtime(
    catalog: &AcceptedEnumCatalog,
    value: &Value,
) -> Result<OutputValue, EnumValueResolutionError> {
    output_value_from_canonical(catalog, value)
}

fn output_value_from_canonical(
    catalog: &AcceptedEnumCatalog,
    value: &CanonicalValue,
) -> Result<OutputValue, EnumValueResolutionError> {
    Ok(match value {
        CanonicalValue::Account(value) => OutputValue::Account(*value),
        CanonicalValue::Blob(value) => OutputValue::Blob(value.clone()),
        CanonicalValue::Bool(value) => OutputValue::Bool(*value),
        CanonicalValue::Date(value) => OutputValue::Date(*value),
        CanonicalValue::Decimal(value) => OutputValue::Decimal(*value),
        CanonicalValue::Duration(value) => OutputValue::Duration(*value),
        CanonicalValue::Enum(value) => {
            let selection = catalog.resolve_value(value.canonical())?;
            let payload = match selection.value_body() {
                CanonicalEnumBody::Unit => None,
                CanonicalEnumBody::Payload(payload) => {
                    Some(output_value_from_canonical(catalog, payload)?)
                }
            };
            OutputValue::Enum(OutputValueEnum::from_catalog_parts(
                selection.variant_name(),
                selection.path(),
                payload,
            ))
        }
        CanonicalValue::Float32(value) => OutputValue::Float32(*value),
        CanonicalValue::Float64(value) => OutputValue::Float64(*value),
        CanonicalValue::Int64(value) => OutputValue::Int64(*value),
        CanonicalValue::Int128(value) => OutputValue::Int128(*value),
        CanonicalValue::IntBig(value) => OutputValue::IntBig(value.clone()),
        CanonicalValue::List(values) => OutputValue::List(
            values
                .iter()
                .map(|value| output_value_from_canonical(catalog, value))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        CanonicalValue::Map(entries) => OutputValue::Map(
            entries
                .iter()
                .map(|(key, value)| {
                    Ok((
                        output_value_from_canonical(catalog, key)?,
                        output_value_from_canonical(catalog, value)?,
                    ))
                })
                .collect::<Result<Vec<_>, EnumValueResolutionError>>()?,
        ),
        CanonicalValue::Null => OutputValue::Null,
        CanonicalValue::Principal(value) => OutputValue::Principal(*value),
        CanonicalValue::Subaccount(value) => OutputValue::Subaccount(*value),
        CanonicalValue::Text(value) => OutputValue::Text(value.clone()),
        CanonicalValue::Timestamp(value) => OutputValue::Timestamp(*value),
        CanonicalValue::Nat64(value) => OutputValue::Nat64(*value),
        CanonicalValue::Nat128(value) => OutputValue::Nat128(*value),
        CanonicalValue::NatBig(value) => OutputValue::NatBig(value.clone()),
        CanonicalValue::Ulid(value) => OutputValue::Ulid(*value),
        CanonicalValue::Unit => OutputValue::Unit,
    })
}
