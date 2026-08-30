//! Catalog-backed output materialization for admitted canonical values.
use super::{AcceptedEnumCatalog, EnumValueResolutionError, admission::CanonicalValue};
use crate::value::{CanonicalEnumBody, OutputValue, PublicEnumValue, PublicValue, Value};

/// Resolve one runtime value into its public representation through the
/// immutable accepted catalog that owns its enum IDs.
pub(in crate::db) fn output_value_from_runtime(
    catalog: &AcceptedEnumCatalog,
    value: &Value,
) -> Result<OutputValue, EnumValueResolutionError> {
    output_value_from_canonical(catalog, value).map(OutputValue::from_public)
}

fn output_value_from_canonical(
    catalog: &AcceptedEnumCatalog,
    value: &CanonicalValue,
) -> Result<PublicValue, EnumValueResolutionError> {
    Ok(match value {
        CanonicalValue::Account(value) => PublicValue::Account(*value),
        CanonicalValue::Blob(value) => PublicValue::Blob(value.clone()),
        CanonicalValue::Bool(value) => PublicValue::Bool(*value),
        CanonicalValue::Date(value) => PublicValue::Date(*value),
        CanonicalValue::Decimal(value) => PublicValue::Decimal(*value),
        CanonicalValue::Duration(value) => PublicValue::Duration(*value),
        CanonicalValue::Enum(value) => {
            let selection = catalog.resolve_value(value.canonical())?;
            let payload = match selection.value_body() {
                CanonicalEnumBody::Unit => None,
                CanonicalEnumBody::Payload(payload) => {
                    Some(output_value_from_canonical(catalog, payload)?)
                }
            };
            PublicValue::Enum(PublicEnumValue::from_catalog_parts(
                selection.variant_name(),
                selection.path(),
                payload,
            ))
        }
        CanonicalValue::Float32(value) => PublicValue::Float32(*value),
        CanonicalValue::Float64(value) => PublicValue::Float64(*value),
        CanonicalValue::Int64(value) => PublicValue::Int64(*value),
        CanonicalValue::Int128(value) => PublicValue::Int128(*value),
        CanonicalValue::IntBig(value) => PublicValue::IntBig(value.clone()),
        CanonicalValue::List(values) => PublicValue::List(
            values
                .iter()
                .map(|value| output_value_from_canonical(catalog, value))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        CanonicalValue::Map(entries) => PublicValue::Map(
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
        CanonicalValue::Null => PublicValue::Null,
        CanonicalValue::Principal(value) => PublicValue::Principal(*value),
        CanonicalValue::Subaccount(value) => PublicValue::Subaccount(*value),
        CanonicalValue::Text(value) => PublicValue::Text(value.clone()),
        CanonicalValue::Timestamp(value) => PublicValue::Timestamp(*value),
        CanonicalValue::Nat64(value) => PublicValue::Nat64(*value),
        CanonicalValue::Nat128(value) => PublicValue::Nat128(*value),
        CanonicalValue::NatBig(value) => PublicValue::NatBig(value.clone()),
        CanonicalValue::Ulid(value) => PublicValue::Ulid(*value),
        CanonicalValue::Unit => PublicValue::Unit,
        CanonicalValue::U256(value) => PublicValue::U256(*value),
    })
}
