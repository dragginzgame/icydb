//! Catalog-backed output materialization for admitted canonical values.

#[cfg(test)]
mod tests;

use super::{AcceptedEnumCatalog, EnumValueResolutionError, admission::CanonicalValue};
use crate::value::{CanonicalEnumBody, OutputValue, PublicEnumValue, PublicValue, Value};

/// Consume one runtime value into its public representation through the
/// immutable accepted catalog that owns its enum IDs.
pub(in crate::db) fn output_value_from_runtime(
    catalog: &AcceptedEnumCatalog,
    value: Value,
) -> Result<OutputValue, EnumValueResolutionError> {
    output_value_from_canonical(catalog, value).map(OutputValue::from_public)
}

fn output_value_from_canonical(
    catalog: &AcceptedEnumCatalog,
    value: CanonicalValue,
) -> Result<PublicValue, EnumValueResolutionError> {
    Ok(match value {
        CanonicalValue::Account(value) => PublicValue::Account(value),
        CanonicalValue::Blob(value) => PublicValue::Blob(value),
        CanonicalValue::Bool(value) => PublicValue::Bool(value),
        CanonicalValue::Date(value) => PublicValue::Date(value),
        CanonicalValue::Decimal(value) => PublicValue::Decimal(value),
        CanonicalValue::Duration(value) => PublicValue::Duration(value),
        CanonicalValue::Enum(value) => {
            let selection = catalog.resolve_value(value.canonical())?;
            // Labels borrow the catalog, not the runtime value. Resolve first,
            // then move the body without cloning it or repeating ID lookup.
            let variant_name = selection.variant_name();
            let path = selection.path();
            let payload = match value.into_body() {
                CanonicalEnumBody::Unit => None,
                CanonicalEnumBody::Payload(payload) => {
                    Some(output_value_from_canonical(catalog, *payload)?)
                }
            };
            PublicValue::Enum(PublicEnumValue::from_catalog_parts(
                variant_name,
                path,
                payload,
            ))
        }
        CanonicalValue::Float32(value) => PublicValue::Float32(value),
        CanonicalValue::Float64(value) => PublicValue::Float64(value),
        CanonicalValue::Int64(value) => PublicValue::Int64(value),
        CanonicalValue::Int128(value) => PublicValue::Int128(value),
        CanonicalValue::IntBig(value) => PublicValue::IntBig(value),
        CanonicalValue::List(values) => PublicValue::List(
            values
                .into_iter()
                .map(|value| output_value_from_canonical(catalog, value))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        CanonicalValue::Map(entries) => PublicValue::Map(
            entries
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        output_value_from_canonical(catalog, key)?,
                        output_value_from_canonical(catalog, value)?,
                    ))
                })
                .collect::<Result<Vec<_>, EnumValueResolutionError>>()?,
        ),
        CanonicalValue::Null => PublicValue::Null,
        CanonicalValue::Principal(value) => PublicValue::Principal(value),
        CanonicalValue::Subaccount(value) => PublicValue::Subaccount(value),
        CanonicalValue::Text(value) => PublicValue::Text(value),
        CanonicalValue::Timestamp(value) => PublicValue::Timestamp(value),
        CanonicalValue::Nat64(value) => PublicValue::Nat64(value),
        CanonicalValue::Nat128(value) => PublicValue::Nat128(value),
        CanonicalValue::NatBig(value) => PublicValue::NatBig(value),
        CanonicalValue::Ulid(value) => PublicValue::Ulid(value),
        CanonicalValue::Unit => PublicValue::Unit,
        CanonicalValue::U256(value) => PublicValue::U256(value),
    })
}
