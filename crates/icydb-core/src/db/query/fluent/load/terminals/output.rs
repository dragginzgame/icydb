//! Module: query::fluent::load::terminals::output
//! Responsibility: convert runtime projection values into facade output values.
//! Does not own: terminal execution, projection planning, or row materialization.
//! Boundary: preserves terminal result order while mapping value representations.

use crate::{
    db::{
        PersistedRow, QueryError,
        schema::output_value_from_runtime,
        session::{AcceptedIdValuesOutput, AcceptedOptionalValueOutput, AcceptedValuesOutput},
    },
    types::Id,
    value::OutputValue,
};

// Convert one ordered runtime projection vector into the public output form.
pub(super) fn output_values(
    accepted: AcceptedValuesOutput,
) -> Result<Vec<OutputValue>, QueryError> {
    let (values, value_catalog) = accepted.into_parts();
    values
        .iter()
        .map(|value| {
            output_value_from_runtime(value_catalog.enum_catalog(), value)
                .map_err(|_error| QueryError::invariant())
        })
        .collect()
}

// Convert one ordered runtime `(id, value)` projection vector into the public output form.
pub(super) fn output_values_with_ids<E: PersistedRow>(
    accepted: AcceptedIdValuesOutput<E>,
) -> Result<Vec<(Id<E>, OutputValue)>, QueryError> {
    let (values, value_catalog) = accepted.into_parts();
    values
        .into_iter()
        .map(|(id, value)| {
            output_value_from_runtime(value_catalog.enum_catalog(), &value)
                .map(|output| (id, output))
                .map_err(|_error| QueryError::invariant())
        })
        .collect()
}

// Convert one optional runtime projection through the catalog retained by the
// guarded plan that produced it.
pub(super) fn output_optional(
    accepted: AcceptedOptionalValueOutput,
) -> Result<Option<OutputValue>, QueryError> {
    let (value, value_catalog) = accepted.into_parts();

    value
        .as_ref()
        .map(|value| {
            output_value_from_runtime(value_catalog.enum_catalog(), value)
                .map_err(|_error| QueryError::invariant())
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::schema::{
            AcceptedSchemaRevision, AcceptedValueCatalogHandle,
            build_initial_accepted_enum_catalog_from_kinds_for_tests,
        },
        db::session::AcceptedExecutionOutput,
        model::field::{EnumVariantModel, FieldKind, FieldStorageDecode},
        value::{CanonicalEnumBody, Value, ValueEnum},
    };

    static VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
        "Ready",
        None,
        FieldStorageDecode::ByKind,
    )];
    static ENUM_KIND: FieldKind = FieldKind::Enum {
        path: "output::Status",
        variants: &VARIANTS,
    };

    #[test]
    fn output_values_resolve_enum_ids_through_bundled_plan_catalog() {
        let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[ENUM_KIND])
            .expect("output test catalog should build");
        let type_id = catalog
            .type_id("output::Status")
            .expect("output enum type should exist");
        let variant_id = catalog
            .enum_type(type_id)
            .and_then(|definition| definition.variant_id("Ready"))
            .expect("output enum variant should exist");
        let handle = AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let accepted = AcceptedExecutionOutput::new(
            vec![Value::Enum(ValueEnum::new(
                type_id,
                variant_id,
                CanonicalEnumBody::Unit,
            ))],
            handle,
        );

        let output = output_values(accepted).expect("bundled catalog should resolve enum output");
        let [OutputValue::Enum(output)] = output.as_slice() else {
            panic!("bundled catalog should produce one enum output");
        };
        assert_eq!(output.variant(), "Ready");
        assert_eq!(output.path(), Some("output::Status"));
        assert_eq!(output.payload(), None);
    }
}
