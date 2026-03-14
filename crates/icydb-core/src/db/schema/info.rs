//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::{
        identity::{EntityName, IndexName},
        index::canonical_index_predicate,
        schema::{FieldType, ValidateError, field_type_from_model_kind, validate},
    },
    model::{
        entity::EntityModel,
        field::FieldKind,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
};
use std::collections::{BTreeMap, BTreeSet};

fn validate_index_field_reference(
    fields: &BTreeMap<String, FieldType>,
    index: &IndexModel,
    field: &'static str,
    seen: &mut BTreeSet<&'static str>,
) -> Result<(), ValidateError> {
    if !fields.contains_key(field) {
        return Err(ValidateError::IndexFieldUnknown {
            index: *index,
            field: field.to_string(),
        });
    }
    if seen.contains(field) {
        return Err(ValidateError::IndexFieldDuplicate {
            index: *index,
            field: field.to_string(),
        });
    }
    seen.insert(field);

    let field_type = fields
        .get(field)
        .expect("index field existence checked above");
    // Guardrail: map fields are deterministic stored values but remain
    // non-queryable and non-indexable in 0.7.
    if matches!(field_type, FieldType::Map { .. }) {
        return Err(ValidateError::IndexFieldMapNotQueryable {
            index: *index,
            field: field.to_string(),
        });
    }
    if !field_type.value_kind().is_queryable() {
        return Err(ValidateError::IndexFieldNotQueryable {
            index: *index,
            field: field.to_string(),
        });
    }

    Ok(())
}

fn validate_index_fields(
    fields: &BTreeMap<String, FieldType>,
    indexes: &[&IndexModel],
) -> Result<(), ValidateError> {
    let mut seen_names = BTreeSet::new();
    for index in indexes {
        if seen_names.contains(index.name()) {
            return Err(ValidateError::DuplicateIndexName {
                name: index.name().to_string(),
            });
        }
        seen_names.insert(index.name());

        let mut seen = BTreeSet::new();
        match index.key_items() {
            IndexKeyItemsRef::Fields(fields_ref) => {
                for field in fields_ref {
                    validate_index_field_reference(fields, index, field, &mut seen)?;
                }
            }
            IndexKeyItemsRef::Items(items) => {
                for item in items {
                    match item {
                        IndexKeyItem::Field(field) => {
                            validate_index_field_reference(fields, index, field, &mut seen)?;
                        }
                        IndexKeyItem::Expression(expression) => {
                            return Err(ValidateError::index_expression_unsupported(
                                **index, expression,
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_index_predicates(
    schema: &SchemaInfo,
    indexes: &[&IndexModel],
) -> Result<(), ValidateError> {
    for index in indexes {
        let Some(predicate_sql) = index.predicate() else {
            continue;
        };

        let predicate = canonical_index_predicate(index)
            .map_err(|_| ValidateError::invalid_index_predicate_syntax(**index, predicate_sql))?;
        let predicate = predicate.expect("index predicate metadata was checked above");
        validate(schema, predicate)
            .map_err(|_| ValidateError::invalid_index_predicate_schema(**index, predicate_sql))?;
    }

    Ok(())
}

///
/// SchemaInfo
///
/// Lightweight, runtime-usable field-type map for one entity.
/// This is the *only* schema surface the predicate validator depends on.
///

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: BTreeMap<String, FieldType>,
    field_kinds: BTreeMap<String, FieldKind>,
}

impl SchemaInfo {
    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        self.fields.get(name)
    }

    #[must_use]
    pub(crate) fn field_kind(&self, name: &str) -> Option<&FieldKind> {
        self.field_kinds.get(name)
    }

    /// Builds runtime predicate schema information from an entity model.
    pub(crate) fn from_entity_model(model: &EntityModel) -> Result<Self, ValidateError> {
        // Validate identity constraints before building schema maps.
        let entity_name = EntityName::try_from_str(model.entity_name).map_err(|err| {
            ValidateError::InvalidEntityName {
                name: model.entity_name.to_string(),
                source: err,
            }
        })?;

        if !model
            .fields
            .iter()
            .any(|field| std::ptr::eq(field, model.primary_key))
        {
            return Err(ValidateError::InvalidPrimaryKey {
                field: model.primary_key.name.to_string(),
            });
        }

        let mut fields = BTreeMap::new();
        let mut field_kinds = BTreeMap::new();
        for field in model.fields {
            if fields.contains_key(field.name) {
                return Err(ValidateError::DuplicateField {
                    field: field.name.to_string(),
                });
            }
            let ty = field_type_from_model_kind(&field.kind);
            fields.insert(field.name.to_string(), ty);
            field_kinds.insert(field.name.to_string(), field.kind);
        }

        let pk_field_type = fields
            .get(model.primary_key.name)
            .expect("primary key verified above");
        if !pk_field_type.is_keyable() {
            return Err(ValidateError::InvalidPrimaryKeyType {
                field: model.primary_key.name.to_string(),
            });
        }

        validate_index_fields(&fields, model.indexes)?;
        for index in model.indexes {
            IndexName::try_from_parts(&entity_name, index.fields()).map_err(|err| {
                ValidateError::InvalidIndexName {
                    index: **index,
                    source: err,
                }
            })?;
        }

        let schema = Self {
            fields,
            field_kinds,
        };
        validate_index_predicates(&schema, model.indexes)?;

        Ok(schema)
    }
}
