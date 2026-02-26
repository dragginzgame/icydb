use crate::{
    db::{
        identity::{EntityName, EntityNameError, IndexName, IndexNameError},
        predicate::{UnsupportedQueryFeature, coercion::CoercionId},
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::db::query::predicate::validate::model::{FieldType, field_type_from_model_kind};

fn validate_index_fields(
    fields: &BTreeMap<String, FieldType>,
    indexes: &[&IndexModel],
) -> Result<(), ValidateError> {
    let mut seen_names = BTreeSet::new();
    for index in indexes {
        if seen_names.contains(index.name) {
            return Err(ValidateError::DuplicateIndexName {
                name: index.name.to_string(),
            });
        }
        seen_names.insert(index.name);

        let mut seen = BTreeSet::new();
        for field in index.fields {
            if !fields.contains_key(*field) {
                return Err(ValidateError::IndexFieldUnknown {
                    index: **index,
                    field: (*field).to_string(),
                });
            }
            if seen.contains(*field) {
                return Err(ValidateError::IndexFieldDuplicate {
                    index: **index,
                    field: (*field).to_string(),
                });
            }
            seen.insert(*field);

            let field_type = fields
                .get(*field)
                .expect("index field existence checked above");
            // Guardrail: map fields are deterministic stored values but remain
            // non-queryable and non-indexable in 0.7.
            if matches!(field_type, FieldType::Map { .. }) {
                return Err(ValidateError::IndexFieldMapNotQueryable {
                    index: **index,
                    field: (*field).to_string(),
                });
            }
            if !field_type.value_kind().is_queryable() {
                return Err(ValidateError::IndexFieldNotQueryable {
                    index: **index,
                    field: (*field).to_string(),
                });
            }
        }
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
            IndexName::try_from_parts(&entity_name, index.fields).map_err(|err| {
                ValidateError::InvalidIndexName {
                    index: **index,
                    source: err,
                }
            })?;
        }

        Ok(Self {
            fields,
            field_kinds,
        })
    }
}

/// Predicate/schema validation failures, including invalid model contracts.
#[derive(Debug, thiserror::Error)]
pub enum ValidateError {
    #[error("invalid entity name '{name}': {source}")]
    InvalidEntityName {
        name: String,
        #[source]
        source: EntityNameError,
    },

    #[error("invalid index name for '{index}': {source}")]
    InvalidIndexName {
        index: IndexModel,
        #[source]
        source: IndexNameError,
    },

    #[error("unknown field '{field}'")]
    UnknownField { field: String },

    #[error("field '{field}' is not queryable")]
    NonQueryableFieldType { field: String },

    #[error("duplicate field '{field}'")]
    DuplicateField { field: String },

    #[error("{0}")]
    UnsupportedQueryFeature(#[from] UnsupportedQueryFeature),

    #[error("primary key '{field}' not present in entity fields")]
    InvalidPrimaryKey { field: String },

    #[error("primary key '{field}' has a non-keyable type")]
    InvalidPrimaryKeyType { field: String },

    #[error("index '{index}' references unknown field '{field}'")]
    IndexFieldUnknown { index: IndexModel, field: String },

    #[error("index '{index}' references non-queryable field '{field}'")]
    IndexFieldNotQueryable { index: IndexModel, field: String },

    #[error(
        "index '{index}' references map field '{field}'; map fields are not queryable in icydb 0.7"
    )]
    IndexFieldMapNotQueryable { index: IndexModel, field: String },

    #[error("index '{index}' repeats field '{field}'")]
    IndexFieldDuplicate { index: IndexModel, field: String },

    #[error("duplicate index name '{name}'")]
    DuplicateIndexName { name: String },

    #[error("operator {op} is not valid for field '{field}'")]
    InvalidOperator { field: String, op: String },

    #[error("coercion {coercion:?} is not valid for field '{field}'")]
    InvalidCoercion { field: String, coercion: CoercionId },

    #[error("invalid literal for field '{field}': {message}")]
    InvalidLiteral { field: String, message: String },
}

impl ValidateError {
    pub(crate) fn invalid_operator(field: &str, op: impl fmt::Display) -> Self {
        Self::InvalidOperator {
            field: field.to_string(),
            op: op.to_string(),
        }
    }

    pub(crate) fn invalid_literal(field: &str, msg: &str) -> Self {
        Self::InvalidLiteral {
            field: field.to_string(),
            message: msg.to_string(),
        }
    }
}
