use crate::{
    db::identity::{EntityName, EntityNameError, IndexName, IndexNameError},
    model::{entity::EntityModel, field::EntityFieldKind, index::IndexModel},
    value::{Value, ValueFamily, ValueFamilyExt},
};
use icydb_schema::{
    node::{
        Entity, Enum, Item, ItemTarget, List, Map, Newtype, Record, Schema, Set, Tuple,
        Value as SValue,
    },
    types::{Cardinality, Primitive},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use super::{
    ast::{CompareOp, ComparePredicate, Predicate},
    coercion::{CoercionId, CoercionSpec, supports_coercion},
};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static SCHEMA_LOOKUP_CALLED: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn reset_schema_lookup_called() {
    SCHEMA_LOOKUP_CALLED.with(|flag| flag.set(false));
}

#[cfg(test)]
pub(crate) fn schema_lookup_called() -> bool {
    SCHEMA_LOOKUP_CALLED.with(Cell::get)
}

///
/// ScalarType
///
/// Internal scalar classification used by predicate validation.
/// This is deliberately *smaller* than the full schema/type system
/// and exists only to support:
/// - coercion rules
/// - literal compatibility checks
/// - operator validity (ordering, equality)
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScalarType {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    E8s,
    E18s,
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Uint,
    Uint128,
    UintBig,
    Ulid,
    Unit,
}

impl ScalarType {
    #[must_use]
    pub const fn family(&self) -> ValueFamily {
        match self {
            Self::Text => ValueFamily::Textual,
            Self::Ulid | Self::Principal | Self::Account => ValueFamily::Identifier,
            Self::Enum => ValueFamily::Enum,
            Self::Blob | Self::Subaccount => ValueFamily::Blob,
            Self::Bool => ValueFamily::Bool,
            Self::Unit => ValueFamily::Unit,
            Self::Date
            | Self::Decimal
            | Self::Duration
            | Self::E8s
            | Self::E18s
            | Self::Float32
            | Self::Float64
            | Self::Int
            | Self::Int128
            | Self::IntBig
            | Self::Timestamp
            | Self::Uint
            | Self::Uint128
            | Self::UintBig => ValueFamily::Numeric,
        }
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        !matches!(self, Self::Blob | Self::Unit)
    }

    #[must_use]
    pub const fn matches_value(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::Account, Value::Account(_))
                | (Self::Blob, Value::Blob(_))
                | (Self::Bool, Value::Bool(_))
                | (Self::Date, Value::Date(_))
                | (Self::Decimal, Value::Decimal(_))
                | (Self::Duration, Value::Duration(_))
                | (Self::Enum, Value::Enum(_))
                | (Self::E8s, Value::E8s(_))
                | (Self::E18s, Value::E18s(_))
                | (Self::Float32, Value::Float32(_))
                | (Self::Float64, Value::Float64(_))
                | (Self::Int, Value::Int(_))
                | (Self::Int128, Value::Int128(_))
                | (Self::IntBig, Value::IntBig(_))
                | (Self::Principal, Value::Principal(_))
                | (Self::Subaccount, Value::Subaccount(_))
                | (Self::Text, Value::Text(_))
                | (Self::Timestamp, Value::Timestamp(_))
                | (Self::Uint, Value::Uint(_))
                | (Self::Uint128, Value::Uint128(_))
                | (Self::UintBig, Value::UintBig(_))
                | (Self::Ulid, Value::Ulid(_))
                | (Self::Unit, Value::Unit)
        )
    }
}

///
/// FieldType
///
/// Reduced runtime type representation used exclusively for predicate validation.
/// This intentionally drops:
/// - record structure
/// - tuple structure
/// - validator/sanitizer metadata
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldType {
    Scalar(ScalarType),
    List(Box<Self>),
    Set(Box<Self>),
    Map { key: Box<Self>, value: Box<Self> },
    Unsupported,
}

impl FieldType {
    #[must_use]
    pub const fn family(&self) -> Option<ValueFamily> {
        match self {
            Self::Scalar(inner) => Some(inner.family()),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(ValueFamily::Collection),
            Self::Unsupported => None,
        }
    }

    #[must_use]
    pub const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarType::Text))
    }

    #[must_use]
    pub const fn is_collection(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_) | Self::Map { .. })
    }

    #[must_use]
    pub const fn is_list_like(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub const fn is_map(&self) -> bool {
        matches!(self, Self::Map { .. })
    }

    #[must_use]
    pub fn map_types(&self) -> Option<(&Self, &Self)> {
        match self {
            Self::Map { key, value } => Some((key.as_ref(), value.as_ref())),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_orderable(),
            _ => false,
        }
    }

    #[must_use]
    pub const fn is_keyable(&self) -> bool {
        matches!(
            self,
            Self::Scalar(
                ScalarType::Account
                    | ScalarType::Int
                    | ScalarType::Principal
                    | ScalarType::Subaccount
                    | ScalarType::Timestamp
                    | ScalarType::Uint
                    | ScalarType::Ulid
                    | ScalarType::Unit
            )
        )
    }
}

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
            // Indexing is hash-based across all Value variants; only Unsupported is rejected here.
            // Collisions are detected during unique enforcement and lookups.
            if matches!(field_type, FieldType::Unsupported) {
                return Err(ValidateError::IndexFieldUnsupported {
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
pub struct SchemaInfo {
    fields: BTreeMap<String, FieldType>,
}

impl SchemaInfo {
    #[must_use]
    #[expect(dead_code)]
    pub(crate) fn new(fields: impl IntoIterator<Item = (String, FieldType)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        self.fields.get(name)
    }

    #[must_use]
    pub fn from_entity_schema(entity: &Entity, schema: &Schema) -> Self {
        let fields = entity
            .fields
            .fields
            .iter()
            .map(|field| {
                let ty = field_type_from_value(&field.value, schema);
                (field.ident.to_string(), ty)
            })
            .collect::<BTreeMap<_, _>>();

        Self { fields }
    }

    pub fn from_entity_model(model: &EntityModel) -> Result<Self, ValidateError> {
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
        for field in model.fields {
            if fields.contains_key(field.name) {
                return Err(ValidateError::DuplicateField {
                    field: field.name.to_string(),
                });
            }
            let ty = field_type_from_model_kind(&field.kind);
            fields.insert(field.name.to_string(), ty);
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

        Ok(Self { fields })
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

    #[error("unsupported field type for '{field}'")]
    UnsupportedFieldType { field: String },

    #[error("duplicate field '{field}'")]
    DuplicateField { field: String },

    #[error("primary key '{field}' not present in entity fields")]
    InvalidPrimaryKey { field: String },

    #[error("primary key '{field}' has an unsupported type")]
    InvalidPrimaryKeyType { field: String },

    #[error("index '{index}' references unknown field '{field}'")]
    IndexFieldUnknown { index: IndexModel, field: String },

    #[error("index '{index}' references unsupported field '{field}'")]
    IndexFieldUnsupported { index: IndexModel, field: String },

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

    #[error("schema unavailable: {0}")]
    SchemaUnavailable(String),
}

pub fn validate(schema: &SchemaInfo, predicate: &Predicate) -> Result<(), ValidateError> {
    match predicate {
        Predicate::True | Predicate::False => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                validate(schema, child)?;
            }
            Ok(())
        }
        Predicate::Not(inner) => validate(schema, inner),
        Predicate::Compare(cmp) => validate_compare(schema, cmp),
        Predicate::IsNull { field } | Predicate::IsMissing { field } => {
            // CONTRACT: presence checks are the only predicates allowed on unsupported fields.
            ensure_field_exists(schema, field).map(|_| ())
        }
        Predicate::IsEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(invalid_operator(field, "is_empty"))
            }
        }
        Predicate::IsNotEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(invalid_operator(field, "is_not_empty"))
            }
        }
        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => validate_map_key(schema, field, key, coercion),
        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => validate_map_value(schema, field, value, coercion),
        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => validate_map_entry(schema, field, key, value, coercion),
        Predicate::TextContains { field, value } => {
            validate_text_contains(schema, field, value, "text_contains")
        }
        Predicate::TextContainsCi { field, value } => {
            validate_text_contains(schema, field, value, "text_contains_ci")
        }
    }
}

pub fn validate_model(model: &EntityModel, predicate: &Predicate) -> Result<(), ValidateError> {
    let schema = SchemaInfo::from_entity_model(model)?;
    validate(&schema, predicate)
}

fn validate_compare(schema: &SchemaInfo, cmp: &ComparePredicate) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, &cmp.field)?;

    match cmp.op {
        CompareOp::Eq | CompareOp::Ne => {
            validate_eq_ne(&cmp.field, field_type, &cmp.value, &cmp.coercion)
        }
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            validate_ordering(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
        CompareOp::In | CompareOp::NotIn => {
            validate_in(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
        CompareOp::Contains => validate_contains(&cmp.field, field_type, &cmp.value, &cmp.coercion),
        CompareOp::StartsWith | CompareOp::EndsWith => {
            validate_text_compare(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
    }
}

fn validate_eq_ne(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_list_like() {
        ensure_list_literal(field, value, field_type)?;
    } else if field_type.is_map() {
        ensure_map_literal(field, value, field_type)?;
    } else if matches!(value, Value::List(_)) {
        return Err(invalid_literal(field, "expected scalar literal"));
    }

    ensure_coercion(field, field_type, value, coercion)
}

fn validate_ordering(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::CollectionElement) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !field_type.is_orderable() {
        return Err(invalid_operator(field, format!("{op:?}")));
    }

    if matches!(value, Value::List(_)) {
        return Err(invalid_literal(field, "expected scalar literal"));
    }

    ensure_coercion(field, field_type, value, coercion)
}

/// Validate list membership predicates.
fn validate_in(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if field_type.is_collection() {
        return Err(invalid_operator(field, format!("{op:?}")));
    }

    let Value::List(items) = value else {
        return Err(invalid_literal(field, "expected list literal"));
    };

    for item in items {
        ensure_coercion(field, field_type, item, coercion)?;
    }

    Ok(())
}

/// Validate collection containment predicates on list/set fields.
fn validate_contains(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_text() {
        // CONTRACT: text substring matching uses TextContains/TextContainsCi only.
        return Err(invalid_operator(
            field,
            format!("{:?}", CompareOp::Contains),
        ));
    }

    let element_type = match field_type {
        FieldType::List(inner) | FieldType::Set(inner) => inner.as_ref(),
        _ => {
            return Err(invalid_operator(
                field,
                format!("{:?}", CompareOp::Contains),
            ));
        }
    };

    if matches!(coercion.id, CoercionId::TextCasefold) {
        // CONTRACT: case-insensitive coercion never applies to structured values.
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    ensure_coercion(field, element_type, value, coercion)
}

/// Validate text prefix/suffix comparisons.
fn validate_text_compare(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if !field_type.is_text() {
        return Err(invalid_operator(field, format!("{op:?}")));
    }

    if !matches!(value, Value::Text(_)) {
        return Err(invalid_literal(field, "expected text literal"));
    }

    ensure_coercion(field, field_type, value, coercion)
}

// Ensure a field exists and is a map, returning key/value types.
fn ensure_map_types<'a>(
    schema: &'a SchemaInfo,
    field: &str,
    op: &str,
) -> Result<(&'a FieldType, &'a FieldType), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    field_type
        .map_types()
        .ok_or_else(|| invalid_operator(field, op))
}

fn validate_map_key(
    schema: &SchemaInfo,
    field: &str,
    key: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    let (key_type, _) = ensure_map_types(schema, field, "map_contains_key")?;

    ensure_coercion(field, key_type, key, coercion)
}

fn validate_map_value(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    let (_, value_type) = ensure_map_types(schema, field, "map_contains_value")?;

    ensure_coercion(field, value_type, value, coercion)
}

fn validate_map_entry(
    schema: &SchemaInfo,
    field: &str,
    key: &Value,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    let (key_type, value_type) = ensure_map_types(schema, field, "map_contains_entry")?;

    ensure_coercion(field, key_type, key, coercion)?;
    ensure_coercion(field, value_type, value, coercion)?;

    Ok(())
}

/// Validate substring predicates on text fields.
fn validate_text_contains(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    op: &str,
) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    if !field_type.is_text() {
        return Err(invalid_operator(field, op));
    }

    if !matches!(value, Value::Text(_)) {
        return Err(invalid_literal(field, "expected text literal"));
    }

    Ok(())
}

fn ensure_field<'a>(schema: &'a SchemaInfo, field: &str) -> Result<&'a FieldType, ValidateError> {
    let field_type = schema
        .field(field)
        .ok_or_else(|| ValidateError::UnknownField {
            field: field.to_string(),
        })?;

    if matches!(field_type, FieldType::Unsupported) {
        return Err(ValidateError::UnsupportedFieldType {
            field: field.to_string(),
        });
    }

    Ok(field_type)
}

fn ensure_field_exists<'a>(
    schema: &'a SchemaInfo,
    field: &str,
) -> Result<&'a FieldType, ValidateError> {
    schema
        .field(field)
        .ok_or_else(|| ValidateError::UnknownField {
            field: field.to_string(),
        })
}

fn invalid_operator(field: &str, op: impl fmt::Display) -> ValidateError {
    ValidateError::InvalidOperator {
        field: field.to_string(),
        op: op.to_string(),
    }
}

fn invalid_literal(field: &str, msg: &str) -> ValidateError {
    ValidateError::InvalidLiteral {
        field: field.to_string(),
        message: msg.to_string(),
    }
}

fn ensure_coercion(
    field: &str,
    field_type: &FieldType,
    literal: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    let left_family = field_type
        .family()
        .ok_or_else(|| ValidateError::UnsupportedFieldType {
            field: field.to_string(),
        })?;
    let right_family = literal.family();

    if matches!(coercion.id, CoercionId::TextCasefold) && !field_type.is_text() {
        // CONTRACT: case-insensitive coercions are text-only.
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !supports_coercion(left_family, right_family, coercion.id) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if matches!(
        coercion.id,
        CoercionId::Strict | CoercionId::CollectionElement
    ) && !literal_matches_type(literal, field_type)
    {
        return Err(invalid_literal(
            field,
            "literal type does not match field type",
        ));
    }

    Ok(())
}

fn ensure_list_literal(
    field: &str,
    literal: &Value,
    field_type: &FieldType,
) -> Result<(), ValidateError> {
    if !literal_matches_type(literal, field_type) {
        return Err(invalid_literal(
            field,
            "list literal does not match field element type",
        ));
    }

    Ok(())
}

fn ensure_map_literal(
    field: &str,
    literal: &Value,
    field_type: &FieldType,
) -> Result<(), ValidateError> {
    if !literal_matches_type(literal, field_type) {
        return Err(invalid_literal(
            field,
            "map literal does not match field key/value types",
        ));
    }

    Ok(())
}

pub(crate) fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Scalar(inner) => inner.matches_value(literal),
        FieldType::List(element) | FieldType::Set(element) => match literal {
            Value::List(items) => items.iter().all(|item| literal_matches_type(item, element)),
            _ => false,
        },
        FieldType::Map { key, value } => match literal {
            Value::List(entries) => entries.iter().all(|entry| match entry {
                Value::List(pair) if pair.len() == 2 => {
                    literal_matches_type(&pair[0], key) && literal_matches_type(&pair[1], value)
                }
                _ => false,
            }),
            _ => false,
        },
        FieldType::Unsupported => false,
    }
}

fn field_type_from_value(value: &SValue, schema: &Schema) -> FieldType {
    let base = field_type_from_item(&value.item, schema);

    match value.cardinality {
        Cardinality::Many => FieldType::List(Box::new(base)),
        Cardinality::One | Cardinality::Opt => base,
    }
}

fn field_type_from_item(item: &Item, schema: &Schema) -> FieldType {
    match &item.target {
        ItemTarget::Primitive(prim) => FieldType::Scalar(scalar_from_primitive(*prim)),
        ItemTarget::Is(path) => {
            if schema.cast_node::<Enum>(path).is_ok() {
                return FieldType::Scalar(ScalarType::Enum);
            }
            if let Ok(node) = schema.cast_node::<Newtype>(path) {
                return field_type_from_item(&node.item, schema);
            }
            if let Ok(node) = schema.cast_node::<List>(path) {
                return FieldType::List(Box::new(field_type_from_item(&node.item, schema)));
            }
            if let Ok(node) = schema.cast_node::<Set>(path) {
                return FieldType::Set(Box::new(field_type_from_item(&node.item, schema)));
            }
            if let Ok(node) = schema.cast_node::<Map>(path) {
                let key = field_type_from_item(&node.key, schema);
                let value = field_type_from_value(&node.value, schema);
                return FieldType::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                };
            }
            if schema.cast_node::<Record>(path).is_ok() {
                return FieldType::Unsupported;
            }
            if schema.cast_node::<Tuple>(path).is_ok() {
                return FieldType::Unsupported;
            }

            FieldType::Unsupported
        }
    }
}

const fn scalar_from_primitive(prim: Primitive) -> ScalarType {
    match prim {
        Primitive::Account => ScalarType::Account,
        Primitive::Blob => ScalarType::Blob,
        Primitive::Bool => ScalarType::Bool,
        Primitive::Date => ScalarType::Date,
        Primitive::Decimal => ScalarType::Decimal,
        Primitive::Duration => ScalarType::Duration,
        Primitive::E8s => ScalarType::E8s,
        Primitive::E18s => ScalarType::E18s,
        Primitive::Float32 => ScalarType::Float32,
        Primitive::Float64 => ScalarType::Float64,
        Primitive::Int => ScalarType::IntBig,
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => ScalarType::Int,
        Primitive::Int128 => ScalarType::Int128,
        Primitive::Nat => ScalarType::UintBig,
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            ScalarType::Uint
        }
        Primitive::Nat128 => ScalarType::Uint128,
        Primitive::Principal => ScalarType::Principal,
        Primitive::Subaccount => ScalarType::Subaccount,
        Primitive::Text => ScalarType::Text,
        Primitive::Timestamp => ScalarType::Timestamp,
        Primitive::Ulid => ScalarType::Ulid,
        Primitive::Unit => ScalarType::Unit,
    }
}

fn field_type_from_model_kind(kind: &EntityFieldKind) -> FieldType {
    match kind {
        EntityFieldKind::Account => FieldType::Scalar(ScalarType::Account),
        EntityFieldKind::Blob => FieldType::Scalar(ScalarType::Blob),
        EntityFieldKind::Bool => FieldType::Scalar(ScalarType::Bool),
        EntityFieldKind::Date => FieldType::Scalar(ScalarType::Date),
        EntityFieldKind::Decimal => FieldType::Scalar(ScalarType::Decimal),
        EntityFieldKind::Duration => FieldType::Scalar(ScalarType::Duration),
        EntityFieldKind::Enum => FieldType::Scalar(ScalarType::Enum),
        EntityFieldKind::E8s => FieldType::Scalar(ScalarType::E8s),
        EntityFieldKind::E18s => FieldType::Scalar(ScalarType::E18s),
        EntityFieldKind::Float32 => FieldType::Scalar(ScalarType::Float32),
        EntityFieldKind::Float64 => FieldType::Scalar(ScalarType::Float64),
        EntityFieldKind::Int => FieldType::Scalar(ScalarType::Int),
        EntityFieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        EntityFieldKind::IntBig => FieldType::Scalar(ScalarType::IntBig),
        EntityFieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        EntityFieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        EntityFieldKind::Text => FieldType::Scalar(ScalarType::Text),
        EntityFieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        EntityFieldKind::Uint => FieldType::Scalar(ScalarType::Uint),
        EntityFieldKind::Uint128 => FieldType::Scalar(ScalarType::Uint128),
        EntityFieldKind::UintBig => FieldType::Scalar(ScalarType::UintBig),
        EntityFieldKind::Ulid => FieldType::Scalar(ScalarType::Ulid),
        EntityFieldKind::Unit => FieldType::Scalar(ScalarType::Unit),
        EntityFieldKind::List(inner) => {
            FieldType::List(Box::new(field_type_from_model_kind(inner)))
        }
        EntityFieldKind::Set(inner) => FieldType::Set(Box::new(field_type_from_model_kind(inner))),
        EntityFieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_model_kind(key)),
            value: Box::new(field_type_from_model_kind(value)),
        },
        EntityFieldKind::Unsupported => FieldType::Unsupported,
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(inner) => write!(f, "{inner:?}"),
            Self::List(inner) => write!(f, "List<{inner}>"),
            Self::Set(inner) => write!(f, "Set<{inner}>"),
            Self::Map { key, value } => write!(f, "Map<{key}, {value}>"),
            Self::Unsupported => write!(f, "Unsupported"),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{ValidateError, validate_model};
    use crate::{
        db::query::{
            FieldRef,
            predicate::{CoercionId, Predicate},
        },
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
            index::IndexModel,
        },
        types::Ulid,
    };

    fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
        EntityFieldModel { name, kind }
    }

    fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel {
        let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
        let primary_key = &fields[pk_index];
        let indexes: &'static [&'static IndexModel] = &[];

        EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key,
            fields,
            indexes,
        }
    }

    #[test]
    fn validate_model_accepts_scalars_and_coercions() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("email", EntityFieldKind::Text),
                field("age", EntityFieldKind::Uint),
                field("created_at", EntityFieldKind::Timestamp),
                field("active", EntityFieldKind::Bool),
            ],
            0,
        );

        let predicate = Predicate::And(vec![
            FieldRef::new("id").eq(Ulid::nil()),
            FieldRef::new("email").text_eq_ci("User@example.com"),
            FieldRef::new("age").lt(30u32),
        ]);

        assert!(validate_model(&model, &predicate).is_ok());
    }

    #[test]
    fn validate_model_accepts_collections_and_map_contains() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("tags", EntityFieldKind::List(&EntityFieldKind::Text)),
                field(
                    "principals",
                    EntityFieldKind::Set(&EntityFieldKind::Principal),
                ),
                field(
                    "attributes",
                    EntityFieldKind::Map {
                        key: &EntityFieldKind::Text,
                        value: &EntityFieldKind::Uint,
                    },
                ),
            ],
            0,
        );

        let predicate = Predicate::And(vec![
            FieldRef::new("tags").is_empty(),
            FieldRef::new("principals").is_not_empty(),
            FieldRef::new("attributes").map_contains_entry("k", 1u64, CoercionId::Strict),
        ]);

        assert!(validate_model(&model, &predicate).is_ok());

        let bad =
            FieldRef::new("attributes").map_contains_entry("k", 1u64, CoercionId::TextCasefold);

        assert!(matches!(
            validate_model(&model, &bad),
            Err(ValidateError::InvalidCoercion { .. })
        ));
    }

    #[test]
    fn validate_model_rejects_unsupported_fields() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("broken", EntityFieldKind::Unsupported),
            ],
            0,
        );

        let predicate = FieldRef::new("broken").eq(1u64);

        assert!(matches!(
            validate_model(&model, &predicate),
            Err(ValidateError::UnsupportedFieldType { field }) if field == "broken"
        ));
    }

    #[test]
    fn validate_model_accepts_text_contains() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("email", EntityFieldKind::Text),
            ],
            0,
        );

        let predicate = FieldRef::new("email").text_contains("example");
        assert!(validate_model(&model, &predicate).is_ok());

        let predicate = FieldRef::new("email").text_contains_ci("EXAMPLE");
        assert!(validate_model(&model, &predicate).is_ok());
    }

    #[test]
    fn validate_model_rejects_text_contains_on_non_text() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("age", EntityFieldKind::Uint),
            ],
            0,
        );

        let predicate = FieldRef::new("age").text_contains("1");
        assert!(matches!(
            validate_model(&model, &predicate),
            Err(ValidateError::InvalidOperator { field, op })
                if field == "age" && op == "text_contains"
        ));
    }
}
