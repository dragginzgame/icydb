use super::{
    ast::{CompareOp, ComparePredicate, Predicate},
    coercion::{CoercionId, CoercionSpec, supports_coercion},
};
use crate::{
    db::identity::{EntityName, EntityNameError, IndexName, IndexNameError},
    model::{entity::EntityModel, field::EntityFieldKind, index::IndexModel},
    value::{Value, ValueFamily, ValueFamilyExt},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

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

// Local helpers to expand the scalar registry into match arms.
macro_rules! scalar_family_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $family, )*
        }
    };
}

macro_rules! scalar_matches_value_from_registry {
    ( @args $self:expr, $value:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        matches!(
            ($self, $value),
            $( (ScalarType::$scalar, $value_pat) )|*
        )
    };
}

#[cfg(test)]
macro_rules! scalar_supports_arithmetic_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_arithmetic, )*
        }
    };
}

macro_rules! scalar_is_keyable_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $is_keyable, )*
        }
    };
}

#[cfg(test)]
macro_rules! scalar_supports_equality_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_equality, )*
        }
    };
}

macro_rules! scalar_supports_ordering_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_ordering, )*
        }
    };
}

impl ScalarType {
    #[must_use]
    pub const fn family(&self) -> ValueFamily {
        scalar_registry!(scalar_family_from_registry, self)
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        self.supports_ordering()
    }

    #[must_use]
    pub const fn matches_value(&self, value: &Value) -> bool {
        scalar_registry!(scalar_matches_value_from_registry, self, value)
    }

    #[must_use]
    #[cfg(test)]
    pub const fn supports_arithmetic(&self) -> bool {
        scalar_registry!(scalar_supports_arithmetic_from_registry, self)
    }

    #[must_use]
    pub const fn is_keyable(&self) -> bool {
        scalar_registry!(scalar_is_keyable_from_registry, self)
    }

    #[must_use]
    #[cfg(test)]
    pub const fn supports_equality(&self) -> bool {
        scalar_registry!(scalar_supports_equality_from_registry, self)
    }

    #[must_use]
    pub const fn supports_ordering(&self) -> bool {
        scalar_registry!(scalar_supports_ordering_from_registry, self)
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
            _ => {
                // NOTE: Only map field types expose key/value type pairs.
                None
            }
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
        match self {
            Self::Scalar(inner) => inner.is_keyable(),
            _ => false,
        }
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
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        self.fields.get(name)
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
    } else {
        ensure_scalar_literal(field, value)?;
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

    ensure_scalar_literal(field, value)?;

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

    ensure_text_literal(field, value)?;

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
    ensure_no_text_casefold(field, coercion)?;

    let (key_type, _) = ensure_map_types(schema, field, "map_contains_key")?;

    ensure_coercion(field, key_type, key, coercion)
}

fn validate_map_value(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    ensure_no_text_casefold(field, coercion)?;

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
    ensure_no_text_casefold(field, coercion)?;

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

    ensure_text_literal(field, value)?;

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

// Reject unsupported case-insensitive coercions for non-text comparisons.
fn ensure_no_text_casefold(field: &str, coercion: &CoercionSpec) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    Ok(())
}

// Ensure the literal is text to match text-only operators.
fn ensure_text_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if !matches!(value, Value::Text(_)) {
        return Err(invalid_literal(field, "expected text literal"));
    }

    Ok(())
}

// Reject list literals when scalar comparisons are required.
fn ensure_scalar_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if matches!(value, Value::List(_)) {
        return Err(invalid_literal(field, "expected scalar literal"));
    }

    Ok(())
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
        FieldType::Unsupported => {
            // NOTE: Unsupported field types never match predicate literals.
            false
        }
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
        EntityFieldKind::Ref { key_kind, .. } => field_type_from_model_kind(key_kind),
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
    // NOTE: Invalid helpers remain only for intentionally invalid or unsupported schemas.
    use super::{
        CompareOp, FieldType, ScalarType, ValidateError, validate_model, validate_ordering,
    };
    use crate::{
        db::query::{
            FieldRef,
            predicate::{CoercionId, CoercionSpec, Predicate},
        },
        model::field::{EntityFieldKind, EntityFieldModel},
        test_fixtures::InvalidEntityModelBuilder,
        traits::EntitySchema,
        types::{
            Account, Date, Decimal, Duration, E8s, E18s, Float32, Float64, Int, Int128, Nat,
            Nat128, Principal, Subaccount, Timestamp, Ulid,
        },
        value::{Value, ValueEnum, ValueFamily},
    };
    use std::collections::BTreeSet;

    /// Build a registry-driven list of all scalar variants.
    fn registry_scalars() -> Vec<ScalarType> {
        macro_rules! collect_scalars {
            ( @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
                vec![ $( ScalarType::$scalar ),* ]
            };
            ( @args $($ignore:tt)*; @entries $( ($scalar:ident, $family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr) ),* $(,)? ) => {
                vec![ $( ScalarType::$scalar ),* ]
            };
        }

        let scalars = scalar_registry!(collect_scalars);

        scalars
    }

    /// Returns the total count of ScalarType variants.
    const SCALAR_TYPE_VARIANT_COUNT: usize = 23;

    /// Map each ScalarType variant to a stable index.
    fn scalar_index(scalar: ScalarType) -> usize {
        match scalar {
            ScalarType::Account => 0,
            ScalarType::Blob => 1,
            ScalarType::Bool => 2,
            ScalarType::Date => 3,
            ScalarType::Decimal => 4,
            ScalarType::Duration => 5,
            ScalarType::Enum => 6,
            ScalarType::E8s => 7,
            ScalarType::E18s => 8,
            ScalarType::Float32 => 9,
            ScalarType::Float64 => 10,
            ScalarType::Int => 11,
            ScalarType::Int128 => 12,
            ScalarType::IntBig => 13,
            ScalarType::Principal => 14,
            ScalarType::Subaccount => 15,
            ScalarType::Text => 16,
            ScalarType::Timestamp => 17,
            ScalarType::Uint => 18,
            ScalarType::Uint128 => 19,
            ScalarType::UintBig => 20,
            ScalarType::Ulid => 21,
            ScalarType::Unit => 22,
        }
    }

    /// Return every ScalarType variant by index, ensuring exhaustiveness.
    fn scalar_from_index(index: usize) -> Option<ScalarType> {
        let scalar = match index {
            0 => ScalarType::Account,
            1 => ScalarType::Blob,
            2 => ScalarType::Bool,
            3 => ScalarType::Date,
            4 => ScalarType::Decimal,
            5 => ScalarType::Duration,
            6 => ScalarType::Enum,
            7 => ScalarType::E8s,
            8 => ScalarType::E18s,
            9 => ScalarType::Float32,
            10 => ScalarType::Float64,
            11 => ScalarType::Int,
            12 => ScalarType::Int128,
            13 => ScalarType::IntBig,
            14 => ScalarType::Principal,
            15 => ScalarType::Subaccount,
            16 => ScalarType::Text,
            17 => ScalarType::Timestamp,
            18 => ScalarType::Uint,
            19 => ScalarType::Uint128,
            20 => ScalarType::UintBig,
            21 => ScalarType::Ulid,
            22 => ScalarType::Unit,
            _ => return None,
        };

        Some(scalar)
    }

    /// Legacy family mapping from the pre-registry implementation.
    fn legacy_family(scalar: ScalarType) -> ValueFamily {
        match scalar {
            ScalarType::Text => ValueFamily::Textual,
            ScalarType::Ulid | ScalarType::Principal | ScalarType::Account => {
                ValueFamily::Identifier
            }
            ScalarType::Enum => ValueFamily::Enum,
            ScalarType::Blob | ScalarType::Subaccount => ValueFamily::Blob,
            ScalarType::Bool => ValueFamily::Bool,
            ScalarType::Unit => ValueFamily::Unit,
            ScalarType::Date
            | ScalarType::Decimal
            | ScalarType::Duration
            | ScalarType::E8s
            | ScalarType::E18s
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int
            | ScalarType::Int128
            | ScalarType::IntBig
            | ScalarType::Timestamp
            | ScalarType::Uint
            | ScalarType::Uint128
            | ScalarType::UintBig => ValueFamily::Numeric,
        }
    }

    /// Legacy value matching from the pre-registry implementation.
    fn legacy_matches_value(scalar: ScalarType, value: &Value) -> bool {
        let matches_value = matches!(
            (scalar, value),
            (ScalarType::Account, Value::Account(_))
                | (ScalarType::Blob, Value::Blob(_))
                | (ScalarType::Bool, Value::Bool(_))
                | (ScalarType::Date, Value::Date(_))
                | (ScalarType::Decimal, Value::Decimal(_))
                | (ScalarType::Duration, Value::Duration(_))
                | (ScalarType::Enum, Value::Enum(_))
                | (ScalarType::E8s, Value::E8s(_))
                | (ScalarType::E18s, Value::E18s(_))
                | (ScalarType::Float32, Value::Float32(_))
                | (ScalarType::Float64, Value::Float64(_))
                | (ScalarType::Int, Value::Int(_))
                | (ScalarType::Int128, Value::Int128(_))
                | (ScalarType::IntBig, Value::IntBig(_))
                | (ScalarType::Principal, Value::Principal(_))
                | (ScalarType::Subaccount, Value::Subaccount(_))
                | (ScalarType::Text, Value::Text(_))
                | (ScalarType::Timestamp, Value::Timestamp(_))
                | (ScalarType::Uint, Value::Uint(_))
                | (ScalarType::Uint128, Value::Uint128(_))
                | (ScalarType::UintBig, Value::UintBig(_))
                | (ScalarType::Ulid, Value::Ulid(_))
                | (ScalarType::Unit, Value::Unit)
        );

        matches_value
    }

    /// Legacy arithmetic support from the pre-registry model.
    fn legacy_supports_arithmetic(scalar: ScalarType) -> bool {
        matches!(
            scalar,
            ScalarType::Decimal
                | ScalarType::E8s
                | ScalarType::E18s
                | ScalarType::Int
                | ScalarType::Int128
                | ScalarType::IntBig
                | ScalarType::Uint
                | ScalarType::Uint128
                | ScalarType::UintBig
        )
    }

    /// Legacy ordering support from the pre-registry model.
    fn legacy_supports_ordering(scalar: ScalarType) -> bool {
        !matches!(scalar, ScalarType::Blob | ScalarType::Unit)
    }

    /// Legacy equality support from the pre-registry model.
    fn legacy_supports_equality(_scalar: ScalarType) -> bool {
        true
    }

    /// Legacy keyability from the pre-registry model.
    fn legacy_is_keyable(scalar: ScalarType) -> bool {
        matches!(
            scalar,
            ScalarType::Account
                | ScalarType::Int
                | ScalarType::Principal
                | ScalarType::Subaccount
                | ScalarType::Timestamp
                | ScalarType::Uint
                | ScalarType::Ulid
                | ScalarType::Unit
        )
    }

    /// Build a representative value for each scalar variant.
    fn sample_value_for_scalar(scalar: ScalarType) -> Value {
        match scalar {
            ScalarType::Account => Value::Account(Account::dummy(1)),
            ScalarType::Blob => Value::Blob(vec![0u8, 1u8]),
            ScalarType::Bool => Value::Bool(true),
            ScalarType::Date => Value::Date(Date::EPOCH),
            ScalarType::Decimal => Value::Decimal(Decimal::ZERO),
            ScalarType::Duration => Value::Duration(Duration::ZERO),
            ScalarType::Enum => Value::Enum(ValueEnum::loose("example")),
            ScalarType::E8s => Value::E8s(E8s::from_atomic(0)),
            ScalarType::E18s => Value::E18s(E18s::from_atomic(0)),
            ScalarType::Float32 => {
                Value::Float32(Float32::try_new(0.0).expect("Float32 sample should be finite"))
            }
            ScalarType::Float64 => {
                Value::Float64(Float64::try_new(0.0).expect("Float64 sample should be finite"))
            }
            ScalarType::Int => Value::Int(0),
            ScalarType::Int128 => Value::Int128(Int128::from(0i128)),
            ScalarType::IntBig => Value::IntBig(Int::from(0i32)),
            ScalarType::Principal => Value::Principal(Principal::anonymous()),
            ScalarType::Subaccount => Value::Subaccount(Subaccount::dummy(2)),
            ScalarType::Text => Value::Text("text".to_string()),
            ScalarType::Timestamp => Value::Timestamp(Timestamp::EPOCH),
            ScalarType::Uint => Value::Uint(0),
            ScalarType::Uint128 => Value::Uint128(Nat128::from(0u128)),
            ScalarType::UintBig => Value::UintBig(Nat::from(0u64)),
            ScalarType::Ulid => Value::Ulid(Ulid::nil()),
            ScalarType::Unit => Value::Unit,
        }
    }

    /// Build a non-matching value for each scalar variant.
    fn mismatching_value_for_scalar(scalar: ScalarType) -> Value {
        match scalar {
            ScalarType::Unit => Value::Bool(false),
            _ => Value::Unit,
        }
    }

    fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
        EntityFieldModel { name, kind }
    }

    crate::test_entity_schema! {
        ScalarPredicateEntity,
        id = Ulid,
        path = "predicate_validate::ScalarEntity",
        entity_name = "ScalarEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", EntityFieldKind::Ulid),
            ("email", EntityFieldKind::Text),
            ("age", EntityFieldKind::Uint),
            ("created_at", EntityFieldKind::Timestamp),
            ("active", EntityFieldKind::Bool),
        ],
        indexes = [],
    }

    crate::test_entity_schema! {
        CollectionPredicateEntity,
        id = Ulid,
        path = "predicate_validate::CollectionEntity",
        entity_name = "CollectionEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", EntityFieldKind::Ulid),
            ("tags", EntityFieldKind::List(&EntityFieldKind::Text)),
            ("principals", EntityFieldKind::Set(&EntityFieldKind::Principal)),
            (
                "attributes",
                EntityFieldKind::Map {
                    key: &EntityFieldKind::Text,
                    value: &EntityFieldKind::Uint,
                }
            ),
        ],
        indexes = [],
    }

    #[test]
    fn validate_model_accepts_scalars_and_coercions() {
        let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

        let predicate = Predicate::And(vec![
            FieldRef::new("id").eq(Ulid::nil()),
            FieldRef::new("email").text_eq_ci("User@example.com"),
            FieldRef::new("age").lt(30u32),
        ]);

        assert!(validate_model(model, &predicate).is_ok());
    }

    #[test]
    fn validate_model_accepts_collections_and_map_contains() {
        let model = <CollectionPredicateEntity as EntitySchema>::MODEL;

        let predicate = Predicate::And(vec![
            FieldRef::new("tags").is_empty(),
            FieldRef::new("principals").is_not_empty(),
            FieldRef::new("attributes").map_contains_entry("k", 1u64, CoercionId::Strict),
        ]);

        assert!(validate_model(model, &predicate).is_ok());

        let bad =
            FieldRef::new("attributes").map_contains_entry("k", 1u64, CoercionId::TextCasefold);

        assert!(matches!(
            validate_model(model, &bad),
            Err(ValidateError::InvalidCoercion { .. })
        ));
    }

    #[test]
    fn validate_model_rejects_unsupported_fields() {
        let model = InvalidEntityModelBuilder::from_fields(
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
        let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

        let predicate = FieldRef::new("email").text_contains("example");
        assert!(validate_model(model, &predicate).is_ok());

        let predicate = FieldRef::new("email").text_contains_ci("EXAMPLE");
        assert!(validate_model(model, &predicate).is_ok());
    }

    #[test]
    fn validate_model_rejects_text_contains_on_non_text() {
        let model = <ScalarPredicateEntity as EntitySchema>::MODEL;

        let predicate = FieldRef::new("age").text_contains("1");
        assert!(matches!(
            validate_model(model, &predicate),
            Err(ValidateError::InvalidOperator { field, op })
                if field == "age" && op == "text_contains"
        ));
    }

    #[test]
    fn scalar_registry_covers_all_variants_exactly_once() {
        let scalars = registry_scalars();
        let mut names = BTreeSet::new();
        let mut seen = [false; SCALAR_TYPE_VARIANT_COUNT];

        for scalar in scalars {
            let index = scalar_index(scalar.clone());
            assert!(!seen[index], "duplicate scalar entry: {scalar:?}");
            seen[index] = true;

            let name = format!("{scalar:?}");
            assert!(names.insert(name.clone()), "duplicate scalar entry: {name}");
        }

        let mut missing = Vec::new();
        for (index, was_seen) in seen.iter().enumerate() {
            if !*was_seen {
                let scalar = scalar_from_index(index).expect("index is in range");
                missing.push(format!("{scalar:?}"));
            }
        }

        assert!(missing.is_empty(), "missing scalar entries: {missing:?}");
        assert_eq!(names.len(), SCALAR_TYPE_VARIANT_COUNT);
    }

    #[test]
    fn scalar_registry_preserves_family_and_matching() {
        for scalar in registry_scalars() {
            let expected_family = legacy_family(scalar.clone());
            let matching = sample_value_for_scalar(scalar.clone());
            let mismatching = mismatching_value_for_scalar(scalar.clone());
            let expected_match = legacy_matches_value(scalar.clone(), &matching);
            let expected_mismatch = legacy_matches_value(scalar.clone(), &mismatching);

            assert_eq!(scalar.family(), expected_family);
            assert_eq!(scalar.matches_value(&matching), expected_match);
            assert_eq!(scalar.matches_value(&mismatching), expected_mismatch);
        }
    }

    #[test]
    fn scalar_registry_preserves_arithmetic_support() {
        for scalar in registry_scalars() {
            let expected = legacy_supports_arithmetic(scalar.clone());

            assert_eq!(scalar.supports_arithmetic(), expected);
        }
    }

    #[test]
    fn scalar_registry_preserves_keyability() {
        for scalar in registry_scalars() {
            let expected = legacy_is_keyable(scalar.clone());

            assert_eq!(scalar.is_keyable(), expected);
        }
    }

    #[test]
    fn scalar_registry_preserves_ordering_support() {
        for scalar in registry_scalars() {
            let expected = legacy_supports_ordering(scalar.clone());

            assert_eq!(scalar.supports_ordering(), expected);
            assert_eq!(scalar.is_orderable(), expected);
        }
    }

    #[test]
    fn scalar_registry_preserves_equality_support() {
        for scalar in registry_scalars() {
            let expected = legacy_supports_equality(scalar.clone());

            assert_eq!(scalar.supports_equality(), expected);
        }
    }

    #[test]
    fn validate_ordering_matches_legacy_support() {
        let coercion = CoercionSpec::default();
        let op = CompareOp::Lt;

        for scalar in registry_scalars() {
            let field_type = FieldType::Scalar(scalar.clone());
            let value = sample_value_for_scalar(scalar.clone());
            let result = validate_ordering("field", &field_type, &value, &coercion, op);
            let is_orderable = legacy_supports_ordering(scalar);

            if is_orderable {
                assert!(result.is_ok(), "scalar should be orderable: {field_type:?}");
            } else {
                assert!(matches!(result, Err(ValidateError::InvalidOperator { .. })));
            }
        }
    }
}
