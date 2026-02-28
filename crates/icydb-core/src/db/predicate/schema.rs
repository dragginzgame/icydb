//! Module: predicate::schema
//! Responsibility: schema-aware predicate validation and coercion legality checks.
//! Does not own: runtime predicate execution or index planning strategy.
//! Boundary: validation boundary between user predicates and executable plans.

use crate::{
    db::{
        identity::{EntityName, EntityNameError, IndexName, IndexNameError},
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate,
            model::UnsupportedQueryFeature, supports_coercion,
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::FieldValueKind,
    value::{CoercionFamily, CoercionFamilyExt, Value},
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

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
macro_rules! scalar_coercion_family_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $coercion_family, )*
        }
    };
}

macro_rules! scalar_matches_value_from_registry {
    ( @args $self:expr, $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        matches!(
            ($self, $value),
            $( (ScalarType::$scalar, $value_pat) )|*
        )
    };
}

macro_rules! scalar_supports_numeric_coercion_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_numeric_coercion, )*
        }
    };
}

macro_rules! scalar_is_keyable_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $is_keyable, )*
        }
    };
}

macro_rules! scalar_supports_ordering_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_ordering, )*
        }
    };
}

impl ScalarType {
    #[must_use]
    pub(crate) const fn coercion_family(&self) -> CoercionFamily {
        scalar_registry!(scalar_coercion_family_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn is_orderable(&self) -> bool {
        // Predicate-level ordering gate.
        // Delegates to registry-backed supports_ordering.
        self.supports_ordering()
    }

    #[must_use]
    pub(crate) const fn matches_value(&self, value: &Value) -> bool {
        scalar_registry!(scalar_matches_value_from_registry, self, value)
    }

    #[must_use]
    pub(crate) const fn supports_numeric_coercion(&self) -> bool {
        scalar_registry!(scalar_supports_numeric_coercion_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn is_keyable(&self) -> bool {
        scalar_registry!(scalar_is_keyable_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn supports_ordering(&self) -> bool {
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
    Structured { queryable: bool },
}

impl FieldType {
    #[must_use]
    pub(crate) const fn value_kind(&self) -> FieldValueKind {
        match self {
            Self::Scalar(_) => FieldValueKind::Atomic,
            Self::List(_) | Self::Set(_) => FieldValueKind::Structured { queryable: true },
            Self::Map { .. } => FieldValueKind::Structured { queryable: false },
            Self::Structured { queryable } => FieldValueKind::Structured {
                queryable: *queryable,
            },
        }
    }

    #[must_use]
    pub(crate) const fn coercion_family(&self) -> Option<CoercionFamily> {
        match self {
            Self::Scalar(inner) => Some(inner.coercion_family()),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(CoercionFamily::Collection),
            Self::Structured { .. } => None,
        }
    }

    #[must_use]
    pub(crate) const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarType::Text))
    }

    #[must_use]
    pub(crate) const fn is_collection(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_) | Self::Map { .. })
    }

    #[must_use]
    pub(crate) const fn is_list_like(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub(crate) const fn is_orderable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_orderable(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn is_keyable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_keyable(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn supports_numeric_coercion(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.supports_numeric_coercion(),
            _ => false,
        }
    }
}

pub(crate) fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Scalar(inner) => inner.matches_value(literal),
        FieldType::List(element) | FieldType::Set(element) => match literal {
            Value::List(items) => items.iter().all(|item| literal_matches_type(item, element)),
            _ => false,
        },
        FieldType::Map { key, value } => match literal {
            Value::Map(entries) => {
                if Value::validate_map_entries(entries.as_slice()).is_err() {
                    return false;
                }

                entries.iter().all(|(entry_key, entry_value)| {
                    literal_matches_type(entry_key, key) && literal_matches_type(entry_value, value)
                })
            }
            _ => false,
        },
        FieldType::Structured { .. } => {
            // NOTE: non-queryable structured field types never match predicate literals.
            false
        }
    }
}

pub(super) fn field_type_from_model_kind(kind: &FieldKind) -> FieldType {
    match kind {
        FieldKind::Account => FieldType::Scalar(ScalarType::Account),
        FieldKind::Blob => FieldType::Scalar(ScalarType::Blob),
        FieldKind::Bool => FieldType::Scalar(ScalarType::Bool),
        FieldKind::Date => FieldType::Scalar(ScalarType::Date),
        FieldKind::Decimal { .. } => FieldType::Scalar(ScalarType::Decimal),
        FieldKind::Duration => FieldType::Scalar(ScalarType::Duration),
        FieldKind::Enum { .. } => FieldType::Scalar(ScalarType::Enum),
        FieldKind::Float32 => FieldType::Scalar(ScalarType::Float32),
        FieldKind::Float64 => FieldType::Scalar(ScalarType::Float64),
        FieldKind::Int => FieldType::Scalar(ScalarType::Int),
        FieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        FieldKind::IntBig => FieldType::Scalar(ScalarType::IntBig),
        FieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        FieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        FieldKind::Text => FieldType::Scalar(ScalarType::Text),
        FieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        FieldKind::Uint => FieldType::Scalar(ScalarType::Uint),
        FieldKind::Uint128 => FieldType::Scalar(ScalarType::Uint128),
        FieldKind::UintBig => FieldType::Scalar(ScalarType::UintBig),
        FieldKind::Ulid => FieldType::Scalar(ScalarType::Ulid),
        FieldKind::Unit => FieldType::Scalar(ScalarType::Unit),
        FieldKind::Relation { key_kind, .. } => field_type_from_model_kind(key_kind),
        FieldKind::List(inner) => FieldType::List(Box::new(field_type_from_model_kind(inner))),
        FieldKind::Set(inner) => FieldType::Set(Box::new(field_type_from_model_kind(inner))),
        FieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_model_kind(key)),
            value: Box::new(field_type_from_model_kind(value)),
        },
        FieldKind::Structured { queryable } => FieldType::Structured {
            queryable: *queryable,
        },
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(inner) => write!(f, "{inner:?}"),
            Self::List(inner) => write!(f, "List<{inner}>"),
            Self::Set(inner) => write!(f, "Set<{inner}>"),
            Self::Map { key, value } => write!(f, "Map<{key}, {value}>"),
            Self::Structured { queryable } => {
                write!(f, "Structured<queryable={queryable}>")
            }
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

/// Reject policy-level non-queryable features before planning.
pub(crate) fn reject_unsupported_query_features(
    predicate: &Predicate,
) -> Result<(), UnsupportedQueryFeature> {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Compare(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                reject_unsupported_query_features(child)?;
            }

            Ok(())
        }
        Predicate::Not(inner) => reject_unsupported_query_features(inner),
    }
}

/// Validates a predicate against the provided schema information.
pub(crate) fn validate(schema: &SchemaInfo, predicate: &Predicate) -> Result<(), ValidateError> {
    reject_unsupported_query_features(predicate)?;

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
            let _field_type = ensure_field(schema, field)?;
            Ok(())
        }
        Predicate::IsEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::invalid_operator(field, "is_empty"))
            }
        }
        Predicate::IsNotEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::invalid_operator(field, "is_not_empty"))
            }
        }
        Predicate::TextContains { field, value } => {
            validate_text_contains(schema, field, value, "text_contains")
        }
        Predicate::TextContainsCi { field, value } => {
            validate_text_contains(schema, field, value, "text_contains_ci")
        }
    }
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
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
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
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
    }

    let Value::List(items) = value else {
        return Err(ValidateError::invalid_literal(
            field,
            "expected list literal",
        ));
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
        return Err(ValidateError::invalid_operator(
            field,
            format!("{:?}", CompareOp::Contains),
        ));
    }

    let element_type = match field_type {
        FieldType::List(inner) | FieldType::Set(inner) => inner.as_ref(),
        _ => {
            return Err(ValidateError::invalid_operator(
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
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
    }

    ensure_text_literal(field, value)?;

    ensure_coercion(field, field_type, value, coercion)
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
        return Err(ValidateError::invalid_operator(field, op));
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

    if matches!(field_type, FieldType::Map { .. }) {
        return Err(UnsupportedQueryFeature::MapPredicate {
            field: field.to_string(),
        }
        .into());
    }

    if !field_type.value_kind().is_queryable() {
        return Err(ValidateError::NonQueryableFieldType {
            field: field.to_string(),
        });
    }

    Ok(field_type)
}

// Ensure the literal is text to match text-only operators.
fn ensure_text_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if !matches!(value, Value::Text(_)) {
        return Err(ValidateError::invalid_literal(
            field,
            "expected text literal",
        ));
    }

    Ok(())
}

// Reject list literals when scalar comparisons are required.
fn ensure_scalar_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if matches!(value, Value::List(_)) {
        return Err(ValidateError::invalid_literal(
            field,
            "expected scalar literal",
        ));
    }

    Ok(())
}

fn ensure_coercion(
    field: &str,
    field_type: &FieldType,
    literal: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) && !field_type.is_text() {
        // CONTRACT: case-insensitive coercions are text-only.
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    // NOTE:
    // NumericWiden eligibility is registry-authoritative.
    // CoercionFamily::Numeric is intentionally NOT sufficient.
    // This prevents validation/runtime divergence for Date, IntBig, UintBig.
    if matches!(coercion.id, CoercionId::NumericWiden)
        && (!field_type.supports_numeric_coercion() || !literal.supports_numeric_coercion())
    {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !matches!(coercion.id, CoercionId::NumericWiden) {
        let left_family =
            field_type
                .coercion_family()
                .ok_or_else(|| ValidateError::NonQueryableFieldType {
                    field: field.to_string(),
                })?;
        let right_family = literal.coercion_family();

        if !supports_coercion(left_family, right_family, coercion.id) {
            return Err(ValidateError::InvalidCoercion {
                field: field.to_string(),
                coercion: coercion.id,
            });
        }
    }

    if matches!(
        coercion.id,
        CoercionId::Strict | CoercionId::CollectionElement
    ) && !literal_matches_type(literal, field_type)
    {
        return Err(ValidateError::invalid_literal(
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
        return Err(ValidateError::invalid_literal(
            field,
            "list literal does not match field element type",
        ));
    }

    Ok(())
}
