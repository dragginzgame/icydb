use crate::{
    traits::EntityKind,
    value::{Value, ValueFamily, ValueFamilyExt},
};
use icydb_schema::{
    build::get_schema,
    node::{
        Entity, Enum, Item, ItemTarget, List, Map, Newtype, Record, Schema, Set, Tuple,
        Value as SValue,
    },
    types::{Cardinality, Primitive},
};
use std::{collections::BTreeMap, fmt};

use super::{
    ast::{CompareOp, ComparePredicate, Predicate},
    coercion::{CoercionId, CoercionSpec, supports_coercion},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarType {
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
    pub fn matches_value(&self, value: &Value) -> bool {
        match (self, value) {
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
            | (Self::Unit, Value::Unit) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    Scalar(ScalarType),
    List(Box<FieldType>),
    Set(Box<FieldType>),
    Map {
        key: Box<FieldType>,
        value: Box<FieldType>,
    },
    Unsupported,
}

impl FieldType {
    #[must_use]
    pub fn family(&self) -> Option<ValueFamily> {
        match self {
            FieldType::Scalar(inner) => Some(inner.family()),
            FieldType::List(_) | FieldType::Set(_) | FieldType::Map { .. } => {
                Some(ValueFamily::Collection)
            }
            FieldType::Unsupported => None,
        }
    }

    #[must_use]
    pub const fn is_text(&self) -> bool {
        matches!(self, FieldType::Scalar(ScalarType::Text))
    }

    #[must_use]
    pub const fn is_collection(&self) -> bool {
        matches!(
            self,
            FieldType::List(_) | FieldType::Set(_) | FieldType::Map { .. }
        )
    }

    #[must_use]
    pub const fn is_list_like(&self) -> bool {
        matches!(self, FieldType::List(_) | FieldType::Set(_))
    }

    #[must_use]
    pub const fn is_map(&self) -> bool {
        matches!(self, FieldType::Map { .. })
    }

    #[must_use]
    pub fn element_type(&self) -> Option<&FieldType> {
        match self {
            FieldType::List(inner) | FieldType::Set(inner) => Some(inner),
            _ => None,
        }
    }

    #[must_use]
    pub fn map_types(&self) -> Option<(&FieldType, &FieldType)> {
        match self {
            FieldType::Map { key, value } => Some((key.as_ref(), value.as_ref())),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        match self {
            FieldType::Scalar(inner) => inner.is_orderable(),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaInfo {
    fields: BTreeMap<String, FieldType>,
}

impl SchemaInfo {
    #[must_use]
    pub fn new(fields: impl IntoIterator<Item = (String, FieldType)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&FieldType> {
        self.fields.get(name)
    }

    pub fn from_entity_path(path: &str) -> Result<Self, ValidateError> {
        let schema =
            get_schema().map_err(|err| ValidateError::SchemaUnavailable(err.to_string()))?;
        let entity = schema
            .cast_node::<Entity>(path)
            .map_err(|err| ValidateError::SchemaUnavailable(err.to_string()))?;

        Ok(Self::from_entity_schema(entity, &schema))
    }

    pub fn from_entity<E: EntityKind>() -> Result<Self, ValidateError> {
        Self::from_entity_path(E::PATH)
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
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateError {
    #[error("unknown field '{field}'")]
    UnknownField { field: String },

    #[error("unsupported field type for '{field}'")]
    UnsupportedFieldType { field: String },

    #[error("operator {op} is not valid for field '{field}'")]
    InvalidOperator { field: String, op: String },

    #[error("coercion {coercion:?} is not valid for field '{field}'")]
    InvalidCoercion { field: String, coercion: CoercionId },

    #[error("invalid literal for field '{field}': {message}")]
    InvalidLiteral { field: String, message: String },

    #[error("schema unavailable: {0}")]
    SchemaUnavailable(String),
}

#[must_use]
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
            ensure_field(schema, field).map(|_| ())
        }
        Predicate::IsEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::InvalidOperator {
                    field: field.clone(),
                    op: "is_empty".to_string(),
                })
            }
        }
        Predicate::IsNotEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::InvalidOperator {
                    field: field.clone(),
                    op: "is_not_empty".to_string(),
                })
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
            validate_in(&cmp.field, field_type, &cmp.value, &cmp.coercion)
        }
        CompareOp::AnyIn | CompareOp::AllIn => {
            validate_any_all_in(&cmp.field, field_type, &cmp.value, &cmp.coercion)
        }
        CompareOp::Contains => validate_contains(&cmp.field, field_type, &cmp.value, &cmp.coercion),
        CompareOp::StartsWith | CompareOp::EndsWith => {
            validate_text_op(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
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
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "expected scalar literal".to_string(),
        });
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
        return Err(ValidateError::InvalidOperator {
            field: field.to_string(),
            op: format!("{op:?}"),
        });
    }

    if matches!(value, Value::List(_)) {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "expected scalar literal".to_string(),
        });
    }

    ensure_coercion(field, field_type, value, coercion)
}

fn validate_in(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_collection() {
        return Err(ValidateError::InvalidOperator {
            field: field.to_string(),
            op: format!("{:?}", CompareOp::In),
        });
    }

    let Value::List(items) = value else {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "expected list literal".to_string(),
        });
    };

    for item in items {
        ensure_coercion(field, field_type, item, coercion)?;
    }

    Ok(())
}

fn validate_any_all_in(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    let element_type = field_type
        .element_type()
        .ok_or_else(|| ValidateError::InvalidOperator {
            field: field.to_string(),
            op: format!("{:?}", CompareOp::AnyIn),
        })?;

    let Value::List(items) = value else {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "expected list literal".to_string(),
        });
    };

    for item in items {
        ensure_coercion(field, element_type, item, coercion)?;
    }

    Ok(())
}

fn validate_contains(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_text() {
        if !matches!(coercion.id, CoercionId::Strict | CoercionId::TextCasefold) {
            return Err(ValidateError::InvalidCoercion {
                field: field.to_string(),
                coercion: coercion.id,
            });
        }
        if !matches!(value, Value::Text(_)) {
            return Err(ValidateError::InvalidLiteral {
                field: field.to_string(),
                message: "expected text literal".to_string(),
            });
        }

        return ensure_coercion(field, field_type, value, coercion);
    }

    let element_type = field_type
        .element_type()
        .ok_or_else(|| ValidateError::InvalidOperator {
            field: field.to_string(),
            op: format!("{:?}", CompareOp::Contains),
        })?;

    ensure_coercion(field, element_type, value, coercion)
}

fn validate_text_op(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if !field_type.is_text() {
        return Err(ValidateError::InvalidOperator {
            field: field.to_string(),
            op: format!("{op:?}"),
        });
    }

    if !matches!(coercion.id, CoercionId::Strict | CoercionId::TextCasefold) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !matches!(value, Value::Text(_)) {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "expected text literal".to_string(),
        });
    }

    ensure_coercion(field, field_type, value, coercion)
}

fn validate_map_key(
    schema: &SchemaInfo,
    field: &str,
    key: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    let (key_type, _) = field_type
        .map_types()
        .ok_or_else(|| ValidateError::InvalidOperator {
            field: field.to_string(),
            op: "map_contains_key".to_string(),
        })?;

    ensure_coercion(field, key_type, key, coercion)
}

fn validate_map_value(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    let (_, value_type) = field_type
        .map_types()
        .ok_or_else(|| ValidateError::InvalidOperator {
            field: field.to_string(),
            op: "map_contains_value".to_string(),
        })?;

    ensure_coercion(field, value_type, value, coercion)
}

fn validate_map_entry(
    schema: &SchemaInfo,
    field: &str,
    key: &Value,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    let (key_type, value_type) =
        field_type
            .map_types()
            .ok_or_else(|| ValidateError::InvalidOperator {
                field: field.to_string(),
                op: "map_contains_entry".to_string(),
            })?;

    ensure_coercion(field, key_type, key, coercion)?;
    ensure_coercion(field, value_type, value, coercion)?;

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
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "literal type does not match field type".to_string(),
        });
    }

    Ok(())
}

fn ensure_list_literal(
    field: &str,
    literal: &Value,
    field_type: &FieldType,
) -> Result<(), ValidateError> {
    if !literal_matches_type(literal, field_type) {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "list literal does not match field element type".to_string(),
        });
    }

    Ok(())
}

fn ensure_map_literal(
    field: &str,
    literal: &Value,
    field_type: &FieldType,
) -> Result<(), ValidateError> {
    if !literal_matches_type(literal, field_type) {
        return Err(ValidateError::InvalidLiteral {
            field: field.to_string(),
            message: "map literal does not match field key/value types".to_string(),
        });
    }

    Ok(())
}

fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
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

fn scalar_from_primitive(prim: Primitive) -> ScalarType {
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

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Scalar(inner) => write!(f, "{inner:?}"),
            FieldType::List(inner) => write!(f, "List<{inner}>"),
            FieldType::Set(inner) => write!(f, "Set<{inner}>"),
            FieldType::Map { key, value } => write!(f, "Map<{key}, {value}>"),
            FieldType::Unsupported => write!(f, "Unsupported"),
        }
    }
}
