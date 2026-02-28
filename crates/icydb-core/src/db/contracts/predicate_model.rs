use crate::{
    model::field::FieldKind,
    traits::FieldValueKind,
    value::{CoercionFamily, TextMode, Value},
};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt,
    mem::discriminant,
    ops::{BitAnd, BitOr},
};
use thiserror::Error as ThisError;

///
/// CoercionId
///
/// Identifier for an explicit comparison coercion policy.
///
/// Coercions express *how* values may be compared, not whether a comparison
/// is valid for a given field. Validation and planning enforce legality.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoercionId {
    Strict,
    NumericWiden,
    TextCasefold,
    CollectionElement,
}

impl CoercionId {
    /// Stable tag used by plan hash encodings (fingerprint/continuation).
    #[must_use]
    pub const fn plan_hash_tag(self) -> u8 {
        match self {
            Self::Strict => 0x01,
            Self::NumericWiden => 0x02,
            Self::TextCasefold => 0x04,
            Self::CollectionElement => 0x05,
        }
    }
}

///
/// UnsupportedQueryFeature
///
/// Policy-level query features intentionally rejected by the engine.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum UnsupportedQueryFeature {
    #[error(
        "map field '{field}' is not queryable; map predicates are disabled. model queryable attributes as scalar/indexed fields or list entries"
    )]
    MapPredicate { field: String },
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

///
/// CoercionSpec
///
/// Fully-specified coercion policy for predicate comparisons.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoercionSpec {
    pub id: CoercionId,
    pub params: BTreeMap<String, String>,
}

impl CoercionSpec {
    #[must_use]
    pub const fn new(id: CoercionId) -> Self {
        Self {
            id,
            params: BTreeMap::new(),
        }
    }
}

impl Default for CoercionSpec {
    fn default() -> Self {
        Self::new(CoercionId::Strict)
    }
}

///
/// CoercionRuleFamily
///
/// Rule-side matcher for coercion routing families.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoercionRuleFamily {
    Any,
    Family(CoercionFamily),
}

///
/// CoercionRule
///
/// Declarative coercion routing rule between value families.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoercionRule {
    pub left: CoercionRuleFamily,
    pub right: CoercionRuleFamily,
    pub id: CoercionId,
}

pub(crate) const COERCION_TABLE: &[CoercionRule] = &[
    CoercionRule {
        left: CoercionRuleFamily::Any,
        right: CoercionRuleFamily::Any,
        id: CoercionId::Strict,
    },
    CoercionRule {
        left: CoercionRuleFamily::Family(CoercionFamily::Numeric),
        right: CoercionRuleFamily::Family(CoercionFamily::Numeric),
        id: CoercionId::NumericWiden,
    },
    CoercionRule {
        left: CoercionRuleFamily::Family(CoercionFamily::Textual),
        right: CoercionRuleFamily::Family(CoercionFamily::Textual),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionRuleFamily::Any,
        right: CoercionRuleFamily::Any,
        id: CoercionId::CollectionElement,
    },
];

/// Returns whether a coercion rule exists for the provided routing families.
#[must_use]
pub(in crate::db) fn supports_coercion(
    left: CoercionFamily,
    right: CoercionFamily,
    id: CoercionId,
) -> bool {
    COERCION_TABLE.iter().any(|rule| {
        rule.id == id && family_matches(rule.left, left) && family_matches(rule.right, right)
    })
}

fn family_matches(rule: CoercionRuleFamily, value: CoercionFamily) -> bool {
    match rule {
        CoercionRuleFamily::Any => true,
        CoercionRuleFamily::Family(expected) => expected == value,
    }
}

///
/// TextOp
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum TextOp {
    StartsWith,
    EndsWith,
}

/// Perform equality comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_eq(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            same_variant(left, right).then_some(left == right)
        }
        CoercionId::NumericWiden => {
            if !left.supports_numeric_coercion() || !right.supports_numeric_coercion() {
                return None;
            }

            left.cmp_numeric(right).map(|ord| ord == Ordering::Equal)
        }
        CoercionId::TextCasefold => compare_casefold(left, right),
    }
}

/// Perform ordering comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_order(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<Ordering> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            if !same_variant(left, right) {
                return None;
            }
            Value::strict_order_cmp(left, right)
        }
        CoercionId::NumericWiden => {
            if !left.supports_numeric_coercion() || !right.supports_numeric_coercion() {
                return None;
            }

            left.cmp_numeric(right)
        }
        CoercionId::TextCasefold => {
            let left = casefold_value(left)?;
            let right = casefold_value(right)?;
            Some(left.cmp(&right))
        }
    }
}

/// Canonical total ordering for database predicate semantics.
#[must_use]
pub(in crate::db) fn canonical_cmp(left: &Value, right: &Value) -> Ordering {
    if let Some(ordering) = Value::strict_order_cmp(left, right) {
        return ordering;
    }

    left.canonical_rank().cmp(&right.canonical_rank())
}

/// Perform text-specific comparison operations.
#[must_use]
pub(in crate::db) fn compare_text(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    op: TextOp,
) -> Option<bool> {
    if !matches!(left, Value::Text(_)) || !matches!(right, Value::Text(_)) {
        return None;
    }

    let mode = match coercion.id {
        CoercionId::Strict => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        _ => return None,
    };

    match op {
        TextOp::StartsWith => left.text_starts_with(right, mode),
        TextOp::EndsWith => left.text_ends_with(right, mode),
    }
}

fn same_variant(left: &Value, right: &Value) -> bool {
    discriminant(left) == discriminant(right)
}

fn compare_casefold(left: &Value, right: &Value) -> Option<bool> {
    let left = casefold_value(left)?;
    let right = casefold_value(right)?;
    Some(left == right)
}

fn casefold_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(casefold(text)),
        _ => None,
    }
}

fn casefold(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}

///
/// CompareOp
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompareOp {
    Eq = 0x01,
    Ne = 0x02,
    Lt = 0x03,
    Lte = 0x04,
    Gt = 0x05,
    Gte = 0x06,
    In = 0x07,
    NotIn = 0x08,
    Contains = 0x09,
    StartsWith = 0x0a,
    EndsWith = 0x0b,
}

impl CompareOp {
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }
}

///
/// ComparePredicate
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparePredicate {
    pub field: String,
    pub op: CompareOp,
    pub value: Value,
    pub coercion: CoercionSpec,
}

impl ComparePredicate {
    fn new(field: String, op: CompareOp, value: Value) -> Self {
        Self {
            field,
            op,
            value,
            coercion: CoercionSpec::default(),
        }
    }

    /// Construct a comparison predicate with an explicit coercion policy.
    #[must_use]
    pub fn with_coercion(
        field: impl Into<String>,
        op: CompareOp,
        value: Value,
        coercion: CoercionId,
    ) -> Self {
        Self {
            field: field.into(),
            op,
            value,
            coercion: CoercionSpec::new(coercion),
        }
    }

    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Eq, value)
    }

    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Ne, value)
    }

    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lt, value)
    }

    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lte, value)
    }

    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gt, value)
    }

    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gte, value)
    }

    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::In, Value::List(values))
    }

    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::NotIn, Value::List(values))
    }
}

///
/// Predicate
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ComparePredicate),
    IsNull { field: String },
    IsMissing { field: String },
    IsEmpty { field: String },
    IsNotEmpty { field: String },
    TextContains { field: String, value: Value },
    TextContainsCi { field: String, value: Value },
}

impl Predicate {
    #[must_use]
    pub const fn and(preds: Vec<Self>) -> Self {
        Self::And(preds)
    }

    #[must_use]
    pub const fn or(preds: Vec<Self>) -> Self {
        Self::Or(preds)
    }

    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(pred: Self) -> Self {
        Self::Not(Box::new(pred))
    }

    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::eq(field, value))
    }

    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::ne(field, value))
    }

    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lt(field, value))
    }

    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lte(field, value))
    }

    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gt(field, value))
    }

    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gte(field, value))
    }

    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::in_(field, values))
    }

    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::not_in(field, values))
    }
}

impl BitAnd for Predicate {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::And(vec![self, rhs])
    }
}

impl BitAnd for &Predicate {
    type Output = Predicate;

    fn bitand(self, rhs: Self) -> Self::Output {
        Predicate::And(vec![self.clone(), rhs.clone()])
    }
}

impl BitOr for Predicate {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::Or(vec![self, rhs])
    }
}

impl BitOr for &Predicate {
    type Output = Predicate;

    fn bitor(self, rhs: Self) -> Self::Output {
        Predicate::Or(vec![self.clone(), rhs.clone()])
    }
}

/// Neutral predicate model consumed by executor/index layers.
pub(crate) type PredicateExecutionModel = Predicate;
