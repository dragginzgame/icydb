use crate::value::Value;

///
/// NumericSubtype
///
/// NumericSubtype keeps planner numeric typing coarse while still preserving
/// the few distinctions needed by operator/function inference and aggregate
/// result shaping.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum NumericSubtype {
    Integer,
    Float,
    Decimal,
    Unknown,
}

///
/// FunctionCategory
///
/// FunctionCategory classifies the canonical scalar function taxonomy into the
/// planner-owned semantic families used by typing, capability, and audit
/// reasoning.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub(in crate::db::query::plan::expr) enum FunctionCategory {
    BooleanPredicate,
    Collection,
    NullHandling,
    Numeric,
    Text,
}

///
/// FunctionNullBehavior
///
/// FunctionNullBehavior records the canonical null contract for one scalar
/// function so planner consumers do not rediscover strictness or special null
/// handling through parallel local ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum FunctionNullBehavior {
    NullIgnoring,
    NullObserving,
    Strict,
}

///
/// FunctionDeterminism
///
/// FunctionDeterminism keeps execution-stability classification on the
/// planner-owned function registry even while the current surface only ships
/// deterministic scalar functions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum FunctionDeterminism {
    Deterministic,
}

///
/// FunctionTypeInferenceShape
///
/// FunctionTypeInferenceShape captures the canonical planner typing contract
/// for one scalar function family so type inference can consume the shared
/// registry instead of acting as the hidden function owner.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum FunctionTypeInferenceShape {
    ByteLengthResult,
    BoolResult {
        text_positions: &'static [usize],
    },
    CollectionContains,
    DynamicCoalesce,
    DynamicNullIf,
    NumericResult {
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
        subtype: NumericSubtype,
    },
    NumericScaleResult,
    TextResult {
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
    },
    UnaryBoolPredicate,
}

///
/// FunctionSurface
///
/// FunctionSurface records the planner-owned expression surfaces where one
/// canonical scalar function family is admitted. This keeps surface
/// eligibility on the function registry instead of parser-local or
/// lowering-local special-case ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FunctionSurface {
    AggregateInput,
    AggregateInputCondition,
    HavingCondition,
    Projection,
    ProjectionCondition,
    Where,
}

///
/// BooleanFunctionShape
///
/// BooleanFunctionShape classifies the small planner-owned scalar function
/// families that participate directly in boolean truth-condition admission and
/// boolean predicate compilation. This keeps those consumers on one shared
/// function-family owner instead of repeating the same admitted boolean
/// subsets locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum BooleanFunctionShape {
    CollectionContains,
    FieldPredicate,
    NullTest,
    TextPredicate,
    TruthCoalesce,
}

///
/// NullTestFunctionKind
///
/// NullTestFunctionKind keeps the small `IS NULL` versus `IS NOT NULL`
/// distinction on the function owner once a caller has already admitted the
/// broader boolean null-test family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum NullTestFunctionKind {
    IsNotNull,
    IsNull,
}

impl NullTestFunctionKind {
    /// Return whether the null branch is the truthy branch for this null-test kind.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn null_matches_true(self) -> bool {
        matches!(self, Self::IsNull)
    }

    /// Evaluate one admitted null test against one value.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn eval_value(self, value: &Value) -> Value {
        Value::Bool(self.null_matches_true() == matches!(value, Value::Null))
    }
}

///
/// TextPredicateFunctionKind
///
/// TextPredicateFunctionKind preserves the finer text-predicate distinction
/// once boolean-function admission has already proven that one scalar
/// function belongs to the text-predicate family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum TextPredicateFunctionKind {
    Contains,
    EndsWith,
    StartsWith,
}

impl TextPredicateFunctionKind {
    /// Evaluate one admitted text predicate against one text input and needle.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_text(self, text: &str, needle: &str) -> Value {
        Value::Bool(match self {
            Self::Contains => text.contains(needle),
            Self::EndsWith => text.ends_with(needle),
            Self::StartsWith => text.starts_with(needle),
        })
    }
}

///
/// UnaryTextFunctionKind
///
/// UnaryTextFunctionKind preserves the finer unary text transform
/// distinction once scalar-evaluation dispatch has already proven that one
/// scalar function belongs to the unary-text family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum UnaryTextFunctionKind {
    Length,
    Lower,
    Ltrim,
    Rtrim,
    Trim,
    Upper,
}

///
/// UnaryNumericFunctionKind
///
/// UnaryNumericFunctionKind preserves the finer unary numeric transform
/// distinction once scalar-evaluation dispatch has already proven that one
/// scalar function belongs to the unary-numeric family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum UnaryNumericFunctionKind {
    Abs,
    Cbrt,
    Ceiling,
    Exp,
    Floor,
    Ln,
    Log10,
    Log2,
    Sign,
    Sqrt,
}

///
/// BinaryNumericFunctionKind
///
/// BinaryNumericFunctionKind preserves the finer binary numeric transform
/// distinction once scalar-evaluation dispatch has already proven that one
/// scalar function belongs to the binary-numeric family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum BinaryNumericFunctionKind {
    Log,
    Mod,
    Power,
}

///
/// NumericScaleFunctionKind
///
/// NumericScaleFunctionKind preserves the finer scale-taking numeric
/// transform distinction once scalar-evaluation dispatch has already proven
/// that one scalar function belongs to this family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum NumericScaleFunctionKind {
    Round,
    Trunc,
}

///
/// LeftRightTextFunctionKind
///
/// LeftRightTextFunctionKind preserves the LEFT versus RIGHT distinction once
/// scalar-evaluation dispatch has already proven that one scalar function
/// belongs to the bounded left/right text family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum LeftRightTextFunctionKind {
    Left,
    Right,
}

///
/// FieldPredicateFunctionKind
///
/// FieldPredicateFunctionKind preserves the finer field-state predicate
/// distinction once boolean-function admission has already proven that one
/// scalar function belongs to the field-predicate family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum FieldPredicateFunctionKind {
    Empty,
    Missing,
    NotEmpty,
}

///
/// AggregateInputConstantFoldShape
///
/// AggregateInputConstantFoldShape classifies the scalar function families
/// whose literal-only aggregate-input forms can collapse to one deterministic
/// literal result before planner grouping and aggregate lowering continue.
/// This keeps builder-side and frontend-lowering-side constant-fold admission
/// on one enum-owned contract instead of repeating the same foldable subsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum AggregateInputConstantFoldShape {
    DynamicCoalesce,
    DynamicNullIf,
    BinaryNumeric,
    Round,
    UnaryNumeric,
}

///
/// ScalarEvalFunctionShape
///
/// ScalarEvalFunctionShape classifies the bounded scalar-function behavior
/// families shared by planner literal preview and executor scalar projection
/// evaluation. This keeps dispatch-family ownership on `Function` and keeps
/// the shared pure value-level transforms on the same owner types.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) enum ScalarEvalFunctionShape {
    DynamicCoalesce,
    DynamicNullIf,
    BinaryNumeric,
    LeftRightText,
    NonExecutableProjection,
    NullTest,
    PositionText,
    ReplaceText,
    NumericScale,
    OctetLength,
    SubstringText,
    TextPredicate,
    UnaryNumeric,
    UnaryText,
}
