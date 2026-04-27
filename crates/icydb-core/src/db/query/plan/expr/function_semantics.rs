//! Module: query::plan::expr::function_semantics
//! Responsibility: planner-owned scalar function taxonomy and semantic facets.
//! Does not own: parser identifier resolution, expression lowering, or runtime evaluation.
//! Boundary: central registry for scalar function category, null behavior, determinism, and typing shape.

use crate::{
    db::{
        numeric::{
            NumericArithmeticOp, apply_decimal_arithmetic, coerce_numeric_decimal, decimal_power,
            decimal_sign, decimal_sqrt,
        },
        query::plan::expr::ast::{Expr, Function},
    },
    types::Decimal,
    value::Value,
};

///
/// NumericSubtype
///
/// NumericSubtype keeps planner numeric typing coarse while still preserving
/// the few distinctions needed by operator/function inference and aggregate
/// result shaping.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericSubtype {
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
pub(crate) enum FunctionCategory {
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
pub(crate) enum FunctionNullBehavior {
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
pub(crate) enum FunctionDeterminism {
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
pub(crate) enum FunctionTypeInferenceShape {
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
pub(crate) enum FunctionSurface {
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
pub(crate) enum BooleanFunctionShape {
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
pub(crate) enum NullTestFunctionKind {
    IsNotNull,
    IsNull,
}

impl NullTestFunctionKind {
    /// Return whether the null branch is the truthy branch for this null-test kind.
    #[must_use]
    pub(crate) const fn null_matches_true(self) -> bool {
        matches!(self, Self::IsNull)
    }

    /// Evaluate one admitted null test against one value.
    #[must_use]
    pub(crate) const fn eval_value(self, value: &Value) -> Value {
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
pub(crate) enum TextPredicateFunctionKind {
    Contains,
    EndsWith,
    StartsWith,
}

impl TextPredicateFunctionKind {
    /// Evaluate one admitted text predicate against one text input and needle.
    #[must_use]
    pub(crate) fn eval_text(self, text: &str, needle: &str) -> Value {
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
pub(crate) enum UnaryTextFunctionKind {
    Length,
    Lower,
    Ltrim,
    Rtrim,
    Trim,
    Upper,
}

impl UnaryTextFunctionKind {
    /// Evaluate one admitted unary text transform against one text input.
    #[must_use]
    pub(crate) fn eval_text(self, text: &str) -> Value {
        match self {
            Self::Trim => Value::Text(text.trim().to_string()),
            Self::Ltrim => Value::Text(text.trim_start().to_string()),
            Self::Rtrim => Value::Text(text.trim_end().to_string()),
            Self::Lower => Value::Text(text.to_lowercase()),
            Self::Upper => Value::Text(text.to_uppercase()),
            Self::Length => Value::Uint(u64::try_from(text.chars().count()).unwrap_or(u64::MAX)),
        }
    }
}

///
/// UnaryNumericFunctionKind
///
/// UnaryNumericFunctionKind preserves the finer unary numeric transform
/// distinction once scalar-evaluation dispatch has already proven that one
/// scalar function belongs to the unary-numeric family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryNumericFunctionKind {
    Abs,
    Ceiling,
    Floor,
    Sign,
    Sqrt,
}

impl UnaryNumericFunctionKind {
    /// Evaluate one admitted unary numeric transform against one decimal input.
    #[must_use]
    pub(crate) fn eval_decimal(self, decimal: Decimal) -> Option<Value> {
        let result = match self {
            Self::Abs => decimal.abs(),
            Self::Ceiling => decimal.ceil_dp0(),
            Self::Floor => decimal.floor_dp0(),
            Self::Sign => decimal_sign(decimal),
            Self::Sqrt => decimal_sqrt(decimal)?,
        };

        Some(Value::Decimal(result))
    }
}

///
/// BinaryNumericFunctionKind
///
/// BinaryNumericFunctionKind preserves the finer binary numeric transform
/// distinction once scalar-evaluation dispatch has already proven that one
/// scalar function belongs to the binary-numeric family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryNumericFunctionKind {
    Mod,
    Power,
}

impl BinaryNumericFunctionKind {
    /// Evaluate one admitted binary numeric transform against decimal inputs.
    #[must_use]
    pub(crate) fn eval_decimal(self, left: Decimal, right: Decimal) -> Option<Value> {
        let result = match self {
            Self::Mod => apply_decimal_arithmetic(NumericArithmeticOp::Rem, left, right),
            Self::Power => decimal_power(left, right)?,
        };

        Some(Value::Decimal(result))
    }
}

///
/// NumericScaleFunctionKind
///
/// NumericScaleFunctionKind preserves the finer scale-taking numeric
/// transform distinction once scalar-evaluation dispatch has already proven
/// that one scalar function belongs to this family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericScaleFunctionKind {
    Round,
    Trunc,
}

impl NumericScaleFunctionKind {
    /// Evaluate one admitted scale-taking numeric transform.
    #[must_use]
    pub(crate) const fn eval_decimal(self, decimal: Decimal, scale: u32) -> Value {
        let result = match self {
            Self::Round => decimal.round_dp(scale),
            Self::Trunc => decimal.trunc_dp(scale),
        };

        Value::Decimal(result)
    }
}

///
/// LeftRightTextFunctionKind
///
/// LeftRightTextFunctionKind preserves the LEFT versus RIGHT distinction once
/// scalar-evaluation dispatch has already proven that one scalar function
/// belongs to the bounded left/right text family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeftRightTextFunctionKind {
    Left,
    Right,
}

impl LeftRightTextFunctionKind {
    /// Evaluate one admitted LEFT/RIGHT transform against one text input.
    #[must_use]
    pub(crate) fn eval_text(self, text: &str, count: i64) -> Value {
        Value::Text(match self {
            Self::Left => Self::left_chars(text, count),
            Self::Right => Self::right_chars(text, count),
        })
    }

    /// Return the first N chars from one text input while keeping
    /// negative/zero lengths on the empty-string SQL boundary.
    fn left_chars(text: &str, count: i64) -> String {
        if count <= 0 {
            return String::new();
        }

        text.chars()
            .take(usize::try_from(count).unwrap_or(usize::MAX))
            .collect()
    }

    /// Return the last N chars from one text input while keeping
    /// negative/zero lengths on the empty-string SQL boundary.
    fn right_chars(text: &str, count: i64) -> String {
        if count <= 0 {
            return String::new();
        }

        let count = usize::try_from(count).unwrap_or(usize::MAX);
        let total = text.chars().count();
        let skip = total.saturating_sub(count);

        text.chars().skip(skip).collect()
    }
}

///
/// FieldPredicateFunctionKind
///
/// FieldPredicateFunctionKind preserves the finer field-state predicate
/// distinction once boolean-function admission has already proven that one
/// scalar function belongs to the field-predicate family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldPredicateFunctionKind {
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
pub(crate) enum AggregateInputConstantFoldShape {
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
pub(crate) enum ScalarEvalFunctionShape {
    DynamicCoalesce,
    DynamicNullIf,
    BinaryNumeric,
    LeftRightText,
    NonExecutableProjection,
    NullTest,
    PositionText,
    ReplaceText,
    NumericScale,
    SubstringText,
    TextPredicate,
    UnaryNumeric,
    UnaryText,
}

///
/// FunctionSpec
///
/// FunctionSpec is the planner-owned semantic registry entry for one scalar
/// function identity. It carries the canonical category, null behavior,
/// determinism, and typing shape used by downstream planner consumers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FunctionSpec {
    pub(crate) category: FunctionCategory,
    pub(crate) null_behavior: FunctionNullBehavior,
    pub(crate) determinism: FunctionDeterminism,
    pub(crate) type_inference_shape: FunctionTypeInferenceShape,
    pub(crate) allowed_surfaces: &'static [FunctionSurface],
}

const BOOLEAN_FUNCTION_SURFACES: &[FunctionSurface] = &[
    FunctionSurface::ProjectionCondition,
    FunctionSurface::AggregateInputCondition,
    FunctionSurface::HavingCondition,
    FunctionSurface::Where,
];

const GENERAL_SCALAR_FUNCTION_SURFACES: &[FunctionSurface] = &[
    FunctionSurface::Projection,
    FunctionSurface::ProjectionCondition,
    FunctionSurface::AggregateInput,
    FunctionSurface::AggregateInputCondition,
    FunctionSurface::HavingCondition,
    FunctionSurface::Where,
];

const ROUND_FUNCTION_SURFACES: &[FunctionSurface] = &[
    FunctionSurface::Projection,
    FunctionSurface::ProjectionCondition,
    FunctionSurface::AggregateInput,
    FunctionSurface::HavingCondition,
    FunctionSurface::Where,
];

impl FunctionSpec {
    /// Build one planner-owned scalar function specification.
    #[must_use]
    pub(crate) const fn new(
        category: FunctionCategory,
        null_behavior: FunctionNullBehavior,
        determinism: FunctionDeterminism,
        type_inference_shape: FunctionTypeInferenceShape,
        allowed_surfaces: &'static [FunctionSurface],
    ) -> Self {
        Self {
            category,
            null_behavior,
            determinism,
            type_inference_shape,
            allowed_surfaces,
        }
    }

    /// Build one strict unary text-result specification.
    #[must_use]
    const fn strict_unary_text_result() -> Self {
        Self::new(
            FunctionCategory::Text,
            FunctionNullBehavior::Strict,
            FunctionDeterminism::Deterministic,
            FunctionTypeInferenceShape::TextResult {
                text_positions: &[0],
                numeric_positions: &[],
            },
            GENERAL_SCALAR_FUNCTION_SURFACES,
        )
    }

    /// Build one strict text-result specification with explicit text/numeric
    /// argument positions.
    #[must_use]
    const fn strict_text_result(
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
    ) -> Self {
        Self::new(
            FunctionCategory::Text,
            FunctionNullBehavior::Strict,
            FunctionDeterminism::Deterministic,
            FunctionTypeInferenceShape::TextResult {
                text_positions,
                numeric_positions,
            },
            GENERAL_SCALAR_FUNCTION_SURFACES,
        )
    }

    /// Build one strict text-predicate boolean specification.
    #[must_use]
    const fn strict_text_bool_result(text_positions: &'static [usize]) -> Self {
        Self::new(
            FunctionCategory::Text,
            FunctionNullBehavior::Strict,
            FunctionDeterminism::Deterministic,
            FunctionTypeInferenceShape::BoolResult { text_positions },
            GENERAL_SCALAR_FUNCTION_SURFACES,
        )
    }

    /// Build one strict numeric-result specification.
    #[must_use]
    const fn strict_numeric_result(
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
        subtype: NumericSubtype,
    ) -> Self {
        Self::new(
            FunctionCategory::Numeric,
            FunctionNullBehavior::Strict,
            FunctionDeterminism::Deterministic,
            FunctionTypeInferenceShape::NumericResult {
                text_positions,
                numeric_positions,
                subtype,
            },
            GENERAL_SCALAR_FUNCTION_SURFACES,
        )
    }

    /// Build one null-observing unary boolean predicate specification.
    #[must_use]
    const fn null_observing_unary_bool_predicate() -> Self {
        Self::new(
            FunctionCategory::BooleanPredicate,
            FunctionNullBehavior::NullObserving,
            FunctionDeterminism::Deterministic,
            FunctionTypeInferenceShape::UnaryBoolPredicate,
            BOOLEAN_FUNCTION_SURFACES,
        )
    }

    /// Return whether this function specification is admitted on the given
    /// planner-owned expression surface.
    #[must_use]
    pub(crate) fn supports_surface(self, surface: FunctionSurface) -> bool {
        let mut index = 0usize;
        while index < self.allowed_surfaces.len() {
            if matches!(self.allowed_surfaces[index], current if current == surface) {
                return true;
            }
            index = index.saturating_add(1);
        }

        false
    }
}

impl Function {
    /// Return the canonical planner-owned scalar function specification.
    #[must_use]
    pub(crate) const fn spec(self) -> FunctionSpec {
        match self {
            Self::Abs | Self::Ceiling | Self::Floor | Self::Sign | Self::Sqrt => {
                FunctionSpec::strict_numeric_result(&[], &[0], NumericSubtype::Decimal)
            }
            Self::Coalesce => FunctionSpec::new(
                FunctionCategory::NullHandling,
                FunctionNullBehavior::NullIgnoring,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::DynamicCoalesce,
                GENERAL_SCALAR_FUNCTION_SURFACES,
            ),
            Self::CollectionContains => FunctionSpec::new(
                FunctionCategory::Collection,
                FunctionNullBehavior::NullObserving,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::CollectionContains,
                BOOLEAN_FUNCTION_SURFACES,
            ),
            Self::Contains | Self::EndsWith | Self::StartsWith => {
                FunctionSpec::strict_text_bool_result(&[0, 1])
            }
            Self::IsEmpty | Self::IsMissing | Self::IsNotEmpty | Self::IsNotNull | Self::IsNull => {
                FunctionSpec::null_observing_unary_bool_predicate()
            }
            Self::Left | Self::Right => FunctionSpec::strict_text_result(&[0], &[1]),
            Self::Length => FunctionSpec::new(
                FunctionCategory::Text,
                FunctionNullBehavior::Strict,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::NumericResult {
                    text_positions: &[0],
                    numeric_positions: &[],
                    subtype: NumericSubtype::Integer,
                },
                GENERAL_SCALAR_FUNCTION_SURFACES,
            ),
            Self::Lower | Self::Ltrim | Self::Rtrim | Self::Trim | Self::Upper => {
                FunctionSpec::strict_unary_text_result()
            }
            Self::Mod | Self::Power => {
                FunctionSpec::strict_numeric_result(&[], &[0, 1], NumericSubtype::Decimal)
            }
            Self::NullIf => FunctionSpec::new(
                FunctionCategory::NullHandling,
                FunctionNullBehavior::NullIgnoring,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::DynamicNullIf,
                GENERAL_SCALAR_FUNCTION_SURFACES,
            ),
            Self::Position => FunctionSpec::new(
                FunctionCategory::Text,
                FunctionNullBehavior::Strict,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::NumericResult {
                    text_positions: &[0, 1],
                    numeric_positions: &[],
                    subtype: NumericSubtype::Integer,
                },
                GENERAL_SCALAR_FUNCTION_SURFACES,
            ),
            Self::Replace => FunctionSpec::strict_text_result(&[0, 1, 2], &[]),
            Self::Round | Self::Trunc => FunctionSpec::new(
                FunctionCategory::Numeric,
                FunctionNullBehavior::Strict,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::NumericScaleResult,
                ROUND_FUNCTION_SURFACES,
            ),
            Self::Substring => FunctionSpec::strict_text_result(&[0], &[1, 2]),
        }
    }

    /// Return the planner-owned typing shape for this function.
    #[must_use]
    pub(crate) const fn type_inference_shape(self) -> FunctionTypeInferenceShape {
        self.spec().type_inference_shape
    }

    /// Return whether this canonical scalar function is admitted on the given
    /// planner-owned expression surface.
    #[must_use]
    pub(crate) fn supports_surface(self, surface: FunctionSurface) -> bool {
        self.spec().supports_surface(surface)
    }

    /// Report whether planner typing classifies this scalar function as part
    /// of the text/numeric compare-operand family consumed by canonicalization.
    #[must_use]
    pub(crate) const fn is_compare_operand_coarse_family(self) -> bool {
        matches!(
            self.type_inference_shape(),
            FunctionTypeInferenceShape::TextResult { .. }
                | FunctionTypeInferenceShape::NumericResult { .. }
                | FunctionTypeInferenceShape::NumericScaleResult
                | FunctionTypeInferenceShape::DynamicCoalesce
                | FunctionTypeInferenceShape::DynamicNullIf
        )
    }

    /// Return one fixed decimal display scale implied by this scalar function
    /// and its planner-frozen arguments, if the function family carries one.
    #[must_use]
    pub(crate) fn fixed_decimal_scale(self, args: &[Expr]) -> Option<u32> {
        if !matches!(self, Self::Round | Self::Trunc) {
            return None;
        }

        match args.get(1) {
            Some(Expr::Literal(Value::Uint(scale))) => u32::try_from(*scale).ok(),
            Some(Expr::Literal(Value::Int(scale))) if *scale >= 0 => u32::try_from(*scale).ok(),
            _ => None,
        }
    }

    /// Return the planner-owned boolean function family used by truth
    /// admission, normalization, and predicate compilation, if this function
    /// participates in that bounded boolean surface.
    #[must_use]
    pub(crate) const fn boolean_function_shape(self) -> Option<BooleanFunctionShape> {
        match self {
            Self::Coalesce => Some(BooleanFunctionShape::TruthCoalesce),
            Self::IsNull | Self::IsNotNull => Some(BooleanFunctionShape::NullTest),
            Self::StartsWith | Self::EndsWith | Self::Contains => {
                Some(BooleanFunctionShape::TextPredicate)
            }
            Self::IsMissing | Self::IsEmpty | Self::IsNotEmpty => {
                Some(BooleanFunctionShape::FieldPredicate)
            }
            Self::CollectionContains => Some(BooleanFunctionShape::CollectionContains),
            Self::Abs
            | Self::Ceiling
            | Self::Floor
            | Self::Left
            | Self::Length
            | Self::Lower
            | Self::Ltrim
            | Self::Mod
            | Self::NullIf
            | Self::Position
            | Self::Power
            | Self::Replace
            | Self::Right
            | Self::Round
            | Self::Rtrim
            | Self::Sign
            | Self::Substring
            | Self::Sqrt
            | Self::Trim
            | Self::Trunc
            | Self::Upper => None,
        }
    }

    /// Return the finer null-test kind once this function has already been
    /// admitted onto the bounded boolean null-test surface.
    #[must_use]
    pub(crate) const fn boolean_null_test_kind(self) -> Option<NullTestFunctionKind> {
        match self {
            Self::IsNull => Some(NullTestFunctionKind::IsNull),
            Self::IsNotNull => Some(NullTestFunctionKind::IsNotNull),
            _ => None,
        }
    }

    /// Return the finer text-predicate kind once this function has already
    /// been admitted onto the bounded boolean text-predicate surface.
    #[must_use]
    pub(crate) const fn boolean_text_predicate_kind(self) -> Option<TextPredicateFunctionKind> {
        match self {
            Self::StartsWith => Some(TextPredicateFunctionKind::StartsWith),
            Self::EndsWith => Some(TextPredicateFunctionKind::EndsWith),
            Self::Contains => Some(TextPredicateFunctionKind::Contains),
            _ => None,
        }
    }

    /// Return the finer field-predicate kind once this function has already
    /// been admitted onto the bounded boolean field-predicate surface.
    #[must_use]
    pub(crate) const fn boolean_field_predicate_kind(self) -> Option<FieldPredicateFunctionKind> {
        match self {
            Self::IsMissing => Some(FieldPredicateFunctionKind::Missing),
            Self::IsEmpty => Some(FieldPredicateFunctionKind::Empty),
            Self::IsNotEmpty => Some(FieldPredicateFunctionKind::NotEmpty),
            _ => None,
        }
    }

    /// Return whether this canonical scalar function is one of the admitted
    /// text casefold transforms that preserve shared LOWER/UPPER wrapper
    /// semantics across planner expression surfaces.
    #[must_use]
    pub(crate) const fn is_casefold_transform(self) -> bool {
        matches!(self, Self::Lower | Self::Upper)
    }

    /// Return the finer unary text transform kind once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(crate) const fn unary_text_function_kind(self) -> Option<UnaryTextFunctionKind> {
        match self {
            Self::Trim => Some(UnaryTextFunctionKind::Trim),
            Self::Ltrim => Some(UnaryTextFunctionKind::Ltrim),
            Self::Rtrim => Some(UnaryTextFunctionKind::Rtrim),
            Self::Lower => Some(UnaryTextFunctionKind::Lower),
            Self::Upper => Some(UnaryTextFunctionKind::Upper),
            Self::Length => Some(UnaryTextFunctionKind::Length),
            _ => None,
        }
    }

    /// Return the finer unary numeric transform kind once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(crate) const fn unary_numeric_function_kind(self) -> Option<UnaryNumericFunctionKind> {
        match self {
            Self::Abs => Some(UnaryNumericFunctionKind::Abs),
            Self::Ceiling => Some(UnaryNumericFunctionKind::Ceiling),
            Self::Floor => Some(UnaryNumericFunctionKind::Floor),
            Self::Sign => Some(UnaryNumericFunctionKind::Sign),
            Self::Sqrt => Some(UnaryNumericFunctionKind::Sqrt),
            _ => None,
        }
    }

    /// Return the finer binary numeric transform kind once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(crate) const fn binary_numeric_function_kind(self) -> Option<BinaryNumericFunctionKind> {
        match self {
            Self::Mod => Some(BinaryNumericFunctionKind::Mod),
            Self::Power => Some(BinaryNumericFunctionKind::Power),
            _ => None,
        }
    }

    /// Return the finer scale-taking numeric transform kind once scalar
    /// evaluation has already proven this function belongs to that family.
    #[must_use]
    pub(crate) const fn numeric_scale_function_kind(self) -> Option<NumericScaleFunctionKind> {
        match self {
            Self::Round => Some(NumericScaleFunctionKind::Round),
            Self::Trunc => Some(NumericScaleFunctionKind::Trunc),
            _ => None,
        }
    }

    /// Return the LEFT versus RIGHT distinction once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(crate) const fn left_right_text_function_kind(self) -> Option<LeftRightTextFunctionKind> {
        match self {
            Self::Left => Some(LeftRightTextFunctionKind::Left),
            Self::Right => Some(LeftRightTextFunctionKind::Right),
            _ => None,
        }
    }

    /// Return the bounded scalar-evaluation behavior family shared by
    /// planner literal preview and executor scalar projection evaluation.
    #[must_use]
    pub(crate) const fn scalar_eval_shape(self) -> ScalarEvalFunctionShape {
        match self {
            Self::IsNull | Self::IsNotNull => ScalarEvalFunctionShape::NullTest,
            Self::IsMissing | Self::IsEmpty | Self::IsNotEmpty | Self::CollectionContains => {
                ScalarEvalFunctionShape::NonExecutableProjection
            }
            Self::Trim | Self::Ltrim | Self::Rtrim | Self::Lower | Self::Upper | Self::Length => {
                ScalarEvalFunctionShape::UnaryText
            }
            Self::Coalesce => ScalarEvalFunctionShape::DynamicCoalesce,
            Self::NullIf => ScalarEvalFunctionShape::DynamicNullIf,
            Self::Abs | Self::Ceiling | Self::Floor | Self::Sign | Self::Sqrt => {
                ScalarEvalFunctionShape::UnaryNumeric
            }
            Self::Mod | Self::Power => ScalarEvalFunctionShape::BinaryNumeric,
            Self::Left | Self::Right => ScalarEvalFunctionShape::LeftRightText,
            Self::StartsWith | Self::EndsWith | Self::Contains => {
                ScalarEvalFunctionShape::TextPredicate
            }
            Self::Position => ScalarEvalFunctionShape::PositionText,
            Self::Replace => ScalarEvalFunctionShape::ReplaceText,
            Self::Substring => ScalarEvalFunctionShape::SubstringText,
            Self::Round | Self::Trunc => ScalarEvalFunctionShape::NumericScale,
        }
    }

    /// Return the stable executor-facing projection function name used by
    /// scalar projection evaluation diagnostics and invariant messages.
    #[must_use]
    pub(crate) const fn projection_eval_name(self) -> &'static str {
        match self {
            Self::IsNull => "is_null",
            Self::IsNotNull => "is_not_null",
            Self::IsMissing => "is_missing",
            Self::IsEmpty => "is_empty",
            Self::IsNotEmpty => "is_not_empty",
            Self::Trim => "trim",
            Self::Ltrim => "ltrim",
            Self::Rtrim => "rtrim",
            Self::Coalesce => "coalesce",
            Self::NullIf => "nullif",
            Self::Abs => "abs",
            Self::Ceiling => "ceiling",
            Self::Floor => "floor",
            Self::Sign => "sign",
            Self::Sqrt => "sqrt",
            Self::Mod => "mod",
            Self::Power => "power",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Length => "length",
            Self::Left => "left",
            Self::Right => "right",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
            Self::Contains => "contains",
            Self::CollectionContains => "collection_contains",
            Self::Position => "position",
            Self::Replace => "replace",
            Self::Substring => "substring",
            Self::Round => "round",
            Self::Trunc => "trunc",
        }
    }

    /// Return the aggregate-input constant-fold family for this scalar
    /// function when a literal-only aggregate input can collapse
    /// deterministically.
    #[must_use]
    pub(crate) const fn aggregate_input_constant_fold_shape(
        self,
    ) -> Option<AggregateInputConstantFoldShape> {
        match self {
            Self::Round | Self::Trunc => Some(AggregateInputConstantFoldShape::Round),
            Self::Coalesce => Some(AggregateInputConstantFoldShape::DynamicCoalesce),
            Self::NullIf => Some(AggregateInputConstantFoldShape::DynamicNullIf),
            Self::Mod | Self::Power => Some(AggregateInputConstantFoldShape::BinaryNumeric),
            Self::Abs | Self::Ceiling | Self::Floor | Self::Sign | Self::Sqrt => {
                Some(AggregateInputConstantFoldShape::UnaryNumeric)
            }
            Self::IsNull
            | Self::IsNotNull
            | Self::IsMissing
            | Self::IsEmpty
            | Self::IsNotEmpty
            | Self::Trim
            | Self::Ltrim
            | Self::Rtrim
            | Self::Left
            | Self::Right
            | Self::StartsWith
            | Self::EndsWith
            | Self::Contains
            | Self::CollectionContains
            | Self::Position
            | Self::Replace
            | Self::Substring
            | Self::Lower
            | Self::Upper
            | Self::Length => None,
        }
    }

    /// Evaluate one admitted COALESCE call after caller-side arity validation.
    #[must_use]
    pub(crate) fn eval_coalesce_values(self, args: &[Value]) -> Value {
        debug_assert!(matches!(self, Self::Coalesce));

        args.iter()
            .find(|value| !matches!(value, Value::Null))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Evaluate one admitted NULLIF result once the caller has already computed
    /// its equality outcome through the layer-owned comparison boundary.
    #[must_use]
    pub(crate) fn eval_nullif_values(self, left: &Value, right: &Value, equals: bool) -> Value {
        debug_assert!(matches!(self, Self::NullIf));

        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return left.clone();
        }

        if equals { Value::Null } else { left.clone() }
    }

    /// Evaluate one admitted POSITION call after the caller has already
    /// validated both text operands.
    #[must_use]
    pub(crate) fn eval_position_text(self, text: &str, needle: &str) -> Value {
        debug_assert!(matches!(self, Self::Position));

        Value::Uint(Self::text_position_1_based(text, needle))
    }

    /// Evaluate one admitted REPLACE call after the caller has already
    /// validated all text operands.
    #[must_use]
    pub(crate) fn eval_replace_text(self, text: &str, from: &str, to: &str) -> Value {
        debug_assert!(matches!(self, Self::Replace));

        Value::Text(text.replace(from, to))
    }

    /// Evaluate one admitted SUBSTRING call after the caller has already
    /// validated the text and integer operands.
    #[must_use]
    pub(crate) fn eval_substring_text(self, text: &str, start: i64, length: Option<i64>) -> Value {
        debug_assert!(matches!(self, Self::Substring));

        Value::Text(Self::substring_1_based(text, start, length))
    }

    /// Evaluate one admitted scale-taking numeric call after the caller has
    /// already validated the non-negative scale boundary.
    #[must_use]
    pub(crate) fn eval_numeric_scale(self, value: &Value, scale: u32) -> Option<Value> {
        debug_assert!(matches!(self, Self::Round | Self::Trunc));

        let decimal = coerce_numeric_decimal(value)?;

        Some(
            self.numeric_scale_function_kind()?
                .eval_decimal(decimal, scale),
        )
    }

    /// Convert one found substring byte offset into the stable 1-based SQL
    /// char position used by POSITION(...).
    fn text_position_1_based(haystack: &str, needle: &str) -> u64 {
        let Some(byte_index) = haystack.find(needle) else {
            return 0;
        };
        let char_offset = haystack[..byte_index].chars().count();

        u64::try_from(char_offset)
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }

    /// Slice one text input using SQL-style 1-based substring coordinates.
    fn substring_1_based(text: &str, start: i64, length: Option<i64>) -> String {
        if start <= 0 {
            return String::new();
        }
        if matches!(length, Some(inner) if inner <= 0) {
            return String::new();
        }

        let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
        let chars = text.chars().skip(start_index);

        match length {
            Some(length) => chars
                .take(usize::try_from(length).unwrap_or(usize::MAX))
                .collect(),
            None => chars.collect(),
        }
    }
}
