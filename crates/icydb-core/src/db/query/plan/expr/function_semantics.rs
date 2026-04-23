//! Module: query::plan::expr::function_semantics
//! Responsibility: planner-owned scalar function taxonomy and semantic facets.
//! Does not own: SQL parser identifier resolution, expression lowering, or runtime evaluation.
//! Boundary: central registry for scalar function category, null behavior, determinism, and typing shape.

use crate::{
    db::query::plan::expr::ast::{Expr, Function},
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
    RoundNumericResult,
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
/// This keeps builder-side and SQL-lowering-side constant-fold admission on
/// one enum-owned contract instead of repeating the same foldable subsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateInputConstantFoldShape {
    DynamicCoalesce,
    DynamicNullIf,
    Round,
    UnaryNumeric,
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
            Self::Abs | Self::Ceiling | Self::Floor => {
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
            Self::Round => FunctionSpec::new(
                FunctionCategory::Numeric,
                FunctionNullBehavior::Strict,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::RoundNumericResult,
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
                | FunctionTypeInferenceShape::RoundNumericResult
                | FunctionTypeInferenceShape::DynamicCoalesce
                | FunctionTypeInferenceShape::DynamicNullIf
        )
    }

    /// Return one fixed decimal display scale implied by this scalar function
    /// and its planner-frozen arguments, if the function family carries one.
    #[must_use]
    pub(crate) fn fixed_decimal_scale(self, args: &[Expr]) -> Option<u32> {
        if !matches!(self, Self::Round) {
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
            | Self::NullIf
            | Self::Position
            | Self::Replace
            | Self::Right
            | Self::Round
            | Self::Rtrim
            | Self::Substring
            | Self::Trim
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

    /// Return the aggregate-input constant-fold family for this scalar
    /// function when a literal-only aggregate input can collapse
    /// deterministically.
    #[must_use]
    pub(crate) const fn aggregate_input_constant_fold_shape(
        self,
    ) -> Option<AggregateInputConstantFoldShape> {
        match self {
            Self::Round => Some(AggregateInputConstantFoldShape::Round),
            Self::Coalesce => Some(AggregateInputConstantFoldShape::DynamicCoalesce),
            Self::NullIf => Some(AggregateInputConstantFoldShape::DynamicNullIf),
            Self::Abs | Self::Ceiling | Self::Floor => {
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
}
