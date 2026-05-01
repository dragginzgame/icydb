use crate::{
    db::query::plan::expr::{
        Expr, Function,
        function_semantics::types::{
            AggregateInputConstantFoldShape, BinaryNumericFunctionKind, BooleanFunctionShape,
            FieldPredicateFunctionKind, FunctionCategory, FunctionDeterminism,
            FunctionNullBehavior, FunctionSurface, FunctionTypeInferenceShape,
            LeftRightTextFunctionKind, NullTestFunctionKind, NumericScaleFunctionKind,
            NumericSubtype, ScalarEvalFunctionShape, TextPredicateFunctionKind,
            UnaryNumericFunctionKind, UnaryTextFunctionKind,
        },
    },
    value::Value,
};

///
/// FunctionSpec
///
/// FunctionSpec is the planner-owned semantic registry entry for one scalar
/// function identity. It carries the canonical category, null behavior,
/// determinism, and typing shape used by downstream planner consumers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr) struct FunctionSpec {
    pub(in crate::db::query::plan::expr) category: FunctionCategory,
    pub(in crate::db::query::plan::expr) null_behavior: FunctionNullBehavior,
    pub(in crate::db::query::plan::expr) determinism: FunctionDeterminism,
    pub(in crate::db::query::plan::expr) type_inference_shape: FunctionTypeInferenceShape,
    pub(in crate::db::query::plan::expr) allowed_surfaces: &'static [FunctionSurface],
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
    pub(in crate::db::query::plan::expr) const fn new(
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
    pub(in crate::db::query::plan::expr) fn supports_surface(
        self,
        surface: FunctionSurface,
    ) -> bool {
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
    pub(in crate::db::query::plan::expr) const fn spec(self) -> FunctionSpec {
        match self {
            Self::Abs
            | Self::Cbrt
            | Self::Ceiling
            | Self::Exp
            | Self::Floor
            | Self::Ln
            | Self::Log10
            | Self::Log2
            | Self::Sign
            | Self::Sqrt => FunctionSpec::strict_numeric_result(&[], &[0], NumericSubtype::Decimal),
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
            Self::OctetLength => FunctionSpec::new(
                FunctionCategory::Numeric,
                FunctionNullBehavior::Strict,
                FunctionDeterminism::Deterministic,
                FunctionTypeInferenceShape::ByteLengthResult,
                GENERAL_SCALAR_FUNCTION_SURFACES,
            ),
            Self::Lower | Self::Ltrim | Self::Rtrim | Self::Trim | Self::Upper => {
                FunctionSpec::strict_unary_text_result()
            }
            Self::Log | Self::Mod | Self::Power => {
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
    pub(in crate::db::query::plan::expr) const fn type_inference_shape(
        self,
    ) -> FunctionTypeInferenceShape {
        self.spec().type_inference_shape
    }

    /// Return whether this canonical scalar function is admitted on the given
    /// planner-owned expression surface.
    #[must_use]
    pub(in crate::db) fn supports_surface(self, surface: FunctionSurface) -> bool {
        self.spec().supports_surface(surface)
    }

    /// Report whether planner typing classifies this scalar function as part
    /// of the text/numeric compare-operand family consumed by canonicalization.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn is_compare_operand_coarse_family(self) -> bool {
        matches!(
            self.type_inference_shape(),
            FunctionTypeInferenceShape::TextResult { .. }
                | FunctionTypeInferenceShape::NumericResult { .. }
                | FunctionTypeInferenceShape::ByteLengthResult
                | FunctionTypeInferenceShape::NumericScaleResult
                | FunctionTypeInferenceShape::DynamicCoalesce
                | FunctionTypeInferenceShape::DynamicNullIf
        )
    }

    /// Return one fixed decimal display scale implied by this scalar function
    /// and its planner-frozen arguments, if the function family carries one.
    #[must_use]
    pub(in crate::db) fn fixed_decimal_scale(self, args: &[Expr]) -> Option<u32> {
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
    pub(in crate::db::query::plan::expr) const fn boolean_function_shape(
        self,
    ) -> Option<BooleanFunctionShape> {
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
            | Self::Cbrt
            | Self::Ceiling
            | Self::Exp
            | Self::Floor
            | Self::Left
            | Self::Length
            | Self::Ln
            | Self::Log
            | Self::Log10
            | Self::Log2
            | Self::Lower
            | Self::Ltrim
            | Self::Mod
            | Self::NullIf
            | Self::OctetLength
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
    pub(in crate::db::query::plan::expr) const fn boolean_null_test_kind(
        self,
    ) -> Option<NullTestFunctionKind> {
        match self {
            Self::IsNull => Some(NullTestFunctionKind::IsNull),
            Self::IsNotNull => Some(NullTestFunctionKind::IsNotNull),
            _ => None,
        }
    }

    /// Return the finer text-predicate kind once this function has already
    /// been admitted onto the bounded boolean text-predicate surface.
    #[must_use]
    pub(in crate::db) const fn boolean_text_predicate_kind(
        self,
    ) -> Option<TextPredicateFunctionKind> {
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
    pub(in crate::db::query::plan::expr) const fn boolean_field_predicate_kind(
        self,
    ) -> Option<FieldPredicateFunctionKind> {
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
    pub(in crate::db::query::plan::expr) const fn is_casefold_transform(self) -> bool {
        matches!(self, Self::Lower | Self::Upper)
    }

    /// Return the finer unary text transform kind once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn unary_text_function_kind(
        self,
    ) -> Option<UnaryTextFunctionKind> {
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
    pub(in crate::db::query::plan::expr) const fn unary_numeric_function_kind(
        self,
    ) -> Option<UnaryNumericFunctionKind> {
        match self {
            Self::Abs => Some(UnaryNumericFunctionKind::Abs),
            Self::Cbrt => Some(UnaryNumericFunctionKind::Cbrt),
            Self::Ceiling => Some(UnaryNumericFunctionKind::Ceiling),
            Self::Exp => Some(UnaryNumericFunctionKind::Exp),
            Self::Floor => Some(UnaryNumericFunctionKind::Floor),
            Self::Ln => Some(UnaryNumericFunctionKind::Ln),
            Self::Log10 => Some(UnaryNumericFunctionKind::Log10),
            Self::Log2 => Some(UnaryNumericFunctionKind::Log2),
            Self::Sign => Some(UnaryNumericFunctionKind::Sign),
            Self::Sqrt => Some(UnaryNumericFunctionKind::Sqrt),
            _ => None,
        }
    }

    /// Return the finer binary numeric transform kind once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn binary_numeric_function_kind(
        self,
    ) -> Option<BinaryNumericFunctionKind> {
        match self {
            Self::Log => Some(BinaryNumericFunctionKind::Log),
            Self::Mod => Some(BinaryNumericFunctionKind::Mod),
            Self::Power => Some(BinaryNumericFunctionKind::Power),
            _ => None,
        }
    }

    /// Return the finer scale-taking numeric transform kind once scalar
    /// evaluation has already proven this function belongs to that family.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn numeric_scale_function_kind(
        self,
    ) -> Option<NumericScaleFunctionKind> {
        match self {
            Self::Round => Some(NumericScaleFunctionKind::Round),
            Self::Trunc => Some(NumericScaleFunctionKind::Trunc),
            _ => None,
        }
    }

    /// Return the LEFT versus RIGHT distinction once scalar-evaluation
    /// dispatch has already proven this function belongs to that family.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn left_right_text_function_kind(
        self,
    ) -> Option<LeftRightTextFunctionKind> {
        match self {
            Self::Left => Some(LeftRightTextFunctionKind::Left),
            Self::Right => Some(LeftRightTextFunctionKind::Right),
            _ => None,
        }
    }

    /// Return the bounded scalar-evaluation behavior family shared by
    /// planner literal preview and executor scalar projection evaluation.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn scalar_eval_shape(
        self,
    ) -> ScalarEvalFunctionShape {
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
            Self::OctetLength => ScalarEvalFunctionShape::OctetLength,
            Self::Abs
            | Self::Cbrt
            | Self::Ceiling
            | Self::Exp
            | Self::Floor
            | Self::Ln
            | Self::Log10
            | Self::Log2
            | Self::Sign
            | Self::Sqrt => ScalarEvalFunctionShape::UnaryNumeric,
            Self::Log | Self::Mod | Self::Power => ScalarEvalFunctionShape::BinaryNumeric,
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
    pub(in crate::db::query::plan::expr) const fn projection_eval_name(self) -> &'static str {
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
            Self::OctetLength => "octet_length",
            Self::Abs => "abs",
            Self::Cbrt => "cbrt",
            Self::Ceiling => "ceiling",
            Self::Exp => "exp",
            Self::Floor => "floor",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Log10 => "log10",
            Self::Log2 => "log2",
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
    pub(in crate::db::query::plan::expr) const fn aggregate_input_constant_fold_shape(
        self,
    ) -> Option<AggregateInputConstantFoldShape> {
        match self {
            Self::Round | Self::Trunc => Some(AggregateInputConstantFoldShape::Round),
            Self::Coalesce => Some(AggregateInputConstantFoldShape::DynamicCoalesce),
            Self::NullIf => Some(AggregateInputConstantFoldShape::DynamicNullIf),
            Self::Log | Self::Mod | Self::Power => {
                Some(AggregateInputConstantFoldShape::BinaryNumeric)
            }
            Self::Abs
            | Self::Cbrt
            | Self::Ceiling
            | Self::Exp
            | Self::Floor
            | Self::Ln
            | Self::Log10
            | Self::Log2
            | Self::Sign
            | Self::Sqrt => Some(AggregateInputConstantFoldShape::UnaryNumeric),
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
            | Self::Length
            | Self::OctetLength => None,
        }
    }
}
