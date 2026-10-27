use crate::{
    db::{
        numeric::{
            NumericArithmeticOp, NumericEvalError, apply_decimal_arithmetic_checked,
            apply_value_arithmetic_checked, coerce_numeric_decimal, decimal_cbrt_checked,
            decimal_exp_checked, decimal_ln_checked, decimal_log_base_checked,
            decimal_log2_checked, decimal_log10_checked, decimal_power_checked, decimal_sign,
            decimal_sqrt_checked,
        },
        query::plan::expr::{
            Function,
            function_semantics::types::{
                BinaryNumericFunctionKind, LeftRightTextFunctionKind, NumericScaleFunctionKind,
                UnaryNumericFunctionKind, UnaryTextFunctionKind,
            },
        },
    },
    types::Decimal,
    value::{
        Value, lower_text, lower_text_construction_allowance, upper_text,
        upper_text_construction_allowance,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Borrow;

impl UnaryTextFunctionKind {
    /// Evaluate one admitted unary text transform against one text input.
    pub(in crate::db::query::plan::expr) fn eval_text<E>(
        self,
        text: &str,
        mut charge: impl FnMut(Resource, u64) -> Result<(), E>,
    ) -> Result<Value, E> {
        let casing = match self {
            Self::Lower => Some(lower_text_construction_allowance(text.len())),
            Self::Upper => Some(upper_text_construction_allowance(text.len())),
            Self::Trim | Self::Ltrim | Self::Rtrim | Self::Length => None,
        };
        if let Some((backing, work)) = casing {
            charge(Resource::PredicateExpressionSteps, work)?;
            charge(Resource::TemporaryBytes, backing)?;
        } else {
            // Admit every input walk before inspecting UTF-8 boundaries. The
            // caller supplies its existing runtime or preparation authority.
            charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
        }
        match self {
            Self::Trim => construct_text_slice(text.trim(), &mut charge),
            Self::Ltrim => construct_text_slice(text.trim_start(), &mut charge),
            Self::Rtrim => construct_text_slice(text.trim_end(), &mut charge),
            Self::Length => Ok(Value::Nat64(
                u64::try_from(text.chars().count()).unwrap_or(u64::MAX),
            )),
            Self::Lower => Ok(Value::Text(lower_text(text))),
            Self::Upper => Ok(Value::Text(upper_text(text))),
        }
    }
}

impl UnaryNumericFunctionKind {
    /// Evaluate one admitted unary numeric transform against one decimal input.
    pub(in crate::db::query::plan::expr) fn eval_decimal(
        self,
        decimal: Decimal,
    ) -> Result<Value, NumericEvalError> {
        let result = match self {
            Self::Abs => decimal.checked_abs().ok_or(NumericEvalError::Overflow)?,
            Self::Cbrt => decimal_cbrt_checked(decimal)?,
            Self::Ceiling => decimal.ceil_dp0(),
            Self::Exp => decimal_exp_checked(decimal)?,
            Self::Floor => decimal.floor_dp0(),
            Self::Ln => decimal_ln_checked(decimal)?,
            Self::Log10 => decimal_log10_checked(decimal)?,
            Self::Log2 => decimal_log2_checked(decimal)?,
            Self::Sign => decimal_sign(decimal),
            Self::Sqrt => decimal_sqrt_checked(decimal)?,
        };

        Ok(Value::Decimal(result))
    }
}

impl BinaryNumericFunctionKind {
    /// Evaluate one admitted binary transform against compatible value domains.
    ///
    /// `MOD(U256, U256)` remains fixed-width. Other binary numeric functions
    /// retain the existing decimal coercion contract.
    pub(in crate::db::query::plan::expr) fn eval_values(
        self,
        left: &Value,
        right: &Value,
    ) -> Result<Option<Value>, NumericEvalError> {
        if matches!(self, Self::Mod) {
            return apply_value_arithmetic_checked(NumericArithmeticOp::Rem, left, right);
        }

        let Some(left) = coerce_numeric_decimal(left) else {
            return Ok(None);
        };
        let Some(right) = coerce_numeric_decimal(right) else {
            return Ok(None);
        };

        self.eval_decimal(left, right).map(Some)
    }

    /// Evaluate one admitted binary numeric transform against decimal inputs.
    pub(in crate::db::query::plan::expr) fn eval_decimal(
        self,
        left: Decimal,
        right: Decimal,
    ) -> Result<Value, NumericEvalError> {
        let result = match self {
            Self::Log => decimal_log_base_checked(left, right)?,
            Self::Mod => apply_decimal_arithmetic_checked(NumericArithmeticOp::Rem, left, right)?,
            Self::Power => decimal_power_checked(left, right)?,
        };

        Ok(Value::Decimal(result))
    }
}

impl NumericScaleFunctionKind {
    /// Evaluate one admitted scale-taking numeric transform.
    #[must_use]
    pub(in crate::db::query::plan::expr) const fn eval_decimal(
        self,
        decimal: Decimal,
        scale: u32,
    ) -> Value {
        let result = match self {
            Self::Round => decimal.round_dp(scale),
            Self::Trunc => decimal.trunc_dp(scale),
        };

        Value::Decimal(result)
    }
}

impl LeftRightTextFunctionKind {
    /// Evaluate one admitted LEFT/RIGHT transform against one text input.
    pub(in crate::db::query::plan::expr) fn eval_text<E>(
        self,
        text: &str,
        count: i64,
        mut charge: impl FnMut(Resource, u64) -> Result<(), E>,
    ) -> Result<Value, E> {
        if count <= 0 {
            return Ok(Value::Text(String::new()));
        }
        charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
        let output = match self {
            Self::Left => Self::left_chars(text, count),
            Self::Right => Self::right_chars(text, count),
        };
        construct_text_slice(output, &mut charge)
    }

    /// Return the first N chars from one text input while keeping
    /// negative/zero lengths on the empty-string SQL boundary.
    fn left_chars(text: &str, count: i64) -> &str {
        if count <= 0 {
            return "";
        }
        let end = text
            .char_indices()
            .nth(usize::try_from(count).unwrap_or(usize::MAX))
            .map_or(text.len(), |(index, _)| index);
        &text[..end]
    }

    /// Return the last N chars from one text input while keeping
    /// negative/zero lengths on the empty-string SQL boundary.
    fn right_chars(text: &str, count: i64) -> &str {
        if count <= 0 {
            return "";
        }
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        let start = text
            .char_indices()
            .rev()
            .nth(count - 1)
            .map_or(0, |(index, _)| index);
        &text[start..]
    }
}

impl Function {
    /// Evaluate one admitted COALESCE call after caller-side arity validation.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_coalesce_values(
        self,
        args: &[impl Borrow<Value>],
    ) -> Value {
        debug_assert!(matches!(self, Self::Coalesce));

        args.iter()
            .map(Borrow::borrow)
            .find(|value| !matches!(value, Value::Null))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Evaluate one admitted NULLIF result once the caller has already computed
    /// its equality outcome through the layer-owned comparison boundary.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_nullif_values(
        self,
        left: &Value,
        right: &Value,
        equals: bool,
    ) -> Value {
        debug_assert!(matches!(self, Self::NullIf));

        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return left.clone();
        }

        if equals { Value::Null } else { left.clone() }
    }

    /// Evaluate one admitted POSITION call after the caller has already
    /// validated both text operands.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_position_text(
        self,
        text: &str,
        needle: &str,
    ) -> Value {
        debug_assert!(matches!(self, Self::Position));

        Value::Nat64(Self::text_position_1_based(text, needle))
    }

    /// Compute exact UTF-8 output bytes using the same nonoverlapping match
    /// boundaries as construction. Saturation lets finite budgets reject an
    /// unrepresentable output before any allocation.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn replace_text_output_len(
        text: &str,
        from: &str,
        to: &str,
    ) -> usize {
        if from.is_empty() || from == to {
            return text.len();
        }
        let count = text.matches(from).count();
        text.len()
            .saturating_sub(count.saturating_mul(from.len()))
            .saturating_add(count.saturating_mul(to.len()))
    }

    /// Construct REPLACE after text validation and exact output-size admission.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_replace_text(
        self,
        text: &str,
        from: &str,
        to: &str,
        output_len: usize,
    ) -> Value {
        debug_assert!(matches!(self, Self::Replace));
        let mut output = String::with_capacity(output_len);
        // Empty SQL searches and identity substitutions preserve the input.
        if from.is_empty() || from == to {
            output.push_str(text);
        } else {
            let mut copied = 0;
            for (start, matched) in text.match_indices(from) {
                // match_indices guarantees ascending UTF-8 boundaries within
                // this exact input, including nonoverlapping adjacent matches.
                output.push_str(&text[copied..start]);
                output.push_str(to);
                copied = start + matched.len();
            }
            output.push_str(&text[copied..]);
        }
        Value::Text(output)
    }

    /// Evaluate one admitted SUBSTRING call after the caller has already
    /// validated the text and integer operands.
    pub(in crate::db::query::plan::expr) fn eval_substring_text<E>(
        self,
        text: &str,
        start: i64,
        length: Option<i64>,
        mut charge: impl FnMut(Resource, u64) -> Result<(), E>,
    ) -> Result<Value, E> {
        debug_assert!(matches!(self, Self::Substring));

        if start <= 0 || length.is_some_and(|length| length <= 0) {
            return Ok(Value::Text(String::new()));
        }
        charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
        construct_text_slice(Self::substring_1_based(text, start, length), &mut charge)
    }

    /// Evaluate one admitted scale-taking numeric call after the caller has
    /// already validated the non-negative scale boundary.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_numeric_scale(
        self,
        value: &Value,
        scale: u32,
    ) -> Option<Value> {
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
    fn substring_1_based(text: &str, start: i64, length: Option<i64>) -> &str {
        if start <= 0 {
            return "";
        }
        if matches!(length, Some(inner) if inner <= 0) {
            return "";
        }

        let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
        let Some((start, _)) = text.char_indices().nth(start_index) else {
            return "";
        };
        let suffix = &text[start..];
        let end = length.map_or(suffix.len(), |length| {
            suffix
                .char_indices()
                .nth(usize::try_from(length).unwrap_or(usize::MAX))
                .map_or(suffix.len(), |(index, _)| index)
        });
        &suffix[..end]
    }
}

// Slicing performs no allocation. Admit exact requested backing and copy work
// before constructing the selected bytes, sharing one policy across evaluators.
fn construct_text_slice<E>(
    text: &str,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<Value, E> {
    charge(Resource::TemporaryBytes, text.len() as u64)?;
    charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
    Ok(Value::Text(text.to_owned()))
}
