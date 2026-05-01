use crate::{
    db::{
        numeric::{
            NumericArithmeticOp, NumericEvalError, apply_decimal_arithmetic_checked,
            coerce_numeric_decimal, decimal_cbrt_checked, decimal_exp_checked, decimal_ln_checked,
            decimal_log_base_checked, decimal_log2_checked, decimal_log10_checked,
            decimal_power_checked, decimal_sign, decimal_sqrt_checked,
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
    value::Value,
};

impl UnaryTextFunctionKind {
    /// Evaluate one admitted unary text transform against one text input.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_text(self, text: &str) -> Value {
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
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_text(self, text: &str, count: i64) -> Value {
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

impl Function {
    /// Evaluate one admitted COALESCE call after caller-side arity validation.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_coalesce_values(self, args: &[Value]) -> Value {
        debug_assert!(matches!(self, Self::Coalesce));

        args.iter()
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

        Value::Uint(Self::text_position_1_based(text, needle))
    }

    /// Evaluate one admitted REPLACE call after the caller has already
    /// validated all text operands.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_replace_text(
        self,
        text: &str,
        from: &str,
        to: &str,
    ) -> Value {
        debug_assert!(matches!(self, Self::Replace));

        Value::Text(text.replace(from, to))
    }

    /// Evaluate one admitted SUBSTRING call after the caller has already
    /// validated the text and integer operands.
    #[must_use]
    pub(in crate::db::query::plan::expr) fn eval_substring_text(
        self,
        text: &str,
        start: i64,
        length: Option<i64>,
    ) -> Value {
        debug_assert!(matches!(self, Self::Substring));

        Value::Text(Self::substring_1_based(text, start, length))
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
