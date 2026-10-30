//! Bounded source-level constraint expressions.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    Account, Blob, Date, Decimal, Duration, FieldSourceKey, Float32, Float64,
    MAX_PROPOSAL_LITERAL_BYTES, MAX_SOURCE_CHECK_INSTRUCTIONS, Principal, ScalarKind,
    SchemaContractError, Subaccount, Timestamp, TypeSourceKey, U256, Ulid, Unit,
};

/// One canonical scalar literal carried by a schema proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ScalarLiteral {
    /// Account identifier.
    Account(Account),
    /// Bounded binary value.
    Blob(#[serde(deserialize_with = "crate::codec::deserialize_literal_blob")] Blob),
    /// Boolean value.
    Bool(bool),
    /// Days since the Unix epoch.
    Date(Date),
    /// Canonical fixed-point decimal.
    Decimal(#[serde(deserialize_with = "crate::codec::deserialize_literal_decimal")] Decimal),
    /// Millisecond duration.
    Duration(Duration),
    /// Source-keyed unit-enum variant.
    EnumUnit {
        /// Immutable enum type identity.
        #[serde(
            deserialize_with = "crate::codec::deserialize_key::<_, _, { crate::MAX_SOURCE_KEY_BYTES }>"
        )]
        enum_type: TypeSourceKey,
        /// Immutable unit-variant identity.
        #[serde(
            deserialize_with = "crate::codec::deserialize_key::<_, _, { crate::MAX_SOURCE_KEY_BYTES }>"
        )]
        variant: TypeSourceKey,
    },
    /// Finite 32-bit float.
    Float32(Float32),
    /// Finite 64-bit float.
    Float64(Float64),
    /// Signed fixed-width integer.
    Int(i128),
    /// Unsigned fixed-width integer.
    Nat(u128),
    /// Fixed-width unsigned 256-bit integer.
    U256(U256),
    /// Principal value.
    Principal(Principal),
    /// Fixed-width subaccount.
    Subaccount(Subaccount),
    /// Bounded text value.
    Text(#[serde(deserialize_with = "crate::codec::deserialize_literal_text")] String),
    /// Unix-millisecond timestamp.
    Timestamp(Timestamp),
    /// Canonical ULID.
    Ulid(Ulid),
    /// Explicit unit value.
    Unit(Unit),
}

impl ScalarLiteral {
    /// Return the declared scalar kind represented by this literal.
    #[must_use]
    pub const fn kind(&self) -> ScalarKind {
        match self {
            Self::Account(_) => ScalarKind::Account,
            Self::Blob(_) => ScalarKind::Blob,
            Self::Bool(_) => ScalarKind::Bool,
            Self::Date(_) => ScalarKind::Date,
            Self::Decimal(_) => ScalarKind::Decimal,
            Self::Duration(_) => ScalarKind::Duration,
            Self::EnumUnit { .. } => ScalarKind::Enum,
            Self::Float32(_) => ScalarKind::Float32,
            Self::Float64(_) => ScalarKind::Float64,
            Self::Int(_) => ScalarKind::Int128,
            Self::Nat(_) => ScalarKind::Nat128,
            Self::U256(_) => ScalarKind::U256,
            Self::Principal(_) => ScalarKind::Principal,
            Self::Subaccount(_) => ScalarKind::Subaccount,
            Self::Text(_) => ScalarKind::Text,
            Self::Timestamp(_) => ScalarKind::Timestamp,
            Self::Ulid(_) => ScalarKind::Ulid,
            Self::Unit(_) => ScalarKind::Unit,
        }
    }

    pub(crate) const fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::Decimal(value) if value.scale() > crate::decimal::MAX_SUPPORTED_SCALE => {
                Err(SchemaContractError::InvalidLiteral)
            }
            Self::Blob(value) if value.len() > MAX_PROPOSAL_LITERAL_BYTES => {
                Err(SchemaContractError::InvalidLiteral)
            }
            Self::Text(value) if value.len() > MAX_PROPOSAL_LITERAL_BYTES => {
                Err(SchemaContractError::InvalidLiteral)
            }
            _ => Ok(()),
        }
    }
}

/// One instruction in a bounded source-level postfix check expression.
///
/// This is an AST transport, not accepted bytecode: field references remain
/// typed current-name keys and IcyDB still owns accepted binding and compilation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceCheckInstruction {
    /// Push one field value.
    Field(
        #[serde(
            deserialize_with = "crate::codec::deserialize_key::<_, _, { crate::MAX_SOURCE_KEY_BYTES }>"
        )]
        FieldSourceKey,
    ),
    /// Push one admitted proposal literal.
    Literal(ScalarLiteral),
    /// SQL equality.
    Equal,
    /// SQL inequality.
    NotEqual,
    /// SQL less-than.
    LessThan,
    /// SQL less-than-or-equal.
    LessThanOrEqual,
    /// SQL greater-than.
    GreaterThan,
    /// SQL greater-than-or-equal.
    GreaterThanOrEqual,
    /// SQL three-valued conjunction.
    And,
    /// SQL three-valued disjunction.
    Or,
    /// SQL three-valued negation.
    Not,
    /// Two-valued null test.
    IsNull,
    /// Two-valued non-null test.
    IsNotNull,
    /// Bounded scalar/collection length.
    Length,
}

/// Bounded canonical source expression.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceCheckExpr {
    #[serde(
        deserialize_with = "crate::codec::deserialize_list::<_, _, { crate::MAX_SOURCE_CHECK_INSTRUCTIONS }>"
    )]
    instructions: Vec<SourceCheckInstruction>,
}

impl SourceCheckExpr {
    /// Construct and validate one source expression.
    ///
    /// # Errors
    ///
    /// Returns a typed expression error for empty, oversized, malformed-stack,
    /// or invalid-literal input.
    pub fn try_new(instructions: Vec<SourceCheckInstruction>) -> Result<Self, SchemaContractError> {
        let expression = Self { instructions };
        expression.validate()?;
        Ok(expression)
    }

    /// Borrow canonical postfix instructions.
    #[must_use]
    pub fn instructions(&self) -> &[SourceCheckInstruction] {
        &self.instructions
    }

    /// Derive referenced field source keys from the expression.
    #[must_use]
    pub fn dependencies(&self) -> BTreeSet<FieldSourceKey> {
        self.instructions
            .iter()
            .filter_map(|instruction| match instruction {
                SourceCheckInstruction::Field(field) => Some(field.clone()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        if self.instructions.is_empty() || self.instructions.len() > MAX_SOURCE_CHECK_INSTRUCTIONS {
            return Err(SchemaContractError::InvalidExpression);
        }
        let mut stack_depth = 0usize;
        for instruction in &self.instructions {
            match instruction {
                SourceCheckInstruction::Field(_) => {
                    stack_depth = stack_depth
                        .checked_add(1)
                        .ok_or(SchemaContractError::InvalidExpression)?;
                }
                SourceCheckInstruction::Literal(literal) => {
                    literal.validate()?;
                    stack_depth = stack_depth
                        .checked_add(1)
                        .ok_or(SchemaContractError::InvalidExpression)?;
                }
                SourceCheckInstruction::Not
                | SourceCheckInstruction::IsNull
                | SourceCheckInstruction::IsNotNull
                | SourceCheckInstruction::Length => {
                    if stack_depth < 1 {
                        return Err(SchemaContractError::InvalidExpression);
                    }
                }
                SourceCheckInstruction::Equal
                | SourceCheckInstruction::NotEqual
                | SourceCheckInstruction::LessThan
                | SourceCheckInstruction::LessThanOrEqual
                | SourceCheckInstruction::GreaterThan
                | SourceCheckInstruction::GreaterThanOrEqual
                | SourceCheckInstruction::And
                | SourceCheckInstruction::Or => {
                    if stack_depth < 2 {
                        return Err(SchemaContractError::InvalidExpression);
                    }
                    stack_depth -= 1;
                }
            }
        }
        if stack_depth != 1 {
            return Err(SchemaContractError::InvalidExpression);
        }
        Ok(())
    }
}
