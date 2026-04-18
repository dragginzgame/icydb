use crate::{types::Decimal, value::Value};
use std::str::FromStr;
use thiserror::Error as ThisError;

#[cfg_attr(
    doc,
    doc = "SqlParseError\n\nReduced SQL parser errors shared by standalone predicate parsing and the statement-level SQL frontend."
)]
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum SqlParseError {
    #[cfg(feature = "sql")]
    #[error("empty SQL input")]
    EmptyInput,

    #[error("unsupported SQL feature: {feature}")]
    UnsupportedFeature { feature: &'static str },

    #[error("invalid SQL syntax: {message}")]
    InvalidSyntax { message: String },
}

impl SqlParseError {
    pub(in crate::db) const fn unsupported_feature(feature: &'static str) -> Self {
        Self::UnsupportedFeature { feature }
    }

    pub(in crate::db) fn invalid_syntax(message: impl Into<String>) -> Self {
        Self::InvalidSyntax {
            message: message.into(),
        }
    }

    pub(crate) fn expected(expected: &str, found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected {expected}, found {found}"))
    }

    pub(in crate::db) fn expected_end_of_input(found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected end of input, found {found}"))
    }

    pub(in crate::db) fn invalid_numeric_literal(raw: &str) -> Self {
        Self::invalid_syntax(format!("invalid numeric literal: {raw}"))
    }
}

#[cfg_attr(
    doc,
    doc = "Keyword\n\nReduced SQL keyword taxonomy shared by predicate parsing and statement parsing."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Keyword {
    And,
    As,
    Asc,
    Avg,
    Between,
    By,
    Case,
    Columns,
    Count,
    Delete,
    Describe,
    Desc,
    Distinct,
    Else,
    End,
    Except,
    Execution,
    Explain,
    Entities,
    False,
    Filter,
    From,
    Group,
    Having,
    In,
    Indexes,
    Insert,
    Intersect,
    Is,
    Join,
    Json,
    Limit,
    Max,
    Min,
    Not,
    Null,
    Offset,
    Or,
    Order,
    Over,
    Returning,
    Select,
    Show,
    Sum,
    Tables,
    Then,
    True,
    Union,
    Update,
    Where,
    When,
    With,
}

impl Keyword {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::Avg => "AVG",
            Self::Between => "BETWEEN",
            Self::By => "BY",
            Self::Case => "CASE",
            Self::Columns => "COLUMNS",
            Self::Count => "COUNT",
            Self::Delete => "DELETE",
            Self::Describe => "DESCRIBE",
            Self::Desc => "DESC",
            Self::Distinct => "DISTINCT",
            Self::Else => "ELSE",
            Self::End => "END",
            Self::Except => "EXCEPT",
            Self::Execution => "EXECUTION",
            Self::Explain => "EXPLAIN",
            Self::Entities => "ENTITIES",
            Self::False => "FALSE",
            Self::Filter => "FILTER",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::In => "IN",
            Self::Indexes => "INDEXES",
            Self::Insert => "INSERT",
            Self::Intersect => "INTERSECT",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Json => "JSON",
            Self::Limit => "LIMIT",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Offset => "OFFSET",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Over => "OVER",
            Self::Returning => "RETURNING",
            Self::Select => "SELECT",
            Self::Show => "SHOW",
            Self::Sum => "SUM",
            Self::Tables => "TABLES",
            Self::Then => "THEN",
            Self::True => "TRUE",
            Self::Union => "UNION",
            Self::Update => "UPDATE",
            Self::Where => "WHERE",
            Self::When => "WHEN",
            Self::With => "WITH",
        }
    }
}

// One reduced SQL token kind shared by predicate and statement parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Identifier(String),
    Number(String),
    StringLiteral(String),
    Keyword(Keyword),
    Comma,
    Dot,
    Plus,
    Minus,
    Slash,
    LParen,
    RParen,
    Semicolon,
    Star,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
}

pub(crate) fn token_kind_label(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Identifier(name) => format!("identifier({name})"),
        TokenKind::Number(number) => format!("number({number})"),
        TokenKind::StringLiteral(_) => "string literal".to_string(),
        TokenKind::Keyword(keyword) => keyword.as_str().to_string(),
        TokenKind::Comma => ",".to_string(),
        TokenKind::Dot => ".".to_string(),
        TokenKind::Plus => "+".to_string(),
        TokenKind::Minus => "-".to_string(),
        TokenKind::Slash => "/".to_string(),
        TokenKind::LParen => "(".to_string(),
        TokenKind::RParen => ")".to_string(),
        TokenKind::Semicolon => ";".to_string(),
        TokenKind::Star => "*".to_string(),
        TokenKind::Eq => "=".to_string(),
        TokenKind::Ne => "!=".to_string(),
        TokenKind::Lt => "<".to_string(),
        TokenKind::Lte => "<=".to_string(),
        TokenKind::Gt => ">".to_string(),
        TokenKind::Gte => ">=".to_string(),
    }
}

pub(in crate::db::sql_shared) fn parse_number_literal(raw: &str) -> Result<Value, SqlParseError> {
    if raw.contains('.') {
        let decimal =
            Decimal::from_str(raw).map_err(|_| SqlParseError::invalid_numeric_literal(raw))?;
        return Ok(Value::Decimal(decimal));
    }

    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Value::Int(value));
    }
    if let Ok(value) = raw.parse::<u64>() {
        return Ok(Value::Uint(value));
    }
    if let Ok(value) = Decimal::from_str(raw) {
        return Ok(Value::Decimal(value));
    }

    Err(SqlParseError::invalid_numeric_literal(raw))
}
