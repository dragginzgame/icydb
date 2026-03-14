//! Module: db::sql::parser
//! Responsibility: reduced SQL lexer/parser for deterministic frontend normalization.
//! Does not own: schema validation, planner policy, or execution semantics.
//! Boundary: parses one SQL statement into frontend-neutral statement contracts.

#[cfg(test)]
mod tests;

use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    types::Decimal,
    value::Value,
};
use std::str::FromStr;
use thiserror::Error as ThisError;

///
/// SqlStatement
///
/// Reduced SQL statement contract accepted by the `0.52` parser baseline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlStatement {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
    Explain(SqlExplainStatement),
}

///
/// SqlProjection
///
/// Projection shape parsed from one `SELECT` statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlProjection {
    All,
    Items(Vec<SqlSelectItem>),
}

///
/// SqlSelectItem
///
/// One projection item parsed from one `SELECT` list.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlSelectItem {
    Field(String),
    Aggregate(SqlAggregateCall),
}

///
/// SqlAggregateKind
///
/// Aggregate operator taxonomy accepted by the reduced parser.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlAggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// SqlAggregateCall
///
/// Parsed aggregate call projection item.
/// `field = None` is only valid for `COUNT(*)`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlAggregateCall {
    pub(crate) kind: SqlAggregateKind,
    pub(crate) field: Option<String>,
}

///
/// SqlOrderDirection
///
/// Parsed order direction for one `ORDER BY` item.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlOrderDirection {
    Asc,
    Desc,
}

///
/// SqlOrderTerm
///
/// Parsed `ORDER BY` field/direction pair.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlOrderTerm {
    pub(crate) field: String,
    pub(crate) direction: SqlOrderDirection,
}

///
/// SqlSelectStatement
///
/// Canonical parsed `SELECT` statement shape for reduced SQL.
///
/// This contract is frontend-only and intentionally schema-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlSelectStatement {
    pub(crate) entity: String,
    pub(crate) projection: SqlProjection,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) distinct: bool,
    pub(crate) group_by: Vec<String>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
}

///
/// SqlDeleteStatement
///
/// Canonical parsed `DELETE` statement shape for reduced SQL.
///
/// This contract keeps delete-mode clause policy explicit.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlDeleteStatement {
    pub(crate) entity: String,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
}

///
/// SqlExplainMode
///
/// Reduced EXPLAIN render mode selector.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainMode {
    Plan,
    Execution,
    Json,
}

///
/// SqlExplainTarget
///
/// Statement forms accepted behind one `EXPLAIN` prefix.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainTarget {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
}

///
/// SqlExplainStatement
///
/// Canonical parsed `EXPLAIN` statement.
///
/// Explain remains a wrapper over one executable reduced SQL statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlExplainStatement {
    pub(crate) mode: SqlExplainMode,
    pub(crate) statement: SqlExplainTarget,
}

///
/// SqlParseError
///
/// Reduced SQL parser errors for syntax and subset-policy failures.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum SqlParseError {
    #[error("empty SQL input")]
    EmptyInput,

    #[error("unsupported SQL feature: {feature}")]
    UnsupportedFeature { feature: &'static str },

    #[error("invalid SQL syntax: {message}")]
    InvalidSyntax { message: String },
}

impl SqlParseError {
    const fn unsupported_feature(feature: &'static str) -> Self {
        Self::UnsupportedFeature { feature }
    }

    fn invalid_syntax(message: impl Into<String>) -> Self {
        Self::InvalidSyntax {
            message: message.into(),
        }
    }

    fn expected(expected: &str, found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected {expected}, found {found}"))
    }

    fn expected_end_of_input(found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected end of input, found {found}"))
    }

    fn invalid_numeric_literal(raw: &str) -> Self {
        Self::invalid_syntax(format!("invalid numeric literal: {raw}"))
    }
}

/// Parse one reduced SQL statement.
///
/// Parsing is deterministic and normalization-insensitive for keyword casing,
/// insignificant whitespace, and optional one-statement terminator (`;`).
pub(crate) fn parse_sql(sql: &str) -> Result<SqlStatement, SqlParseError> {
    let tokens = Lexer::tokenize(sql)?;
    if tokens.is_empty() {
        return Err(SqlParseError::EmptyInput);
    }

    let mut parser = Parser::new(tokens);
    let statement = parser.parse_statement()?;

    if parser.eat_semicolon() && !parser.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            "multi-statement SQL input",
        ));
    }

    if !parser.is_eof() {
        if let Some(err) = parser.trailing_clause_order_error(&statement) {
            return Err(err);
        }

        if let Some(feature) = parser.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        return Err(SqlParseError::expected_end_of_input(parser.peek_kind()));
    }

    Ok(statement)
}

/// Parse one SQL predicate expression.
///
/// This is used by schema-level contracts (for example conditional index metadata)
/// that need predicate parsing without a full SQL statement wrapper.
pub(crate) fn parse_sql_predicate(sql: &str) -> Result<Predicate, SqlParseError> {
    let tokens = Lexer::tokenize(sql)?;
    if tokens.is_empty() {
        return Err(SqlParseError::EmptyInput);
    }

    let mut parser = Parser::new(tokens);
    let predicate = parser.parse_predicate()?;

    if parser.eat_semicolon() && !parser.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            "multi-statement SQL input",
        ));
    }

    if !parser.is_eof() {
        if let Some(feature) = parser.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        return Err(SqlParseError::expected_end_of_input(parser.peek_kind()));
    }

    Ok(predicate)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Keyword {
    And,
    As,
    Asc,
    Avg,
    Between,
    By,
    Count,
    Delete,
    Desc,
    Distinct,
    Except,
    Execution,
    Explain,
    False,
    From,
    Group,
    Having,
    In,
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
    Select,
    Sum,
    True,
    Union,
    Update,
    Where,
    With,
}

impl Keyword {
    const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::Avg => "AVG",
            Self::Between => "BETWEEN",
            Self::By => "BY",
            Self::Count => "COUNT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Distinct => "DISTINCT",
            Self::Except => "EXCEPT",
            Self::Execution => "EXECUTION",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::In => "IN",
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
            Self::Select => "SELECT",
            Self::Sum => "SUM",
            Self::True => "TRUE",
            Self::Union => "UNION",
            Self::Update => "UPDATE",
            Self::Where => "WHERE",
            Self::With => "WITH",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TokenKind {
    Identifier(String),
    Number(String),
    StringLiteral(String),
    Keyword(Keyword),
    Comma,
    Dot,
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
struct Token {
    kind: TokenKind,
}

fn token_kind_label(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Identifier(name) => format!("identifier({name})"),
        TokenKind::Number(number) => format!("number({number})"),
        TokenKind::StringLiteral(_) => "string literal".to_string(),
        TokenKind::Keyword(keyword) => keyword.as_str().to_string(),
        TokenKind::Comma => ",".to_string(),
        TokenKind::Dot => ".".to_string(),
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

struct Lexer<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn tokenize(sql: &'a str) -> Result<Vec<Token>, SqlParseError> {
        let mut lexer = Self {
            bytes: sql.as_bytes(),
            pos: 0,
        };
        let mut tokens = Vec::new();

        while let Some(token) = lexer.next_token()? {
            tokens.push(token);
        }

        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Option<Token>, SqlParseError> {
        self.skip_whitespace();
        let Some(byte) = self.peek_byte() else {
            return Ok(None);
        };

        let token_kind = match byte {
            b',' => {
                self.bump();
                TokenKind::Comma
            }
            b'.' => {
                self.bump();
                TokenKind::Dot
            }
            b'(' => {
                self.bump();
                TokenKind::LParen
            }
            b')' => {
                self.bump();
                TokenKind::RParen
            }
            b';' => {
                self.bump();
                TokenKind::Semicolon
            }
            b'*' => {
                self.bump();
                TokenKind::Star
            }
            b'=' => {
                self.bump();
                TokenKind::Eq
            }
            b'!' => {
                self.bump();
                if self.eat_byte(b'=') {
                    TokenKind::Ne
                } else {
                    return Err(SqlParseError::invalid_syntax("unexpected '!'"));
                }
            }
            b'<' => {
                self.bump();
                if self.eat_byte(b'=') {
                    TokenKind::Lte
                } else if self.eat_byte(b'>') {
                    TokenKind::Ne
                } else {
                    TokenKind::Lt
                }
            }
            b'>' => {
                self.bump();
                if self.eat_byte(b'=') {
                    TokenKind::Gte
                } else {
                    TokenKind::Gt
                }
            }
            b'\'' => TokenKind::StringLiteral(self.lex_string_literal()?),
            b'"' | b'`' => return Err(SqlParseError::unsupported_feature("quoted identifiers")),
            b'-' => {
                if self
                    .peek_next_byte()
                    .is_some_and(|next| next.is_ascii_digit())
                {
                    TokenKind::Number(self.lex_number(true))
                } else {
                    return Err(SqlParseError::invalid_syntax("unexpected '-'"));
                }
            }
            byte if byte.is_ascii_digit() => TokenKind::Number(self.lex_number(false)),
            byte if is_identifier_start(byte) => self.lex_identifier_or_keyword(),
            _ => {
                return Err(SqlParseError::invalid_syntax(format!(
                    "unexpected character '{}'",
                    byte as char
                )));
            }
        };

        Ok(Some(Token { kind: token_kind }))
    }

    fn skip_whitespace(&mut self) {
        while self
            .peek_byte()
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            self.bump();
        }
    }

    fn lex_string_literal(&mut self) -> Result<String, SqlParseError> {
        self.expect_byte(b'\'')?;

        let mut out = String::new();
        while let Some(byte) = self.peek_byte() {
            self.bump();

            if byte == b'\'' {
                if self.eat_byte(b'\'') {
                    out.push('\'');
                    continue;
                }
                return Ok(out);
            }

            out.push(byte as char);
        }

        Err(SqlParseError::invalid_syntax("unterminated string literal"))
    }

    fn lex_number(&mut self, has_sign: bool) -> String {
        let mut out = String::new();
        if has_sign {
            out.push('-');
            self.bump();
        }

        while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
            out.push(self.bump().unwrap_or_default() as char);
        }

        if self.peek_byte() == Some(b'.') {
            out.push('.');
            self.bump();

            while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
                out.push(self.bump().unwrap_or_default() as char);
            }
        }

        out
    }

    fn lex_identifier_or_keyword(&mut self) -> TokenKind {
        let mut out = String::new();
        while self.peek_byte().is_some_and(is_identifier_continue) {
            out.push(self.bump().unwrap_or_default() as char);
        }

        match keyword_from_ident(out.as_str()) {
            Some(keyword) => TokenKind::Keyword(keyword),
            None => TokenKind::Identifier(out),
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_next_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos + 1).copied()
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), SqlParseError> {
        match self.bump() {
            Some(found) if found == expected => Ok(()),
            Some(found) => Err(SqlParseError::invalid_syntax(format!(
                "expected '{}', found '{}'",
                expected as char, found as char
            ))),
            None => Err(SqlParseError::invalid_syntax(format!(
                "expected '{}', found end of input",
                expected as char
            ))),
        }
    }

    fn eat_byte(&mut self, expected: u8) -> bool {
        if self.peek_byte() == Some(expected) {
            self.bump();
            return true;
        }

        false
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek_byte()?;
        self.pos += 1;
        Some(byte)
    }
}

const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn keyword_from_ident(value: &str) -> Option<Keyword> {
    let upper = value.to_ascii_uppercase();
    match upper.as_str() {
        "AND" => Some(Keyword::And),
        "AS" => Some(Keyword::As),
        "ASC" => Some(Keyword::Asc),
        "AVG" => Some(Keyword::Avg),
        "BETWEEN" => Some(Keyword::Between),
        "BY" => Some(Keyword::By),
        "COUNT" => Some(Keyword::Count),
        "DELETE" => Some(Keyword::Delete),
        "DESC" => Some(Keyword::Desc),
        "DISTINCT" => Some(Keyword::Distinct),
        "EXCEPT" => Some(Keyword::Except),
        "EXECUTION" => Some(Keyword::Execution),
        "EXPLAIN" => Some(Keyword::Explain),
        "FALSE" => Some(Keyword::False),
        "FROM" => Some(Keyword::From),
        "GROUP" => Some(Keyword::Group),
        "HAVING" => Some(Keyword::Having),
        "IN" => Some(Keyword::In),
        "INSERT" => Some(Keyword::Insert),
        "INTERSECT" => Some(Keyword::Intersect),
        "IS" => Some(Keyword::Is),
        "JOIN" => Some(Keyword::Join),
        "JSON" => Some(Keyword::Json),
        "LIMIT" => Some(Keyword::Limit),
        "MAX" => Some(Keyword::Max),
        "MIN" => Some(Keyword::Min),
        "NOT" => Some(Keyword::Not),
        "NULL" => Some(Keyword::Null),
        "OFFSET" => Some(Keyword::Offset),
        "OR" => Some(Keyword::Or),
        "ORDER" => Some(Keyword::Order),
        "SELECT" => Some(Keyword::Select),
        "SUM" => Some(Keyword::Sum),
        "TRUE" => Some(Keyword::True),
        "UNION" => Some(Keyword::Union),
        "UPDATE" => Some(Keyword::Update),
        "WHERE" => Some(Keyword::Where),
        "WITH" => Some(Keyword::With),
        _ => None,
    }
}

// Parser state over one pre-tokenized SQL statement.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Select) {
            return Ok(SqlStatement::Select(self.parse_select_statement()?));
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(SqlStatement::Delete(self.parse_delete_statement()?));
        }
        if self.eat_keyword(Keyword::Explain) {
            return Ok(SqlStatement::Explain(self.parse_explain_statement()?));
        }

        if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        Err(SqlParseError::expected(
            "one of SELECT, DELETE, EXPLAIN",
            self.peek_kind(),
        ))
    }

    fn parse_explain_statement(&mut self) -> Result<SqlExplainStatement, SqlParseError> {
        let mode = if self.eat_keyword(Keyword::Execution) {
            SqlExplainMode::Execution
        } else if self.eat_keyword(Keyword::Json) {
            SqlExplainMode::Json
        } else {
            SqlExplainMode::Plan
        };

        let statement = if self.eat_keyword(Keyword::Select) {
            SqlExplainTarget::Select(self.parse_select_statement()?)
        } else if self.eat_keyword(Keyword::Delete) {
            SqlExplainTarget::Delete(self.parse_delete_statement()?)
        } else if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        } else {
            return Err(SqlParseError::expected(
                "one of SELECT, DELETE",
                self.peek_kind(),
            ));
        };

        Ok(SqlExplainStatement { mode, statement })
    }

    // Classify one trailing token as a likely out-of-order clause mistake so
    // callers get an actionable parser diagnostic instead of generic EOI.
    fn trailing_clause_order_error(&self, statement: &SqlStatement) -> Option<SqlParseError> {
        match statement {
            SqlStatement::Select(select) => self.select_clause_order_error(select),
            SqlStatement::Delete(delete) => self.delete_clause_order_error(delete),
            SqlStatement::Explain(explain) => match &explain.statement {
                SqlExplainTarget::Select(select) => self.select_clause_order_error(select),
                SqlExplainTarget::Delete(delete) => self.delete_clause_order_error(delete),
            },
        }
    }

    fn select_clause_order_error(&self, statement: &SqlSelectStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET; \
                 try: SELECT ... ORDER BY <field> [ASC|DESC] LIMIT <n> [OFFSET <n>]",
            ));
        }

        None
    }

    fn delete_clause_order_error(&self, statement: &SqlDeleteStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order) && statement.limit.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT in DELETE statements; \
                 try: DELETE ... ORDER BY <field> [ASC|DESC] LIMIT <n>",
            ));
        }

        None
    }

    fn parse_select_statement(&mut self) -> Result<SqlSelectStatement, SqlParseError> {
        let distinct = self.eat_keyword(Keyword::Distinct);
        let projection = self.parse_projection()?;
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        self.reject_table_alias_if_present()?;

        // Phase 1: parse predicate and grouping clauses in canonical sequence.
        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let group_by = if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };

        if self.eat_keyword(Keyword::Having) {
            return Err(SqlParseError::unsupported_feature("HAVING"));
        }

        // Phase 2: parse ordering and window clauses.
        let order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };

        Ok(SqlSelectStatement {
            entity,
            projection,
            predicate,
            distinct,
            group_by,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        self.reject_table_alias_if_present()?;

        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        if self.eat_keyword(Keyword::Offset) {
            return Err(SqlParseError::unsupported_feature("DELETE ... OFFSET"));
        }

        Ok(SqlDeleteStatement {
            entity,
            predicate,
            order_by,
            limit,
        })
    }

    fn parse_projection(&mut self) -> Result<SqlProjection, SqlParseError> {
        if self.eat_star() {
            return Ok(SqlProjection::All);
        }

        let mut items = Vec::new();
        loop {
            items.push(self.parse_select_item()?);

            if self.eat_keyword(Keyword::As) {
                return Err(SqlParseError::unsupported_feature(
                    "column/expression aliases",
                ));
            }
            if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
                return Err(SqlParseError::unsupported_feature(
                    "column/expression aliases",
                ));
            }

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if items.is_empty() {
            return Err(SqlParseError::expected(
                "one projection item",
                self.peek_kind(),
            ));
        }

        Ok(SqlProjection::Items(items))
    }

    fn parse_select_item(&mut self) -> Result<SqlSelectItem, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlSelectItem::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate forms",
            ));
        }

        Ok(SqlSelectItem::Field(field))
    }

    fn parse_aggregate_kind(&self) -> Option<SqlAggregateKind> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => Some(SqlAggregateKind::Count),
            Some(TokenKind::Keyword(Keyword::Sum)) => Some(SqlAggregateKind::Sum),
            Some(TokenKind::Keyword(Keyword::Avg)) => Some(SqlAggregateKind::Avg),
            Some(TokenKind::Keyword(Keyword::Min)) => Some(SqlAggregateKind::Min),
            Some(TokenKind::Keyword(Keyword::Max)) => Some(SqlAggregateKind::Max),
            _ => None,
        }
    }

    fn parse_aggregate_call(
        &mut self,
        kind: SqlAggregateKind,
    ) -> Result<SqlAggregateCall, SqlParseError> {
        self.bump();
        self.expect_lparen()?;

        if self.eat_keyword(Keyword::Distinct) {
            return Err(SqlParseError::unsupported_feature(
                "DISTINCT aggregate qualifiers",
            ));
        }

        let field = if kind == SqlAggregateKind::Count && self.eat_star() {
            None
        } else {
            Some(self.expect_identifier()?)
        };

        self.expect_rparen()?;

        Ok(SqlAggregateCall { kind, field })
    }

    fn parse_order_terms(&mut self) -> Result<Vec<SqlOrderTerm>, SqlParseError> {
        let mut terms = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            let direction = if self.eat_keyword(Keyword::Desc) {
                SqlOrderDirection::Desc
            } else {
                self.eat_keyword(Keyword::Asc);
                SqlOrderDirection::Asc
            };

            terms.push(SqlOrderTerm { field, direction });
            if !self.eat_comma() {
                break;
            }
        }

        Ok(terms)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, SqlParseError> {
        let mut fields = vec![self.expect_identifier()?];
        while self.eat_comma() {
            fields.push(self.expect_identifier()?);
        }

        Ok(fields)
    }

    // Keep reduced-parser table ownership explicit: aliases are intentionally
    // unsupported in this baseline and must fail closed.
    fn reject_table_alias_if_present(&self) -> Result<(), SqlParseError> {
        if self.peek_keyword(Keyword::As)
            || matches!(self.peek_kind(), Some(TokenKind::Identifier(_)))
        {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        Ok(())
    }

    fn parse_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        self.parse_or_predicate()
    }

    fn parse_or_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        let mut left = self.parse_and_predicate()?;
        while self.eat_keyword(Keyword::Or) {
            let right = self.parse_and_predicate()?;
            left = Predicate::Or(vec![left, right]);
        }

        Ok(left)
    }

    fn parse_and_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        let mut left = self.parse_not_predicate()?;
        while self.eat_keyword(Keyword::And) {
            let right = self.parse_not_predicate()?;
            left = Predicate::And(vec![left, right]);
        }

        Ok(left)
    }

    fn parse_not_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        if self.eat_keyword(Keyword::Not) {
            return Ok(Predicate::Not(Box::new(self.parse_not_predicate()?)));
        }

        self.parse_predicate_primary()
    }

    fn parse_predicate_primary(&mut self) -> Result<Predicate, SqlParseError> {
        if self.eat_lparen() {
            let predicate = self.parse_predicate()?;
            self.expect_rparen()?;

            return Ok(predicate);
        }

        self.parse_field_predicate()
    }

    fn parse_field_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        let field = self.expect_identifier()?;

        if self.eat_keyword(Keyword::Is) {
            let is_not = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;

            return Ok(if is_not {
                Predicate::IsNotNull { field }
            } else {
                Predicate::IsNull { field }
            });
        }

        if self.eat_keyword(Keyword::Not) {
            if self.eat_keyword(Keyword::In) {
                return self.parse_in_predicate(field, true);
            }

            return Err(SqlParseError::expected("IN after NOT", self.peek_kind()));
        }

        if self.eat_keyword(Keyword::In) {
            return self.parse_in_predicate(field, false);
        }

        if self.eat_keyword(Keyword::Between) {
            return self.parse_between_predicate(field);
        }

        let op = self.parse_compare_operator()?;
        let value = self.parse_literal()?;

        Ok(predicate_compare(field, op, value))
    }

    fn parse_in_predicate(
        &mut self,
        field: String,
        negated: bool,
    ) -> Result<Predicate, SqlParseError> {
        self.expect_lparen()?;

        let mut values = Vec::new();
        loop {
            values.push(self.parse_literal()?);
            if !self.eat_comma() {
                break;
            }
        }
        self.expect_rparen()?;

        let op = if negated {
            CompareOp::NotIn
        } else {
            CompareOp::In
        };

        Ok(Predicate::Compare(ComparePredicate::with_coercion(
            field,
            op,
            Value::List(values),
            CoercionId::Strict,
        )))
    }

    fn parse_between_predicate(&mut self, field: String) -> Result<Predicate, SqlParseError> {
        let lower = self.parse_literal()?;
        self.expect_keyword(Keyword::And)?;
        let upper = self.parse_literal()?;

        Ok(Predicate::And(vec![
            predicate_compare(field.clone(), CompareOp::Gte, lower),
            predicate_compare(field, CompareOp::Lte, upper),
        ]))
    }

    fn parse_compare_operator(&mut self) -> Result<CompareOp, SqlParseError> {
        let op = match self.peek_kind() {
            Some(TokenKind::Eq) => CompareOp::Eq,
            Some(TokenKind::Ne) => CompareOp::Ne,
            Some(TokenKind::Lt) => CompareOp::Lt,
            Some(TokenKind::Lte) => CompareOp::Lte,
            Some(TokenKind::Gt) => CompareOp::Gt,
            Some(TokenKind::Gte) => CompareOp::Gte,
            _ => {
                return Err(SqlParseError::expected(
                    "one of =, !=, <, <=, >, >=",
                    self.peek_kind(),
                ));
            }
        };

        self.bump();
        Ok(op)
    }

    fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        match self.bump() {
            Some(TokenKind::StringLiteral(value)) => Ok(Value::Text(value)),
            Some(TokenKind::Number(value)) => parse_number_literal(value.as_str()),
            Some(TokenKind::Keyword(Keyword::Null)) => Ok(Value::Null),
            Some(TokenKind::Keyword(Keyword::True)) => Ok(Value::Bool(true)),
            Some(TokenKind::Keyword(Keyword::False)) => Ok(Value::Bool(false)),
            _ => Err(SqlParseError::expected("literal", self.peek_kind())),
        }
    }

    fn parse_u32_literal(&mut self, clause: &str) -> Result<u32, SqlParseError> {
        let token = self.bump();
        let Some(TokenKind::Number(value)) = token else {
            return Err(SqlParseError::expected(
                &format!("integer literal after {clause}"),
                self.peek_kind(),
            ));
        };

        if value.contains('.') || value.starts_with('-') {
            return Err(SqlParseError::invalid_syntax(format!(
                "{clause} requires a non-negative integer literal"
            )));
        }

        value.parse::<u32>().map_err(|_| {
            SqlParseError::invalid_syntax(format!("{clause} value exceeds supported u32 bound"))
        })
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), SqlParseError> {
        if self.eat_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(keyword.as_str(), self.peek_kind()))
    }

    fn expect_identifier(&mut self) -> Result<String, SqlParseError> {
        let Some(TokenKind::Identifier(mut name)) = self.bump() else {
            return Err(SqlParseError::expected("identifier", self.peek_kind()));
        };

        // Support dotted names (`schema.table`, `table.field`) without enabling
        // quoted identifiers or arbitrary expression parsing.
        while self.eat_dot() {
            let Some(TokenKind::Identifier(part)) = self.bump() else {
                return Err(SqlParseError::expected(
                    "identifier after '.'",
                    self.peek_kind(),
                ));
            };
            name.push('.');
            name.push_str(part.as_str());
        }

        Ok(name)
    }

    fn expect_lparen(&mut self) -> Result<(), SqlParseError> {
        if self.eat_lparen() {
            return Ok(());
        }

        Err(SqlParseError::expected("(", self.peek_kind()))
    }

    fn expect_rparen(&mut self) -> Result<(), SqlParseError> {
        if self.eat_rparen() {
            return Ok(());
        }

        Err(SqlParseError::expected(")", self.peek_kind()))
    }

    fn eat_keyword(&mut self, keyword: Keyword) -> bool {
        if !self.peek_keyword(keyword) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    fn eat_comma(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Comma)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn eat_dot(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Dot)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn eat_lparen(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::LParen)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn eat_rparen(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::RParen)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn eat_semicolon(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Semicolon)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn eat_star(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Star)) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn peek_lparen(&self) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::LParen))
    }

    fn peek_unsupported_feature(&self) -> Option<&'static str> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::As)) => Some("column/expression aliases"),
            Some(TokenKind::Keyword(Keyword::Having)) => Some("HAVING"),
            Some(TokenKind::Keyword(Keyword::Insert)) => Some("INSERT"),
            Some(TokenKind::Keyword(Keyword::Join)) => Some("JOIN"),
            Some(TokenKind::Keyword(Keyword::With)) => Some("WITH"),
            Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
                Some("UNION/INTERSECT/EXCEPT")
            }
            Some(TokenKind::Keyword(Keyword::Update)) => Some("UPDATE"),
            _ => None,
        }
    }

    fn bump(&mut self) -> Option<TokenKind> {
        let token = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(token.kind.clone())
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.pos).map(|token| &token.kind)
    }

    const fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}

fn parse_number_literal(raw: &str) -> Result<Value, SqlParseError> {
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

fn predicate_compare(field: String, op: CompareOp, value: Value) -> Predicate {
    let coercion = match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    };

    Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
}
