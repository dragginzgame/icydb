//! Module: error
//!
//! Responsibility: public typed error payloads and facade error category mapping.
//! Does not own: internal error construction or accepted constraint authority.
//! Boundary: maps core diagnostics to canister-facing Wire error records.

mod bootstrap;
#[cfg(test)]
mod tests;

use std::fmt;

use icydb_core::db::QueryError;
use icydb_core::error::{ErrorOrigin as CoreErrorOrigin, InternalError};
use icydb_diagnostic_code::RuntimeBoundaryCode;
use serde::Deserialize;

use crate::db::{DatabaseBootstrapError, TypedAdapterError, TypedOperationError};

pub use icydb_core::error::{
    ConstraintValidationFindingOutput, ConstraintValuePath, ConstraintValuePathComponent,
};

//
// DiagnosticFact
//

/// One bounded numeric parameter attached to a public IcyDB error.
///
/// The leaf error code owns the reason. Facts contain only production-safe
/// numeric parameters interpreted together with that code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct DiagnosticFact {
    tag: u8,
    value: u64,
}

impl DiagnosticFact {
    const fn from_numeric(tag: icydb_diagnostic_code::DiagnosticFactTag, value: u64) -> Self {
        Self {
            tag: tag.raw(),
            value,
        }
    }

    /// Return the stable numeric fact-tag identity.
    #[must_use]
    pub const fn tag(&self) -> u8 {
        self.tag
    }

    /// Return the numeric fact value.
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.value
    }
}

//
// QueryFieldDiagnostic
//

/// One bounded rejected query-field reference attached to a planning error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct QueryFieldDiagnostic {
    role: u8,
    field: String,
}

impl QueryFieldDiagnostic {
    fn from_core(role: icydb_diagnostic_code::QueryFieldRole, field: &str) -> Self {
        Self {
            role: role.raw(),
            field: field.to_owned(),
        }
    }

    /// Return the raw compact role identity.
    #[must_use]
    pub const fn role(&self) -> u8 {
        self.role
    }

    /// Return the known typed role, or `None` for malformed decoded context.
    #[must_use]
    pub const fn known_role(&self) -> Option<icydb_diagnostic_code::QueryFieldRole> {
        icydb_diagnostic_code::QueryFieldRole::known(self.role)
    }

    /// Borrow the exact post-normalization rejected field reference.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }
}

//
// Error
//

#[doc = "Error\n\nPublic error payload."]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct Error {
    code: u16,
    class: u8,
    origin: u8,
    facts: Vec<DiagnosticFact>,
    query_field: Option<QueryFieldDiagnostic>,
}

impl Error {
    /// Build a compact public error from one diagnostic code and origin.
    #[must_use]
    pub const fn from_code(
        code: icydb_diagnostic_code::DiagnosticCode,
        origin: ErrorOrigin,
    ) -> Self {
        Self::from_error_code(code.error_code(), origin)
    }

    /// Build a public error from one numeric wire code and origin.
    #[must_use]
    pub const fn from_error_code(
        code: icydb_diagnostic_code::ErrorCode,
        origin: ErrorOrigin,
    ) -> Self {
        Self {
            code: code.raw(),
            class: code.class().wire_code(),
            origin: origin.wire_code(),
            facts: Vec::new(),
            query_field: None,
        }
    }

    /// Build a compact public error from one public category and origin.
    ///
    /// This helper keeps generated endpoint code concise while the wire
    /// payload itself remains code/detail-first.
    #[must_use]
    pub const fn from_kind(kind: ErrorKind, origin: ErrorOrigin) -> Self {
        let code = kind.diagnostic_code();
        let error_code =
            icydb_diagnostic_code::ErrorCode::from_parts(code, Some(kind.diagnostic_detail()));

        Self::from_error_code(error_code, origin)
    }

    /// Build a compact public runtime-boundary error.
    #[must_use]
    pub const fn from_runtime_boundary(
        boundary: icydb_diagnostic_code::RuntimeBoundaryCode,
        origin: ErrorOrigin,
    ) -> Self {
        let detail = icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary { boundary };
        let error_code =
            icydb_diagnostic_code::ErrorCode::from_parts(detail.diagnostic_code(), Some(detail));

        Self::from_error_code(error_code, origin)
    }

    /// Build a compact public error from a full diagnostic payload.
    #[must_use]
    pub fn from_diagnostic(diagnostic: &icydb_diagnostic_code::Diagnostic) -> Self {
        Self::from_error_code(diagnostic.error_code(), diagnostic.origin().into())
    }

    fn from_internal_error(err: &InternalError) -> Self {
        Self::from_diagnostic_and_facts(&err.diagnostic(), err.diagnostic_facts())
    }

    pub(crate) fn from_diagnostic_and_facts(
        diagnostic: &icydb_diagnostic_code::Diagnostic,
        facts: Vec<(icydb_diagnostic_code::DiagnosticFactTag, u64)>,
    ) -> Self {
        Self::from_diagnostic_facts_and_query_field(diagnostic, facts, None)
    }

    fn from_diagnostic_facts_and_query_field(
        diagnostic: &icydb_diagnostic_code::Diagnostic,
        facts: Vec<(icydb_diagnostic_code::DiagnosticFactTag, u64)>,
        query_field: Option<(icydb_diagnostic_code::QueryFieldRole, &str)>,
    ) -> Self {
        if icydb_diagnostic_code::validate_known_diagnostic_fact_schema(
            diagnostic.error_code(),
            facts.as_slice(),
        )
        .is_err()
        {
            return Self::from_kind(
                ErrorKind::Runtime(RuntimeErrorKind::InvariantViolation),
                diagnostic.origin().into(),
            );
        }

        if let Some((role, field)) = query_field
            && icydb_diagnostic_code::validate_query_field_schema(
                diagnostic.error_code(),
                role.raw(),
                field,
            )
            .is_err()
        {
            return Self::from_kind(
                ErrorKind::Runtime(RuntimeErrorKind::InvariantViolation),
                diagnostic.origin().into(),
            );
        }

        let mut error = Self::from_diagnostic(diagnostic);
        error.facts = facts
            .into_iter()
            .map(|(tag, value)| DiagnosticFact::from_numeric(tag, value))
            .collect();
        error.query_field =
            query_field.map(|(role, field)| QueryFieldDiagnostic::from_core(role, field));
        error
    }

    /// Return the compact diagnostic code.
    #[must_use]
    pub const fn code(&self) -> icydb_diagnostic_code::ErrorCode {
        icydb_diagnostic_code::ErrorCode::from_raw(self.code)
    }

    /// Return the broad compact diagnostic class.
    #[must_use]
    pub const fn class(&self) -> icydb_diagnostic_code::ErrorClass {
        match icydb_diagnostic_code::ErrorClass::from_wire_code(self.class) {
            Some(class) => class,
            None => self.code().class(),
        }
    }

    /// Return the diagnostic origin.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        ErrorOrigin::from_wire_code(self.origin)
    }

    /// Return compact diagnostic identity for this error.
    #[must_use]
    pub fn diagnostic(&self) -> icydb_diagnostic_code::Diagnostic {
        self.code().diagnostic(self.origin().into())
    }

    /// Return the compact diagnostic code for this error.
    #[must_use]
    pub const fn diagnostic_code(&self) -> icydb_diagnostic_code::DiagnosticCode {
        self.code().diagnostic_code()
    }

    /// Borrow the bounded numeric facts carried by this error.
    #[must_use]
    pub const fn facts(&self) -> &[DiagnosticFact] {
        self.facts.as_slice()
    }

    /// Borrow the raw optional query-field context.
    #[must_use]
    pub const fn query_field(&self) -> Option<&QueryFieldDiagnostic> {
        self.query_field.as_ref()
    }

    /// Validate and borrow the optional query-field context.
    ///
    /// Decoded client data remains untrusted until this check succeeds.
    pub fn validated_query_field(
        &self,
    ) -> Result<
        Option<(icydb_diagnostic_code::QueryFieldRole, &str)>,
        icydb_diagnostic_code::QueryFieldSchemaMismatch,
    > {
        let Some(context) = self.query_field.as_ref() else {
            return Ok(None);
        };
        let role = icydb_diagnostic_code::validate_query_field_schema(
            self.code(),
            context.role,
            context.field.as_str(),
        )?;

        Ok(Some((role, context.field.as_str())))
    }

    pub(crate) fn core_facts(
        &self,
    ) -> Option<Vec<(icydb_diagnostic_code::DiagnosticFactTag, u64)>> {
        self.facts
            .iter()
            .map(|fact| {
                icydb_diagnostic_code::DiagnosticFactTag::known(fact.tag)
                    .map(|tag| (tag, fact.value))
            })
            .collect()
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::from_internal_error(&err)
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Self::from_diagnostic_facts_and_query_field(
            &err.diagnostic(),
            err.diagnostic_facts(),
            err.query_field_context(),
        )
    }
}

impl From<DatabaseBootstrapError> for Error {
    fn from(err: DatabaseBootstrapError) -> Self {
        bootstrap::bootstrap_error(&err)
    }
}

impl From<TypedAdapterError> for Error {
    /// Project an adapter refusal into its compact interface diagnostic.
    /// Generated source strings remain on the original Rust error; they are
    /// not accepted numeric identities and do not become public facts.
    fn from(error: TypedAdapterError) -> Self {
        let boundary = match error {
            TypedAdapterError::BatchHandleMismatch => {
                RuntimeBoundaryCode::TypedAdapterBatchHandleMismatch
            }
            TypedAdapterError::BatchRowConsumed => {
                RuntimeBoundaryCode::TypedAdapterBatchRowConsumed
            }
            TypedAdapterError::BindingUnavailable(_) => {
                RuntimeBoundaryCode::TypedAdapterBindingUnavailable
            }
            TypedAdapterError::EntityMismatch => RuntimeBoundaryCode::TypedAdapterEntityMismatch,
            TypedAdapterError::FieldUnavailable => {
                RuntimeBoundaryCode::TypedAdapterFieldUnavailable
            }
            TypedAdapterError::IncompatibleField => {
                RuntimeBoundaryCode::TypedAdapterIncompatibleField
            }
            TypedAdapterError::RowFieldUnavailable => {
                RuntimeBoundaryCode::TypedAdapterRowFieldUnavailable
            }
            TypedAdapterError::RowShapeMismatch => {
                RuntimeBoundaryCode::TypedAdapterRowShapeMismatch
            }
            TypedAdapterError::StaleBinding => RuntimeBoundaryCode::TypedAdapterStaleBinding,
            TypedAdapterError::ValueShapeMismatch => {
                RuntimeBoundaryCode::TypedAdapterValueShapeMismatch
            }
        };
        Self::from_runtime_boundary(boundary, ErrorOrigin::Interface)
    }
}

impl From<TypedOperationError> for Error {
    fn from(error: TypedOperationError) -> Self {
        match error {
            TypedOperationError::Adapter(error) => error.into(),
            TypedOperationError::Database(error) => error,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "E{}", self.code)
    }
}

impl std::error::Error for Error {}

#[doc = "ErrorKind\n\nPublic error category."]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Query admission, planning or result-cardinality failure.
    Query(QueryErrorKind),

    /// Runtime failure.
    Runtime(RuntimeErrorKind),
}

impl ErrorKind {
    /// Return the compact diagnostic code for this public error category.
    #[must_use]
    pub const fn diagnostic_code(self) -> icydb_diagnostic_code::DiagnosticCode {
        match self {
            Self::Query(kind) => kind.diagnostic_code(),
            Self::Runtime(kind) => kind.diagnostic_code(),
        }
    }

    const fn diagnostic_detail(self) -> icydb_diagnostic_code::DiagnosticDetail {
        match self {
            Self::Query(kind) => icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                kind: kind.diagnostic_kind(),
            },
            Self::Runtime(kind) => icydb_diagnostic_code::DiagnosticDetail::RuntimeKind {
                kind: kind.diagnostic_kind(),
            },
        }
    }
}

// Keep the facade's named Debug and public type identities while declaring
// each category/code pair once for both compact-code and detail projections.
macro_rules! facade_error_kind {
    ($kind:ident { $( $(#[$attr:meta])* $variant:ident => $code:ident, )+ }) => {
        #[doc = concat!(stringify!($kind), " public error category.")]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum $kind {
            $( $(#[$attr])* $variant, )+
        }

        impl $kind {
            /// Return the compact diagnostic code for this error category.
            #[must_use]
            pub const fn diagnostic_code(self) -> icydb_diagnostic_code::DiagnosticCode {
                match self {
                    $(Self::$variant => icydb_diagnostic_code::DiagnosticCode::$code,)+
                }
            }

            const fn diagnostic_kind(self) -> icydb_diagnostic_code::$kind {
                match self {
                    $(Self::$variant => icydb_diagnostic_code::$kind::$variant,)+
                }
            }
        }
    };
}

facade_error_kind! {
    RuntimeErrorKind {
        /// Persisted data failed an integrity check.
        Corruption => RuntimeCorruption,
        /// Persisted bytes use a format this release cannot read.
        IncompatiblePersistedFormat => RuntimeIncompatiblePersistedFormat,
        /// An internal runtime invariant was violated.
        InvariantViolation => RuntimeInvariantViolation,
        /// The operation conflicts with current database state.
        Conflict => RuntimeConflict,
        /// The requested runtime resource was absent.
        NotFound => RuntimeNotFound,
        /// The requested runtime operation is unsupported.
        Unsupported => RuntimeUnsupported,
        /// An internal operation failed.
        Internal => RuntimeInternal,
    }
}

facade_error_kind! {
    QueryErrorKind {
        /// Validation failed.
        Validate => QueryValidate,
        /// Intent validation failed.
        Intent => QueryIntent,
        /// Planning failed.
        Plan => QueryPlan,
        /// Pagination lacked ordering.
        UnorderedPagination => QueryUnorderedPagination,
        /// Continuation cursor was invalid.
        InvalidContinuationCursor => QueryInvalidContinuationCursor,
        /// No rows matched.
        NotFound => QueryNotFound,
        /// More than one row matched.
        NotUnique => QueryNotUnique,
    }
}

// The facade intentionally exposes named Debug rather than compact numeric
// Debug. Generate its identity projection in both directions from one list.
macro_rules! facade_error_origin {
    ($($variant:ident),+ $(,)?) => {
        /// Public error origin.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum ErrorOrigin {
            $(
                #[doc = concat!("Error originating from [`icydb_diagnostic_code::ErrorOrigin::", stringify!($variant), "`].")]
                $variant,
            )+
        }

        impl ErrorOrigin {
            const fn wire_code(self) -> u8 {
                self.diagnostic_origin().wire_code()
            }

            const fn from_wire_code(code: u8) -> Self {
                Self::from_diagnostic_origin(icydb_diagnostic_code::ErrorOrigin::from_wire_code(code))
            }

            const fn diagnostic_origin(self) -> icydb_diagnostic_code::ErrorOrigin {
                match self {
                    $(Self::$variant => icydb_diagnostic_code::ErrorOrigin::$variant,)+
                }
            }

            const fn from_diagnostic_origin(origin: icydb_diagnostic_code::ErrorOrigin) -> Self {
                match origin {
                    $(icydb_diagnostic_code::ErrorOrigin::$variant => Self::$variant,)+
                }
            }
        }
    };
}

facade_error_origin! {
    Cursor, Executor, Identity, Index, Interface, Planner, Query, Recovery,
    Response, Runtime, Serialize, Store,
}

impl From<CoreErrorOrigin> for ErrorOrigin {
    fn from(origin: CoreErrorOrigin) -> Self {
        Self::from_diagnostic_origin(origin.diagnostic_origin())
    }
}

impl From<ErrorOrigin> for icydb_diagnostic_code::ErrorOrigin {
    fn from(origin: ErrorOrigin) -> Self {
        origin.diagnostic_origin()
    }
}

impl From<icydb_diagnostic_code::ErrorOrigin> for ErrorOrigin {
    fn from(origin: icydb_diagnostic_code::ErrorOrigin) -> Self {
        Self::from_diagnostic_origin(origin)
    }
}
