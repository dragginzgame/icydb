//! Module: diagnostic
//! Responsibility: compact diagnostic vocabulary for the example crate.
//! Does not own: production error codes or runtime error mapping.
//! Boundary: carries typed failures without relying on string matching.

use std::fmt::{self, Display};

///
/// StyleDiagnosticCode
///
/// Stable diagnostic categories used by the documentation-only example crate.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleDiagnosticCode {
    EmptyEntityName,
    EmptyPlanRoute,
    MissingSnapshotVersion,
}

impl Display for StyleDiagnosticCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::EmptyEntityName => "empty entity name",
            Self::EmptyPlanRoute => "empty plan route",
            Self::MissingSnapshotVersion => "missing snapshot version",
        };

        f.write_str(label)
    }
}

///
/// StyleDiagnostic
///
/// Typed diagnostic value used by constructors that enforce example invariants.
/// The code is the stable contract; the message is only human-facing context.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleDiagnostic {
    code: StyleDiagnosticCode,
    message: &'static str,
}

impl StyleDiagnostic {
    /// Build one diagnostic from a stable code and static message.
    #[must_use]
    pub const fn new(code: StyleDiagnosticCode, message: &'static str) -> Self {
        Self { code, message }
    }

    /// Return the diagnostic code for an empty entity name.
    #[must_use]
    pub const fn empty_entity_name() -> Self {
        Self::new(
            StyleDiagnosticCode::EmptyEntityName,
            "entity name must not be empty",
        )
    }

    /// Return the diagnostic code for an empty route label.
    #[must_use]
    pub const fn empty_plan_route() -> Self {
        Self::new(
            StyleDiagnosticCode::EmptyPlanRoute,
            "plan route label must not be empty",
        )
    }

    /// Return the diagnostic code for an absent accepted snapshot version.
    #[must_use]
    pub const fn missing_snapshot_version() -> Self {
        Self::new(
            StyleDiagnosticCode::MissingSnapshotVersion,
            "accepted snapshot version must be greater than zero",
        )
    }

    /// Return the stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> StyleDiagnosticCode {
        self.code
    }

    /// Return the human-facing diagnostic message.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl Display for StyleDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for StyleDiagnostic {}
