//! Schema transition admission identity and version/fingerprint gate.

use std::fmt;

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        schema::{
            PersistedSchemaSnapshot, SchemaVersion, accepted_schema_admission_fingerprint,
            accepted_schema_admission_fingerprint_method_version,
        },
    },
    error::InternalError,
};

use super::{
    SchemaTransitionRejection, SchemaTransitionRejectionDetail,
    SchemaTransitionRejectionDetailCode, SchemaTransitionRejectionKind,
};

#[cfg(test)]
use crate::db::codec::hex::encode_hex_lower;

///
/// SchemaAdmissionIdentity
///
/// SchemaAdmissionIdentity is the 0.177 version/method/fingerprint tuple that
/// schema-owned admission compares before mutation compatibility may publish a
/// changed accepted shape. Query hot paths consume already accepted identity
/// and must not build this candidate tuple.
///

#[cfg_attr(test, derive(Debug))]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaAdmissionIdentity {
    pub(super) schema_version: SchemaVersion,
    pub(super) fingerprint_method_version: u8,
    pub(super) schema_fingerprint: CommitSchemaFingerprint,
}

impl SchemaAdmissionIdentity {
    fn from_snapshot(snapshot: &PersistedSchemaSnapshot) -> Result<Self, InternalError> {
        Ok(Self {
            schema_version: snapshot.version(),
            fingerprint_method_version: accepted_schema_admission_fingerprint_method_version(),
            schema_fingerprint: accepted_schema_admission_fingerprint(snapshot)?,
        })
    }
}

///
/// SchemaAdmissionIdentityComparison
///
/// Pairs stored and candidate admission identity. Enforcement remains separate
/// so 0.177 can land identity preparation before changing transition policy.
///

#[cfg_attr(test, derive(Debug))]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaAdmissionIdentityComparison {
    pub(super) stored: SchemaAdmissionIdentity,
    pub(super) candidate: SchemaAdmissionIdentity,
}

impl SchemaAdmissionIdentityComparison {
    pub(in crate::db::schema) fn from_snapshots(
        stored: &PersistedSchemaSnapshot,
        candidate: &PersistedSchemaSnapshot,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            stored: SchemaAdmissionIdentity::from_snapshot(stored)?,
            candidate: SchemaAdmissionIdentity::from_snapshot(candidate)?,
        })
    }
}

///
/// SchemaAdmissionRejectionReason
///
/// SchemaAdmissionRejectionReason is the schema-version admission reason before
/// it is rendered into user-facing diagnostic text. Keeping this structured
/// lets policy tests prove the matrix without matching formatted errors.
///

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaAdmissionRejectionReason {
    EmptyVersionBump,
    FingerprintMethodMismatch,
    MissingVersionBump,
    VersionGap,
    VersionRollback,
}

impl SchemaAdmissionRejectionReason {
    const fn code(self) -> u8 {
        match self {
            Self::EmptyVersionBump => 1,
            Self::FingerprintMethodMismatch => 2,
            Self::MissingVersionBump => 3,
            Self::VersionGap => 4,
            Self::VersionRollback => 5,
        }
    }

    #[cfg(test)]
    const fn detail(self) -> &'static str {
        match self {
            Self::EmptyVersionBump => "schema_version bumped without schema shape change",
            Self::FingerprintMethodMismatch => "schema fingerprint method changed",
            Self::MissingVersionBump => "schema changed without schema_version bump",
            Self::VersionGap => "schema_version jumped",
            Self::VersionRollback => "schema_version moved backwards",
        }
    }
}

impl fmt::Debug for SchemaAdmissionRejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = self.code();
        write!(f, "{code}")
    }
}

///
/// SchemaAdmissionRejectionClassification
///
/// SchemaAdmissionRejectionClassification is the structured admission-matrix
/// decision used before final transition rejection formatting.
///

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaAdmissionRejectionClassification {
    pub(super) reason: SchemaAdmissionRejectionReason,
    pub(super) expected_next: Option<u32>,
}

impl SchemaAdmissionRejectionClassification {
    const fn new(reason: SchemaAdmissionRejectionReason, expected_next: Option<u32>) -> Self {
        Self {
            reason,
            expected_next,
        }
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::schema) const fn reason(self) -> SchemaAdmissionRejectionReason {
        self.reason
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::schema) const fn expected_next(self) -> Option<u32> {
        self.expected_next
    }
}

impl fmt::Debug for SchemaAdmissionRejectionClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = self.reason.code();
        let expected_next = self.expected_next.unwrap_or_default();
        write!(f, "{reason}:{expected_next}")
    }
}

// Apply the 0.177 version/method/fingerprint gate before mutation compatibility
// classification. Passing this gate only admits a candidate to compatibility
// checks; it does not publish the candidate snapshot by itself.
pub(in crate::db::schema) fn schema_admission_rejection(
    comparison: SchemaAdmissionIdentityComparison,
) -> Option<SchemaTransitionRejection> {
    let classification = classify_schema_admission_rejection(comparison)?;

    Some(SchemaTransitionRejection::new(
        SchemaTransitionRejectionKind::SchemaVersion,
        schema_admission_rejection_detail(classification, comparison),
        Some(classification),
    ))
}

pub(super) fn classify_schema_admission_rejection(
    comparison: SchemaAdmissionIdentityComparison,
) -> Option<SchemaAdmissionRejectionClassification> {
    if comparison.stored.fingerprint_method_version
        != comparison.candidate.fingerprint_method_version
    {
        return Some(SchemaAdmissionRejectionClassification::new(
            SchemaAdmissionRejectionReason::FingerprintMethodMismatch,
            None,
        ));
    }

    let stored_version = comparison.stored.schema_version.get();
    let candidate_version = comparison.candidate.schema_version.get();
    let same_fingerprint =
        comparison.stored.schema_fingerprint == comparison.candidate.schema_fingerprint;

    if candidate_version < stored_version {
        return Some(SchemaAdmissionRejectionClassification::new(
            SchemaAdmissionRejectionReason::VersionRollback,
            None,
        ));
    }

    if stored_version == candidate_version {
        return (!same_fingerprint).then(|| {
            SchemaAdmissionRejectionClassification::new(
                SchemaAdmissionRejectionReason::MissingVersionBump,
                None,
            )
        });
    }

    if same_fingerprint {
        return Some(SchemaAdmissionRejectionClassification::new(
            SchemaAdmissionRejectionReason::EmptyVersionBump,
            None,
        ));
    }

    let expected_next = stored_version.saturating_add(1);
    if candidate_version == expected_next {
        None
    } else {
        Some(SchemaAdmissionRejectionClassification::new(
            SchemaAdmissionRejectionReason::VersionGap,
            Some(expected_next),
        ))
    }
}

#[cfg(test)]
fn schema_admission_rejection_detail(
    classification: SchemaAdmissionRejectionClassification,
    comparison: SchemaAdmissionIdentityComparison,
) -> SchemaTransitionRejectionDetail {
    let facts = schema_admission_identity_facts(comparison);
    let extra = classification
        .expected_next
        .map(|expected_next| format!("expected_next={expected_next}"));

    let rich = match extra {
        Some(extra) => format!("{}: {facts} {extra}", classification.reason.detail()),
        None => format!("{}: {facts}", classification.reason.detail()),
    };

    SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::SchemaAdmission, rich)
}

#[cfg(not(test))]
const fn schema_admission_rejection_detail(
    classification: SchemaAdmissionRejectionClassification,
    comparison: SchemaAdmissionIdentityComparison,
) -> SchemaTransitionRejectionDetail {
    let _ = (classification, comparison);
    SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::SchemaAdmission)
}

#[cfg(test)]
fn schema_admission_identity_facts(comparison: SchemaAdmissionIdentityComparison) -> String {
    format!(
        "stored_version={} candidate_version={} stored_method={} candidate_method={} stored_fingerprint={} candidate_fingerprint={}",
        comparison.stored.schema_version.get(),
        comparison.candidate.schema_version.get(),
        comparison.stored.fingerprint_method_version,
        comparison.candidate.fingerprint_method_version,
        encode_hex_lower(&comparison.stored.schema_fingerprint),
        encode_hex_lower(&comparison.candidate.schema_fingerprint),
    )
}
