//! Public source-migration command and status contracts.
//!
//! This module owns the bounded Candid surface only. Planning, publication,
//! recovery, and controller authorization remain separate authorities.

use candid::CandidType;
use icydb_schema::{
    EntitySourceKey, ExpectedAcceptedHead, SchemaMigrationPlanDigest, TargetDatabaseIdentity,
};
use serde::Deserialize;

const MAX_SCHEMA_MIGRATION_STATUS_CURSOR_BYTES: usize = 256;
pub(in crate::db::schema) const MAX_SCHEMA_MIGRATION_FINDINGS_PER_PAGE: usize = 64;

/// One controller-authorized source-migration operation.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SchemaMigrationCommand {
    /// Adopt an exact existing version-1 generated schema without changing it.
    Adopt {
        /// Exact target database identity observed before confirmation.
        expected_database: TargetDatabaseIdentity,
        /// Exact accepted head observed before confirmation.
        expected_head: ExpectedAcceptedHead,
    },
    /// Prepare, resume, or complete one bounded migration step. A new plan may
    /// remain `Idle` while this step drains a prior terminal plan's journal.
    Advance {
        /// Exact target database identity observed by the caller.
        expected_database: TargetDatabaseIdentity,
        /// Exact predecessor accepted head.
        expected_head: ExpectedAcceptedHead,
        /// Exact deployed coordinated migration plan.
        expected_plan: SchemaMigrationPlanDigest,
        /// Exact finding page acknowledged before resuming, when required.
        acknowledged_finding_page: Option<u64>,
    },
    /// Abort a migration before irreversible row rewriting begins.
    Abort {
        /// Exact target database identity observed by the caller.
        expected_database: TargetDatabaseIdentity,
        /// Exact predecessor accepted head.
        expected_head: ExpectedAcceptedHead,
        /// Exact deployed coordinated migration plan.
        expected_plan: SchemaMigrationPlanDigest,
    },
}

/// Bounded status-page request. Cursors are opaque engine-owned bytes.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SchemaMigrationStatusRequest {
    cursor: Option<Vec<u8>>,
}

impl SchemaMigrationStatusRequest {
    /// Construct a status request from an optional opaque cursor.
    #[must_use]
    pub const fn new(cursor: Option<Vec<u8>>) -> Self {
        Self { cursor }
    }

    /// Borrow the optional opaque cursor.
    #[must_use]
    pub fn cursor(&self) -> Option<&[u8]> {
        self.cursor.as_deref()
    }

    pub(in crate::db::schema) fn validate(&self) -> bool {
        self.cursor
            .as_ref()
            .is_none_or(|cursor| cursor.len() <= MAX_SCHEMA_MIGRATION_STATUS_CURSOR_BYTES)
    }
}

/// Current source-migration lifecycle phase.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SchemaMigrationPhase {
    /// Existing generated entities have not been adopted into lineage authority.
    Unadopted,
    /// Existing version-1 generated entities were adopted exactly.
    Adopted,
    /// An exact coordinated plan is deployed and ready for its first advance.
    Idle,
    /// The exact plan and candidate were prepared durably.
    Prepared,
    /// Historical candidate validation is in progress.
    Validating,
    /// Validation completed and row rewriting may begin.
    ReadyToRewrite,
    /// Candidate row layouts are being written.
    RewritingRows,
    /// Candidate index generations are being rebuilt.
    RebuildingIndexes,
    /// Complete candidate state is undergoing final validation.
    FinalValidation,
    /// Candidate authority is crossing the atomic publication boundary.
    Publishing,
    /// The coordinated target entity versions are accepted.
    Applied,
    /// Validation produced a terminal rejected result.
    Rejected,
    /// A pre-rewrite migration was aborted.
    Aborted,
}

/// One canonical declared entity-version transition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaMigrationEntityTransition {
    entity: EntitySourceKey,
    from_version: Option<u32>,
    to_version: u32,
}

impl SchemaMigrationEntityTransition {
    pub(in crate::db::schema) const fn new(
        entity: EntitySourceKey,
        from_version: Option<u32>,
        to_version: u32,
    ) -> Self {
        Self {
            entity,
            from_version,
            to_version,
        }
    }

    /// Borrow the current entity source key.
    #[must_use]
    pub const fn entity(&self) -> &EntitySourceKey {
        &self.entity
    }

    /// Return the predecessor source version, or `None` for adoption.
    #[must_use]
    pub const fn from_version(&self) -> Option<u32> {
        self.from_version
    }

    /// Return the declared target source version.
    #[must_use]
    pub const fn to_version(&self) -> u32 {
        self.to_version
    }
}

/// Typed migration validation-finding family.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SchemaMigrationFindingKind {
    /// A closed transform failed on one accepted row.
    Transform,
    /// Candidate unique-index meaning failed.
    UniqueIndex,
    /// Candidate relation meaning failed.
    Relation,
    /// Candidate row-constraint meaning failed.
    Constraint,
}

/// One bounded accepted-ID migration finding.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaMigrationFinding {
    kind: SchemaMigrationFindingKind,
    entity_tag: u64,
    primary_key: Vec<u8>,
}

impl SchemaMigrationFinding {
    pub(in crate::db::schema) const fn new(
        kind: SchemaMigrationFindingKind,
        entity_tag: u64,
        primary_key: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            entity_tag,
            primary_key,
        }
    }

    /// Return the typed validation-finding family.
    #[must_use]
    pub const fn kind(&self) -> SchemaMigrationFindingKind {
        self.kind
    }

    /// Return the accepted entity identity associated with the finding.
    #[must_use]
    pub const fn entity_tag(&self) -> u64 {
        self.entity_tag
    }

    /// Borrow the exact primary-key bytes associated with the finding.
    #[must_use]
    pub const fn primary_key(&self) -> &[u8] {
        self.primary_key.as_slice()
    }
}

/// Terminal proof for one adopted or applied source transition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaMigrationReceipt {
    database_identity: TargetDatabaseIdentity,
    plan_digest: Option<SchemaMigrationPlanDigest>,
    prior_head: ExpectedAcceptedHead,
    accepted_head: ExpectedAcceptedHead,
}

impl SchemaMigrationReceipt {
    pub(in crate::db::schema) const fn new(
        database_identity: TargetDatabaseIdentity,
        plan_digest: Option<SchemaMigrationPlanDigest>,
        prior_head: ExpectedAcceptedHead,
        accepted_head: ExpectedAcceptedHead,
    ) -> Self {
        Self {
            database_identity,
            plan_digest,
            prior_head,
            accepted_head,
        }
    }

    /// Return the target database identity.
    #[must_use]
    pub const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    /// Return the coordinated plan digest, or `None` for adoption.
    #[must_use]
    pub const fn plan_digest(&self) -> Option<SchemaMigrationPlanDigest> {
        self.plan_digest
    }

    /// Borrow the predecessor accepted head.
    #[must_use]
    pub const fn prior_head(&self) -> &ExpectedAcceptedHead {
        &self.prior_head
    }

    /// Borrow the terminal accepted head.
    #[must_use]
    pub const fn accepted_head(&self) -> &ExpectedAcceptedHead {
        &self.accepted_head
    }
}

/// One bounded page of deployed source-migration status.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaMigrationStatusPage {
    database_identity: TargetDatabaseIdentity,
    accepted_head: ExpectedAcceptedHead,
    plan_digest: Option<SchemaMigrationPlanDigest>,
    phase: SchemaMigrationPhase,
    transitions: Vec<SchemaMigrationEntityTransition>,
    rows_validated: u64,
    rows_rewritten: u64,
    indexes_rebuilt: u32,
    findings: Vec<SchemaMigrationFinding>,
    next_cursor: Option<Vec<u8>>,
    terminal_receipt: Option<SchemaMigrationReceipt>,
}

impl SchemaMigrationStatusPage {
    #[expect(
        clippy::too_many_arguments,
        reason = "the constructor mirrors the frozen bounded status-page contract"
    )]
    pub(in crate::db::schema) fn new(
        database_identity: TargetDatabaseIdentity,
        accepted_head: ExpectedAcceptedHead,
        plan_digest: Option<SchemaMigrationPlanDigest>,
        phase: SchemaMigrationPhase,
        transitions: Vec<SchemaMigrationEntityTransition>,
        rows_validated: u64,
        rows_rewritten: u64,
        indexes_rebuilt: u32,
        findings: Vec<SchemaMigrationFinding>,
        next_cursor: Option<Vec<u8>>,
        terminal_receipt: Option<SchemaMigrationReceipt>,
    ) -> Self {
        debug_assert!(findings.len() <= MAX_SCHEMA_MIGRATION_FINDINGS_PER_PAGE);
        Self {
            database_identity,
            accepted_head,
            plan_digest,
            phase,
            transitions,
            rows_validated,
            rows_rewritten,
            indexes_rebuilt,
            findings,
            next_cursor,
            terminal_receipt,
        }
    }

    /// Return the target database identity.
    #[must_use]
    pub const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    /// Borrow the current accepted database head.
    #[must_use]
    pub const fn accepted_head(&self) -> &ExpectedAcceptedHead {
        &self.accepted_head
    }

    /// Return the deployed plan digest when one is present.
    #[must_use]
    pub const fn plan_digest(&self) -> Option<SchemaMigrationPlanDigest> {
        self.plan_digest
    }

    /// Return the current lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> SchemaMigrationPhase {
        self.phase
    }

    /// Borrow canonical entity transitions.
    #[must_use]
    pub const fn transitions(&self) -> &[SchemaMigrationEntityTransition] {
        self.transitions.as_slice()
    }

    /// Return rows validated so far.
    #[must_use]
    pub const fn rows_validated(&self) -> u64 {
        self.rows_validated
    }

    /// Return rows rewritten so far.
    #[must_use]
    pub const fn rows_rewritten(&self) -> u64 {
        self.rows_rewritten
    }

    /// Return indexes rebuilt so far.
    #[must_use]
    pub const fn indexes_rebuilt(&self) -> u32 {
        self.indexes_rebuilt
    }

    /// Borrow this page's typed findings.
    #[must_use]
    pub const fn findings(&self) -> &[SchemaMigrationFinding] {
        self.findings.as_slice()
    }

    /// Borrow the optional next opaque cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&[u8]> {
        self.next_cursor.as_deref()
    }

    /// Borrow the optional terminal receipt.
    #[must_use]
    pub const fn terminal_receipt(&self) -> Option<&SchemaMigrationReceipt> {
        self.terminal_receipt.as_ref()
    }
}
