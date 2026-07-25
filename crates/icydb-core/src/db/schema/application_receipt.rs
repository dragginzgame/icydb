//! Module: db::schema::application_receipt
//! Responsibility: define durable schema-application receipt and job identity contracts.
//! Does not own: proposal lowering, accepted-schema mutation, or stable-memory access.
//! Boundary: application admission/result -> bounded current-form durable record.

use crate::db::codec::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
    write_hash_u64,
};
use crate::error::InternalError;
use candid::CandidType;
use icydb_diagnostic_code::{ErrorClass, ErrorCode, ErrorOrigin};
use icydb_schema::{
    ExpectedAcceptedHead, SchemaProposalDigest, SchemaSubmissionKey, TargetDatabaseIdentity,
    TargetStoreIdentity,
};
use serde::Deserialize;
use sha2::Digest;

const SCHEMA_CHANGE_JOB_ID_PROFILE: &[u8] = b"icydb.schema-application.job-id.v1";
const MAX_SCHEMA_CHANGE_ACTIVATIONS: usize = 512;

///
/// SchemaChangeJobId
///
/// Opaque identity for one admitted resumable schema change.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[serde(transparent)]
pub struct SchemaChangeJobId([u8; 32]);

impl SchemaChangeJobId {
    fn from_bytes(bytes: [u8; 32]) -> Result<Self, InternalError> {
        if bytes == [0; 32] {
            return Err(InternalError::store_corruption());
        }
        Ok(Self(bytes))
    }

    /// Return the opaque job identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    fn validate(self) -> Result<(), InternalError> {
        if self.0 == [0; 32] {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

///
/// SchemaChangeJob
///
/// Queryable resumable work attached to a pending schema receipt.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaChangeJob {
    id: SchemaChangeJobId,
}

impl SchemaChangeJob {
    #[cfg(test)]
    pub(in crate::db) const fn new(id: SchemaChangeJobId) -> Self {
        Self { id }
    }

    /// Return the opaque durable job identity.
    #[must_use]
    pub const fn id(self) -> SchemaChangeJobId {
        self.id
    }
}

///
/// SchemaChangeFailure
///
/// Compact typed failure retained after an admitted schema job terminates.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaChangeFailure {
    code: u16,
    class: u8,
    origin: u8,
}

impl SchemaChangeFailure {
    #[cfg(test)]
    pub(in crate::db) const fn from_error_code(code: ErrorCode, origin: ErrorOrigin) -> Self {
        Self {
            code: code.raw(),
            class: code.class().wire_code(),
            origin: origin.wire_code(),
        }
    }

    /// Return the stable compact diagnostic code.
    #[must_use]
    pub const fn code(self) -> ErrorCode {
        ErrorCode::from_raw(self.code)
    }

    /// Return the compact diagnostic class.
    #[must_use]
    pub const fn class(self) -> ErrorClass {
        match ErrorClass::from_wire_code(self.class) {
            Some(class) => class,
            None => self.code().class(),
        }
    }

    /// Return the compact diagnostic origin.
    #[must_use]
    pub const fn origin(self) -> ErrorOrigin {
        ErrorOrigin::from_wire_code(self.origin)
    }

    fn validate(self) -> Result<(), InternalError> {
        if !self.code().is_known()
            || ErrorClass::from_wire_code(self.class) != Some(self.code().class())
            || ErrorOrigin::from_known_wire_code(self.origin).is_none()
        {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

///
/// SchemaChangeOutcome
///
/// Durable terminal or resumable outcome of one admitted proposal.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SchemaChangeOutcome {
    /// The exact proposal was already reflected by the accepted head.
    NoOp {
        /// Accepted head retained by the no-op.
        accepted_head: ExpectedAcceptedHead,
    },
    /// The proposal published a new accepted head atomically.
    Applied {
        /// Newly accepted database-wide head.
        accepted_head: ExpectedAcceptedHead,
    },
    /// Bounded activation work remains under one durable job.
    Pending {
        /// Resumable job identity.
        job: SchemaChangeJob,
        /// Candidate head reserved by the pending work.
        candidate_head: ExpectedAcceptedHead,
    },
    /// An admitted job reached a durable typed failure.
    Failed {
        /// Compact terminal failure.
        error: SchemaChangeFailure,
    },
}

///
/// SchemaChangeReceipt
///
/// Durable idempotency and result record for one schema submission.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaChangeReceipt {
    database_identity: TargetDatabaseIdentity,
    submission_key: SchemaSubmissionKey,
    proposal_digest: SchemaProposalDigest,
    prior_head: ExpectedAcceptedHead,
    outcome: SchemaChangeOutcome,
}

impl SchemaChangeReceipt {
    #[cfg(test)]
    pub(in crate::db) fn new(
        database_identity: TargetDatabaseIdentity,
        submission_key: SchemaSubmissionKey,
        proposal_digest: SchemaProposalDigest,
        prior_head: ExpectedAcceptedHead,
        outcome: SchemaChangeOutcome,
    ) -> Result<Self, InternalError> {
        let receipt = Self {
            database_identity,
            submission_key,
            proposal_digest,
            prior_head,
            outcome,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Return the target database identity admitted with the proposal.
    #[must_use]
    pub const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    /// Borrow the immutable caller submission key.
    #[must_use]
    pub const fn submission_key(&self) -> &SchemaSubmissionKey {
        &self.submission_key
    }

    /// Return the canonical proposal digest.
    #[must_use]
    pub const fn proposal_digest(&self) -> SchemaProposalDigest {
        self.proposal_digest
    }

    /// Borrow the exact accepted head observed before admission.
    #[must_use]
    pub const fn prior_head(&self) -> &ExpectedAcceptedHead {
        &self.prior_head
    }

    /// Borrow the durable terminal or pending outcome.
    #[must_use]
    pub const fn outcome(&self) -> &SchemaChangeOutcome {
        &self.outcome
    }

    pub(in crate::db) fn validate(&self) -> Result<(), InternalError> {
        if self.database_identity.to_bytes() == [0; 32]
            || self.proposal_digest.to_bytes() == [0; 32]
        {
            return Err(InternalError::store_corruption());
        }
        validate_head(&self.prior_head, true)?;
        match &self.outcome {
            SchemaChangeOutcome::NoOp { accepted_head } => {
                validate_head(accepted_head, true)?;
                if accepted_head != &self.prior_head {
                    return Err(InternalError::store_corruption());
                }
            }
            SchemaChangeOutcome::Applied { accepted_head } => {
                validate_head(accepted_head, false)?;
                if accepted_head == &self.prior_head {
                    return Err(InternalError::store_corruption());
                }
            }
            SchemaChangeOutcome::Pending {
                job,
                candidate_head,
            } => {
                job.id.validate()?;
                validate_head(candidate_head, false)?;
                if candidate_head == &self.prior_head
                    || job.id
                        != derive_schema_change_job_id(
                            self.database_identity,
                            &self.submission_key,
                            self.proposal_digest,
                            &self.prior_head,
                        )?
                {
                    return Err(InternalError::store_corruption());
                }
            }
            SchemaChangeOutcome::Failed { error } => error.validate()?,
        }
        Ok(())
    }
}

///
/// SchemaChangeActivationKind
///
/// Accepted 0.211 activation family resumed by one schema-change job.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum SchemaChangeActivationKind {
    Check,
    NotNull,
    Relation,
    UniqueIndex,
}

///
/// SchemaChangeActivation
///
/// Minimal accepted activation identity carried by a pending application job.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct SchemaChangeActivation {
    store: TargetStoreIdentity,
    entity_tag: u64,
    constraint_id: u32,
    kind: SchemaChangeActivationKind,
}

impl SchemaChangeActivation {
    #[cfg(test)]
    pub(in crate::db) fn new(
        store: TargetStoreIdentity,
        entity_tag: u64,
        constraint_id: u32,
        kind: SchemaChangeActivationKind,
    ) -> Result<Self, InternalError> {
        if entity_tag == 0 || constraint_id == 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            store,
            entity_tag,
            constraint_id,
            kind,
        })
    }

    fn validate(&self) -> Result<(), InternalError> {
        if self.store.to_bytes() == [0; 32] || self.entity_tag == 0 || self.constraint_id == 0 {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

///
/// SchemaApplicationRecord
///
/// Canonical durable receipt plus the exact 0.211 activations owned by a
/// pending job. Terminal receipts cannot retain activation state.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct SchemaApplicationRecord {
    receipt: SchemaChangeReceipt,
    activations: Vec<SchemaChangeActivation>,
}

impl SchemaApplicationRecord {
    #[cfg(test)]
    pub(in crate::db) fn new(
        receipt: SchemaChangeReceipt,
        activations: Vec<SchemaChangeActivation>,
    ) -> Result<Self, InternalError> {
        let record = Self {
            receipt,
            activations,
        };
        record.validate()?;
        Ok(record)
    }

    pub(in crate::db) const fn receipt(&self) -> &SchemaChangeReceipt {
        &self.receipt
    }

    pub(in crate::db) fn validate(&self) -> Result<(), InternalError> {
        self.receipt.validate()?;
        if self.activations.len() > MAX_SCHEMA_CHANGE_ACTIVATIONS
            || self.activations.windows(2).any(|pair| {
                (pair[0].store, pair[0].entity_tag, pair[0].constraint_id)
                    >= (pair[1].store, pair[1].entity_tag, pair[1].constraint_id)
            })
            || self
                .activations
                .iter()
                .any(|activation| activation.validate().is_err())
        {
            return Err(InternalError::store_corruption());
        }

        let pending = matches!(self.receipt.outcome(), SchemaChangeOutcome::Pending { .. });
        if pending == self.activations.is_empty() {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

pub(in crate::db) fn derive_schema_change_job_id(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
    proposal_digest: SchemaProposalDigest,
    prior_head: &ExpectedAcceptedHead,
) -> Result<SchemaChangeJobId, InternalError> {
    validate_head(prior_head, true)?;
    let mut hasher = new_hash_sha256_prefixed(SCHEMA_CHANGE_JOB_ID_PROFILE);
    hasher.update(database_identity.to_bytes());
    write_hash_str_u32(&mut hasher, submission_key.as_str());
    hasher.update(proposal_digest.to_bytes());
    write_head(&mut hasher, prior_head);
    SchemaChangeJobId::from_bytes(finalize_hash_sha256(hasher))
}

fn validate_head(head: &ExpectedAcceptedHead, empty_allowed: bool) -> Result<(), InternalError> {
    match head {
        ExpectedAcceptedHead::Empty if empty_allowed => Ok(()),
        ExpectedAcceptedHead::Exact { revision: 0, .. } | ExpectedAcceptedHead::Empty => {
            Err(InternalError::store_corruption())
        }
        ExpectedAcceptedHead::Exact { fingerprint, .. } if fingerprint.to_bytes() == [0; 32] => {
            Err(InternalError::store_corruption())
        }
        ExpectedAcceptedHead::Exact { .. } => Ok(()),
    }
}

fn write_head(hasher: &mut sha2::Sha256, head: &ExpectedAcceptedHead) {
    match head {
        ExpectedAcceptedHead::Empty => write_hash_tag_u8(hasher, 0),
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => {
            write_hash_tag_u8(hasher, 1);
            write_hash_u64(hasher, *revision);
            hasher.update(fingerprint.to_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SchemaApplicationRecord, SchemaChangeActivation, SchemaChangeActivationKind,
        SchemaChangeFailure, SchemaChangeJob, SchemaChangeOutcome, SchemaChangeReceipt,
        derive_schema_change_job_id,
    };
    use icydb_schema::{
        ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposalDigest, SchemaSubmissionKey,
        TargetDatabaseIdentity, TargetStoreIdentity,
    };

    #[test]
    fn schema_change_job_identity_covers_the_complete_idempotency_tuple() {
        let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
        let submission =
            SchemaSubmissionKey::try_new("job-id").expect("submission key should admit");
        let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
        let empty = derive_schema_change_job_id(
            database,
            &submission,
            digest,
            &ExpectedAcceptedHead::Empty,
        )
        .expect("empty-head identity should derive");
        let exact = derive_schema_change_job_id(
            database,
            &submission,
            digest,
            &ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
            },
        )
        .expect("exact-head identity should derive");

        assert_ne!(empty, exact);
    }

    #[test]
    fn pending_and_terminal_record_state_is_exact() {
        let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
        let submission =
            SchemaSubmissionKey::try_new("state").expect("submission key should admit");
        let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
        let head = ExpectedAcceptedHead::Empty;
        let job = SchemaChangeJob::new(
            derive_schema_change_job_id(database, &submission, digest, &head)
                .expect("job identity should derive"),
        );
        let pending = SchemaChangeReceipt::new(
            database,
            submission.clone(),
            digest,
            head.clone(),
            SchemaChangeOutcome::Pending {
                job,
                candidate_head: ExpectedAcceptedHead::Exact {
                    revision: 1,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
                },
            },
        )
        .expect("pending receipt should admit");
        assert!(
            SchemaApplicationRecord::new(pending.clone(), Vec::new()).is_err(),
            "pending records require exact activation ownership",
        );
        let activation = SchemaChangeActivation::new(
            TargetStoreIdentity::from_bytes([0x44; 32]),
            1,
            1,
            SchemaChangeActivationKind::UniqueIndex,
        )
        .expect("activation should admit");
        SchemaApplicationRecord::new(pending, vec![activation])
            .expect("pending record with activation should admit");

        let terminal = SchemaChangeReceipt::new(
            database,
            submission,
            digest,
            head,
            SchemaChangeOutcome::Failed {
                error: SchemaChangeFailure::from_error_code(
                    icydb_diagnostic_code::ErrorCode::RUNTIME_CONFLICT,
                    icydb_diagnostic_code::ErrorOrigin::Runtime,
                ),
            },
        )
        .expect("terminal receipt should admit");
        assert!(
            SchemaApplicationRecord::new(
                terminal,
                vec![
                    SchemaChangeActivation::new(
                        TargetStoreIdentity::from_bytes([0x44; 32]),
                        1,
                        1,
                        SchemaChangeActivationKind::Check,
                    )
                    .expect("activation should admit"),
                ],
            )
            .is_err(),
            "terminal records cannot retain activation state",
        );
    }

    #[test]
    fn schema_change_failure_rejects_unknown_or_mismatched_diagnostic_identity() {
        let valid = SchemaChangeFailure::from_error_code(
            icydb_diagnostic_code::ErrorCode::RUNTIME_CONFLICT,
            icydb_diagnostic_code::ErrorOrigin::Runtime,
        );
        valid.validate().expect("known diagnostic should admit");

        assert!(
            SchemaChangeFailure {
                code: u16::MAX,
                class: valid.class,
                origin: valid.origin,
            }
            .validate()
            .is_err(),
        );
        assert!(
            SchemaChangeFailure {
                code: valid.code,
                class: icydb_diagnostic_code::ErrorClass::Query.wire_code(),
                origin: valid.origin,
            }
            .validate()
            .is_err(),
        );
        assert!(
            SchemaChangeFailure {
                code: valid.code,
                class: valid.class,
                origin: u8::MAX,
            }
            .validate()
            .is_err(),
        );
    }

    #[test]
    fn schema_change_receipt_outcome_heads_have_exact_temporal_closure() {
        let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
        let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
        let prior = ExpectedAcceptedHead::Exact {
            revision: 7,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
        };
        let changed = ExpectedAcceptedHead::Exact {
            revision: 8,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        };

        SchemaChangeReceipt::new(
            database,
            SchemaSubmissionKey::try_new("noop").expect("submission key should admit"),
            digest,
            prior.clone(),
            SchemaChangeOutcome::NoOp {
                accepted_head: prior.clone(),
            },
        )
        .expect("no-op must retain the exact prior head");
        assert!(
            SchemaChangeReceipt::new(
                database,
                SchemaSubmissionKey::try_new("invalid-noop").expect("submission key should admit"),
                digest,
                prior.clone(),
                SchemaChangeOutcome::NoOp {
                    accepted_head: changed.clone(),
                },
            )
            .is_err(),
        );
        assert!(
            SchemaChangeReceipt::new(
                database,
                SchemaSubmissionKey::try_new("invalid-applied")
                    .expect("submission key should admit"),
                digest,
                prior.clone(),
                SchemaChangeOutcome::Applied {
                    accepted_head: prior.clone(),
                },
            )
            .is_err(),
        );
        SchemaChangeReceipt::new(
            database,
            SchemaSubmissionKey::try_new("applied").expect("submission key should admit"),
            digest,
            prior,
            SchemaChangeOutcome::Applied {
                accepted_head: changed,
            },
        )
        .expect("applied receipt must identify a different exact head");
    }
}
