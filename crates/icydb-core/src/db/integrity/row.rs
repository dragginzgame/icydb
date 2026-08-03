//! Module: db::integrity::row
//! Responsibility: bounded accepted-native physical row inspection.
//! Does not own: durable Deep jobs, proof-vector stability, or derived-state traversal.
//! Boundary: accepted inspection plan + raw data interval + private checkpoint -> bounded page.

use crate::{
    db::{
        Db,
        codec::MAX_ROW_BYTES,
        commit::database_incarnation_id,
        data::{
            DecodedDataStoreKey, RawDataStoreKey, RawRow, SlotReader, StoreVisit,
            StructuralSlotReader,
        },
        index::IndexEntryValue,
        integrity::{
            IntegrityEntityIdentity, IntegrityFinding, IntegrityFindingClass, IntegrityFindingKind,
            IntegrityPhase, IntegritySeverity, IntegrityVerifierFamily, relation_field_paths,
        },
        key_taxonomy::{PrimaryKeyComponent, RawDataStoreKeyRange},
        relation::RelationConstraintProjection,
        schema::{
            AcceptedFieldKind, AcceptedInspectionPlan, AcceptedRowConstraintEvaluationError,
            identity_kind_maximum,
        },
    },
    error::{ErrorClass, InternalError},
    traits::CanisterKind,
};
use candid::CandidType;
use serde::Deserialize;
use std::ops::Bound;

const MAX_ROW_INSPECTION_ROWS_PER_PAGE: usize = 32;
const MAX_ROW_INSPECTION_ATOMS_PER_PAGE: usize = 64;
const MAX_ROW_INSPECTION_FINDINGS_PER_PAGE: usize = 64;
const MAX_ROW_INSPECTION_BYTES_PER_PAGE: usize = MAX_ROW_BYTES as usize;

/// Private exact checkpoint within the canonical physical row interval.
///
/// Public callers never author this value. The durable Deep job owner added in
/// Patch 5 will encode and validate it inside the current job profile.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum PhysicalUnitCheckpoint {
    /// No physical key has been classified.
    BeforeFirst,

    /// One verifier atom completed, but the physical row is not complete.
    Within {
        physical_key: Vec<u8>,
        verifier_family: IntegrityVerifierFamily,
        ordinal: u32,
    },

    /// The named physical row is fully classified.
    After { physical_key: Vec<u8> },
}

impl PhysicalUnitCheckpoint {
    pub(super) fn raw_data_key(&self) -> Result<Option<RawDataStoreKey>, InternalError> {
        let bytes = match self {
            Self::BeforeFirst => return Ok(None),
            Self::Within { physical_key, .. } | Self::After { physical_key } => physical_key,
        };
        if bytes.len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE {
            return Err(InternalError::store_corruption());
        }

        Ok(Some(RawDataStoreKey::from_persisted_bytes(bytes.clone())))
    }

    pub(super) fn raw_index_key(
        &self,
    ) -> Result<Option<crate::db::index::RawIndexStoreKey>, InternalError> {
        let bytes = match self {
            Self::BeforeFirst => return Ok(None),
            Self::Within { physical_key, .. } | Self::After { physical_key } => physical_key,
        };
        if bytes.len() > crate::db::index::IndexKey::MAX_STORED_SIZE_USIZE {
            return Err(InternalError::store_corruption());
        }

        Ok(Some(
            crate::db::index::RawIndexStoreKey::from_persisted_bytes(bytes.clone()),
        ))
    }
}

/// Hard per-call bounds for the row inspection core.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct RowInspectionLimits {
    rows: usize,
    atoms: usize,
    findings: usize,
    decoded_bytes: usize,
}

impl RowInspectionLimits {
    /// Return the maintained production row-page bounds.
    #[must_use]
    pub(in crate::db) const fn standard() -> Self {
        Self {
            rows: MAX_ROW_INSPECTION_ROWS_PER_PAGE,
            atoms: MAX_ROW_INSPECTION_ATOMS_PER_PAGE,
            findings: MAX_ROW_INSPECTION_FINDINGS_PER_PAGE,
            decoded_bytes: MAX_ROW_INSPECTION_BYTES_PER_PAGE,
        }
    }

    fn validate(self) -> Result<Self, InternalError> {
        if self.rows == 0 || self.atoms == 0 || self.findings == 0 || self.decoded_bytes == 0 {
            return Err(InternalError::store_invariant());
        }

        Ok(self)
    }
}

/// One bounded page from the accepted physical row phase.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct RowIntegrityPage {
    checkpoint: PhysicalUnitCheckpoint,
    exhausted: bool,
    rows_started: u32,
    rows_completed: u32,
    atoms_classified: u32,
    decoded_bytes: u64,
    findings: Vec<IntegrityFinding>,
    blocked_verifier_families: Vec<IntegrityVerifierFamily>,
}

impl RowIntegrityPage {
    /// Borrow the private next checkpoint.
    #[must_use]
    pub(in crate::db) const fn checkpoint(&self) -> &PhysicalUnitCheckpoint {
        &self.checkpoint
    }

    /// Return whether the canonical entity interval was authoritatively exhausted.
    #[must_use]
    pub(in crate::db) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// Borrow the bounded finding page.
    #[must_use]
    pub(in crate::db) const fn findings(&self) -> &[IntegrityFinding] {
        self.findings.as_slice()
    }

    /// Borrow verifier families blocked by malformed prerequisite state.
    #[must_use]
    pub(in crate::db) const fn blocked_verifier_families(&self) -> &[IntegrityVerifierFamily] {
        self.blocked_verifier_families.as_slice()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowAtom {
    family: IntegrityVerifierFamily,
    ordinal: u32,
}

#[cfg_attr(
    not(target_arch = "wasm32"),
    expect(
        clippy::large_enum_variant,
        reason = "the finding stays stack-owned until direct page insertion; boxing would add one heap allocation per physical finding"
    )
)]
enum RowAtomOutcome {
    Clean,
    Finding(IntegrityFinding),
    Blocked(IntegrityVerifierFamily),
}

enum DecodedRowValues {
    Unknown,
    Invalid,
    Valid(Vec<Option<crate::value::Value>>),
}

struct RowPageAccumulator {
    checkpoint: PhysicalUnitCheckpoint,
    rows_started: usize,
    rows_completed: usize,
    atoms_classified: usize,
    decoded_bytes: usize,
    findings: Vec<IntegrityFinding>,
    blocked_verifier_families: Vec<IntegrityVerifierFamily>,
    stopped: bool,
}

impl RowPageAccumulator {
    const fn new(checkpoint: PhysicalUnitCheckpoint) -> Self {
        Self {
            checkpoint,
            rows_started: 0,
            rows_completed: 0,
            atoms_classified: 0,
            decoded_bytes: 0,
            findings: Vec::new(),
            blocked_verifier_families: Vec::new(),
            stopped: false,
        }
    }

    fn can_start_row(&self, row_bytes: usize, limits: RowInspectionLimits) -> bool {
        self.rows_started < limits.rows
            && (self.rows_started == 0
                || self
                    .decoded_bytes
                    .checked_add(row_bytes)
                    .is_some_and(|bytes| bytes <= limits.decoded_bytes))
    }

    const fn can_classify_atom(&self, limits: RowInspectionLimits) -> bool {
        self.atoms_classified < limits.atoms && self.findings.len() < limits.findings
    }

    fn record_blocked(&mut self, family: IntegrityVerifierFamily) {
        if !self.blocked_verifier_families.contains(&family) {
            self.blocked_verifier_families.push(family);
        }
    }

    fn finish(self, exhausted: bool) -> Result<RowIntegrityPage, InternalError> {
        Ok(RowIntegrityPage {
            checkpoint: self.checkpoint,
            exhausted,
            rows_started: u32::try_from(self.rows_started)
                .map_err(|_| InternalError::store_invariant())?,
            rows_completed: u32::try_from(self.rows_completed)
                .map_err(|_| InternalError::store_invariant())?,
            atoms_classified: u32::try_from(self.atoms_classified)
                .map_err(|_| InternalError::store_invariant())?,
            decoded_bytes: u64::try_from(self.decoded_bytes)
                .map_err(|_| InternalError::store_invariant())?,
            findings: self.findings,
            blocked_verifier_families: self.blocked_verifier_families,
        })
    }
}

/// Execute one bounded page of accepted-native row verification.
pub(in crate::db) fn execute_row_integrity_page<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    checkpoint: PhysicalUnitCheckpoint,
    limits: RowInspectionLimits,
) -> Result<RowIntegrityPage, InternalError> {
    let limits = limits.validate()?;
    let identity = plan.identity();
    let store = db.store_handle(identity.store_path())?;
    let identity_high_water = plan
        .identity_inspection()
        .map(|identity_inspection| {
            store.with_schema(|schema_store| {
                schema_store.identity_high_water_for_integrity(
                    database_incarnation_id()?,
                    identity.entity_tag(),
                    identity_inspection.field_id(),
                    identity_inspection.accepted_kind(),
                )
            })
        })
        .transpose()?;
    let relations = plan.relation_inspection();
    let range = RawDataStoreKeyRange::entity_prefix(identity.entity_tag());
    let checkpoint_key = checkpoint.raw_data_key()?;
    if checkpoint_key
        .as_ref()
        .is_some_and(|key| !raw_key_in_range(&range, key))
    {
        return Err(InternalError::store_corruption());
    }
    let lower = match (&checkpoint, checkpoint_key.as_ref()) {
        (PhysicalUnitCheckpoint::BeforeFirst, None) => {
            Bound::Included(RawDataStoreKey::store_range_lower_key(&range))
        }
        (PhysicalUnitCheckpoint::Within { .. }, Some(key)) => Bound::Included(key.clone()),
        (PhysicalUnitCheckpoint::After { .. }, Some(key)) => Bound::Excluded(key.clone()),
        _ => return Err(InternalError::store_corruption()),
    };
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let expected_within_key = matches!(checkpoint, PhysicalUnitCheckpoint::Within { .. })
        .then(|| checkpoint_key.clone())
        .flatten();
    let mut observed_within_key = expected_within_key.is_none();
    let mut page = RowPageAccumulator::new(checkpoint);

    store.with_data(|data| {
        data.visit_range((lower, upper), |raw_key, raw_row| {
            if let Some(expected) = expected_within_key.as_ref()
                && !observed_within_key
            {
                if raw_key != expected {
                    return Err(InternalError::store_corruption());
                }
                observed_within_key = true;
            }
            if !page.can_start_row(raw_row.len(), limits) {
                page.stopped = true;
                return Ok(StoreVisit::Stop);
            }
            page.rows_started = page
                .rows_started
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            page.decoded_bytes = page
                .decoded_bytes
                .checked_add(raw_row.len())
                .ok_or_else(InternalError::store_invariant)?;

            let start = start_atom_for_row(&page.checkpoint, raw_key, plan, relations.len())?;
            inspect_one_row(
                db,
                plan,
                relations,
                identity_high_water,
                raw_key,
                raw_row,
                start,
                limits,
                &mut page,
            )?;

            if page.stopped {
                Ok(StoreVisit::Stop)
            } else {
                Ok(StoreVisit::Continue)
            }
        })
    })?;

    if !observed_within_key {
        return Err(InternalError::store_corruption());
    }
    let exhausted = !page.stopped;
    page.finish(exhausted)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the row boundary keeps database, accepted authority, physical unit, bounds, and page owner explicit"
)]
fn inspect_one_row<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    relations: &[RelationConstraintProjection],
    identity_high_water: Option<u128>,
    raw_key: &RawDataStoreKey,
    raw_row: &RawRow,
    mut atom: Option<RowAtom>,
    limits: RowInspectionLimits,
    page: &mut RowPageAccumulator,
) -> Result<(), InternalError> {
    let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key);
    let oversized = raw_row.len() > MAX_ROW_BYTES as usize;
    let mut reader = if oversized {
        None
    } else {
        Some(StructuralSlotReader::from_raw_row_with_borrowed_contract(
            raw_row,
            plan.row_contract(),
        ))
    };
    let mut decoded_values = DecodedRowValues::Unknown;

    while let Some(current) = atom {
        if !page.can_classify_atom(limits) {
            page.stopped = true;
            return Ok(());
        }

        let outcome = inspect_row_atom(
            db,
            plan,
            relations,
            raw_key,
            raw_row,
            &decoded_key,
            &mut reader,
            &mut decoded_values,
            identity_high_water,
            current,
        )?;
        page.atoms_classified = page
            .atoms_classified
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;
        match outcome {
            RowAtomOutcome::Clean => {}
            RowAtomOutcome::Finding(finding) => page.findings.push(finding),
            RowAtomOutcome::Blocked(family) => page.record_blocked(family),
        }
        page.checkpoint = PhysicalUnitCheckpoint::Within {
            physical_key: bounded_physical_key(raw_key)?,
            verifier_family: current.family,
            ordinal: current.ordinal,
        };

        if current.family == IntegrityVerifierFamily::RowEnvelope
            && (oversized || reader.is_none() || reader.as_ref().is_some_and(Result::is_err))
        {
            for family in [
                IntegrityVerifierFamily::FieldValue,
                IntegrityVerifierFamily::PrimaryKey,
                IntegrityVerifierFamily::ValidatedConstraints,
                IntegrityVerifierFamily::ForwardIndex,
                IntegrityVerifierFamily::Relation,
            ] {
                page.record_blocked(family);
            }
            atom = None;
        } else {
            atom = next_row_atom(current, plan, relations.len())?;
        }
    }

    page.checkpoint = PhysicalUnitCheckpoint::After {
        physical_key: bounded_physical_key(raw_key)?,
    };
    page.rows_completed = page
        .rows_completed
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;

    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one exhaustive verifier-family dispatch keeps checkpoint vocabulary and physical classification visibly aligned"
)]
fn inspect_row_atom<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    relations: &[RelationConstraintProjection],
    raw_key: &RawDataStoreKey,
    raw_row: &RawRow,
    decoded_key: &Result<DecodedDataStoreKey, crate::db::data::DecodedDataStoreKeyDecodeError>,
    reader: &mut Option<Result<StructuralSlotReader<'_>, InternalError>>,
    decoded_values: &mut DecodedRowValues,
    identity_high_water: Option<u128>,
    atom: RowAtom,
) -> Result<RowAtomOutcome, InternalError> {
    match atom.family {
        IntegrityVerifierFamily::DataKey => match decoded_key {
            Ok(key) if key.entity_tag() == plan.identity().entity_tag() => {
                Ok(RowAtomOutcome::Clean)
            }
            Ok(_) | Err(_) => Ok(RowAtomOutcome::Finding(row_finding(
                plan,
                raw_key,
                decoded_key.as_ref().ok(),
                IntegrityVerifierFamily::DataKey,
                IntegrityFindingKind::MalformedDataKey,
                icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
            )?)),
        },
        IntegrityVerifierFamily::IdentityState => {
            let (Some(identity), Some(high_water)) =
                (plan.identity_inspection(), identity_high_water)
            else {
                return Err(InternalError::store_invariant());
            };
            let Some(key) = decoded_key.as_ref().ok() else {
                return Ok(RowAtomOutcome::Blocked(
                    IntegrityVerifierFamily::IdentityState,
                ));
            };
            inspect_identity_key(plan, identity, key, raw_key, high_water)
        }
        IntegrityVerifierFamily::RowEnvelope => {
            if raw_row.len() > MAX_ROW_BYTES as usize {
                return Ok(RowAtomOutcome::Finding(row_finding(
                    plan,
                    raw_key,
                    decoded_key.as_ref().ok(),
                    IntegrityVerifierFamily::RowEnvelope,
                    IntegrityFindingKind::OversizedRow,
                    icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
                )?));
            }
            if reader.as_ref().is_some_and(Result::is_ok) {
                return Ok(RowAtomOutcome::Clean);
            }
            let Some(Err(error)) = reader.take() else {
                return Err(InternalError::store_invariant());
            };
            physical_error_finding(
                plan,
                raw_key,
                decoded_key.as_ref().ok(),
                IntegrityVerifierFamily::RowEnvelope,
                IntegrityFindingKind::MalformedRow,
                Vec::new(),
                error,
            )
        }
        IntegrityVerifierFamily::FieldValue => {
            let slot =
                usize::try_from(atom.ordinal).map_err(|_| InternalError::store_invariant())?;
            let Some(Ok(reader)) = reader.as_mut() else {
                return Ok(RowAtomOutcome::Blocked(IntegrityVerifierFamily::FieldValue));
            };
            if !plan.row_contract().has_active_field_slot(slot) {
                return Ok(RowAtomOutcome::Clean);
            }
            match reader.get_value(slot) {
                Ok(_) => Ok(RowAtomOutcome::Clean),
                Err(error) => {
                    *decoded_values = DecodedRowValues::Invalid;
                    physical_error_finding(
                        plan,
                        raw_key,
                        decoded_key.as_ref().ok(),
                        IntegrityVerifierFamily::FieldValue,
                        IntegrityFindingKind::InvalidFieldValue,
                        vec![plan.row_contract().field_name(slot)?.to_string()],
                        error,
                    )
                }
            }
        }
        IntegrityVerifierFamily::PrimaryKey => {
            let (Some(key), Some(Ok(reader))) = (decoded_key.as_ref().ok(), reader.as_mut()) else {
                return Ok(RowAtomOutcome::Blocked(IntegrityVerifierFamily::PrimaryKey));
            };
            match reader.validate_primary_key(key) {
                Ok(()) => Ok(RowAtomOutcome::Clean),
                Err(error) => physical_error_finding(
                    plan,
                    raw_key,
                    Some(key),
                    IntegrityVerifierFamily::PrimaryKey,
                    IntegrityFindingKind::PrimaryKeyMismatch,
                    field_paths_for_slots(plan, plan.row_contract().primary_key_slot_indices())?,
                    error,
                ),
            }
        }
        IntegrityVerifierFamily::ValidatedConstraints => {
            let Some(Ok(reader)) = reader.as_mut() else {
                return Ok(RowAtomOutcome::Blocked(
                    IntegrityVerifierFamily::ValidatedConstraints,
                ));
            };
            let Some(values) = decode_all_fields(reader, plan, decoded_values)? else {
                return Ok(RowAtomOutcome::Blocked(
                    IntegrityVerifierFamily::ValidatedConstraints,
                ));
            };
            let ordinal =
                usize::try_from(atom.ordinal).map_err(|_| InternalError::store_invariant())?;
            match plan.write_constraints().evaluate_integrity_constraint(
                ordinal,
                plan.identity().accepted_schema_fingerprint(),
                values,
            ) {
                Ok(()) => Ok(RowAtomOutcome::Clean),
                Err(AcceptedRowConstraintEvaluationError::Violation {
                    constraint_id,
                    constraint_name,
                    field_paths,
                    ..
                }) => Ok(RowAtomOutcome::Finding(IntegrityFinding {
                    diagnostic_code:
                        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
                            .raw(),
                    class: IntegrityFindingClass::Corruption,
                    severity: IntegritySeverity::Error,
                    kind: IntegrityFindingKind::ConstraintViolation,
                    entity: IntegrityEntityIdentity::from_plan(plan),
                    store_path: plan.identity().store_path().to_string(),
                    phase: IntegrityPhase::Rows,
                    verifier_family: IntegrityVerifierFamily::ValidatedConstraints,
                    physical_key: bounded_physical_key(raw_key)?,
                    primary_key: primary_key_bytes(decoded_key.as_ref().ok(), raw_key),
                    field_paths,
                    value_path: None,
                    constraint_id: Some(constraint_id.get()),
                    constraint_name: Some(constraint_name),
                    schema_index_id: None,
                    relation_id: None,
                    expected: Some("true_or_unknown".to_string()),
                    observed: Some("false".to_string()),
                })),
                Err(AcceptedRowConstraintEvaluationError::TargetedRuleViolation {
                    constraint_id,
                    constraint_name,
                    field_path,
                    path,
                }) => Ok(RowAtomOutcome::Finding(IntegrityFinding {
                    diagnostic_code:
                        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
                            .raw(),
                    class: IntegrityFindingClass::Corruption,
                    severity: IntegritySeverity::Error,
                    kind: IntegrityFindingKind::ConstraintViolation,
                    entity: IntegrityEntityIdentity::from_plan(plan),
                    store_path: plan.identity().store_path().to_string(),
                    phase: IntegrityPhase::Rows,
                    verifier_family: IntegrityVerifierFamily::ValidatedConstraints,
                    physical_key: bounded_physical_key(raw_key)?,
                    primary_key: primary_key_bytes(decoded_key.as_ref().ok(), raw_key),
                    field_paths: vec![field_path],
                    value_path: Some(Box::new(path.into_constraint_value_path())),
                    constraint_id: Some(constraint_id.get()),
                    constraint_name: Some(constraint_name),
                    schema_index_id: None,
                    relation_id: None,
                    expected: Some("targeted_rule_satisfied".to_string()),
                    observed: Some("targeted_rule_violated".to_string()),
                })),
                Err(
                    AcceptedRowConstraintEvaluationError::InvalidExpression(_)
                    | AcceptedRowConstraintEvaluationError::LiteralCorrupt
                    | AcceptedRowConstraintEvaluationError::FingerprintMismatch
                    | AcceptedRowConstraintEvaluationError::MissingSlot
                    | AcceptedRowConstraintEvaluationError::RuntimeValueMismatch
                    | AcceptedRowConstraintEvaluationError::WorkBudgetExceeded
                    | AcceptedRowConstraintEvaluationError::ValueDepthExceeded
                    | AcceptedRowConstraintEvaluationError::ValueNodeBudgetExceeded
                    | AcceptedRowConstraintEvaluationError::OperationBudgetExceeded
                    | AcceptedRowConstraintEvaluationError::PathBudgetExceeded,
                ) => Err(InternalError::accepted_row_constraint_program_corrupt()),
            }
        }
        IntegrityVerifierFamily::ForwardIndex => {
            let (Some(key), Some(Ok(reader))) = (decoded_key.as_ref().ok(), reader.as_mut()) else {
                return Ok(RowAtomOutcome::Blocked(
                    IntegrityVerifierFamily::ForwardIndex,
                ));
            };
            if decode_all_fields(reader, plan, decoded_values)?.is_none() {
                return Ok(RowAtomOutcome::Blocked(
                    IntegrityVerifierFamily::ForwardIndex,
                ));
            }
            let ordinal =
                usize::try_from(atom.ordinal).map_err(|_| InternalError::store_invariant())?;
            let Some(witness) = plan.index_inspection().project(
                ordinal,
                key.entity_tag(),
                &key.primary_key_value(),
                reader,
            )?
            else {
                return Ok(RowAtomOutcome::Clean);
            };
            let index_store = db.store_handle(witness.store_path())?;
            let actual = index_store.with_index(|store| store.get(witness.raw_key()));
            match actual {
                None => Ok(RowAtomOutcome::Finding(index_finding(
                    plan,
                    raw_key,
                    key,
                    witness.schema_index_id().get(),
                    IntegrityFindingKind::MissingIndexEntry,
                    "missing",
                )?)),
                Some(value) if value == IndexEntryValue::presence() => Ok(RowAtomOutcome::Clean),
                Some(_) => Ok(RowAtomOutcome::Finding(index_finding(
                    plan,
                    raw_key,
                    key,
                    witness.schema_index_id().get(),
                    IntegrityFindingKind::DivergentIndexEntry,
                    "divergent",
                )?)),
            }
        }
        IntegrityVerifierFamily::Relation => {
            let (Some(key), Some(Ok(reader))) = (decoded_key.as_ref().ok(), reader.as_mut()) else {
                return Ok(RowAtomOutcome::Blocked(IntegrityVerifierFamily::Relation));
            };
            if decode_all_fields(reader, plan, decoded_values)?.is_none() {
                return Ok(RowAtomOutcome::Blocked(IntegrityVerifierFamily::Relation));
            }
            let ordinal =
                usize::try_from(atom.ordinal).map_err(|_| InternalError::store_invariant())?;
            let projection = relations
                .get(ordinal)
                .ok_or_else(InternalError::store_invariant)?;
            let projected = projection.project_row(&key.primary_key_value(), reader, true)?;
            if !projected.missing_targets().is_empty() {
                return Ok(RowAtomOutcome::Finding(relation_finding(
                    plan,
                    raw_key,
                    key,
                    projection.relation_id().get(),
                    IntegrityFindingKind::MissingRelationTarget,
                    "missing_target",
                )?));
            }
            for entry in projected.entries() {
                let actual = entry
                    .target_store()
                    .with_index(|store| store.get(entry.key()));
                match actual {
                    None => {
                        return Ok(RowAtomOutcome::Finding(relation_finding(
                            plan,
                            raw_key,
                            key,
                            projection.relation_id().get(),
                            IntegrityFindingKind::MissingReverseRelationEntry,
                            "missing",
                        )?));
                    }
                    Some(value) if value == IndexEntryValue::presence() => {}
                    Some(_) => {
                        return Ok(RowAtomOutcome::Finding(relation_finding(
                            plan,
                            raw_key,
                            key,
                            projection.relation_id().get(),
                            IntegrityFindingKind::DivergentReverseRelationEntry,
                            "divergent",
                        )?));
                    }
                }
            }
            Ok(RowAtomOutcome::Clean)
        }
        IntegrityVerifierFamily::IndexEntry
        | IntegrityVerifierFamily::UniqueIndex
        | IntegrityVerifierFamily::ReverseRelationEntry
        | IntegrityVerifierFamily::JournalEnvelope
        | IntegrityVerifierFamily::JournalBatchIdentity => Err(InternalError::store_corruption()),
    }
}

fn decode_all_fields<'a>(
    reader: &mut StructuralSlotReader<'_>,
    plan: &AcceptedInspectionPlan,
    decoded_values: &'a mut DecodedRowValues,
) -> Result<Option<&'a [Option<crate::value::Value>]>, InternalError> {
    match decoded_values {
        DecodedRowValues::Invalid => return Ok(None),
        DecodedRowValues::Valid(values) => return Ok(Some(values.as_slice())),
        DecodedRowValues::Unknown => {}
    }
    let mut values = Vec::with_capacity(plan.row_contract().field_count());
    for slot in 0..plan.row_contract().field_count() {
        if !plan.row_contract().has_active_field_slot(slot) {
            values.push(None);
            continue;
        }
        match reader.get_value(slot) {
            Ok(value) => values.push(value),
            Err(error)
                if matches!(
                    error.class(),
                    ErrorClass::Corruption | ErrorClass::IncompatiblePersistedFormat
                ) =>
            {
                *decoded_values = DecodedRowValues::Invalid;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
    }
    *decoded_values = DecodedRowValues::Valid(values);

    match decoded_values {
        DecodedRowValues::Valid(values) => Ok(Some(values.as_slice())),
        DecodedRowValues::Unknown | DecodedRowValues::Invalid => {
            Err(InternalError::store_invariant())
        }
    }
}

fn inspect_identity_key(
    plan: &AcceptedInspectionPlan,
    identity: &crate::db::schema::AcceptedIdentityInspection,
    decoded_key: &DecodedDataStoreKey,
    raw_key: &RawDataStoreKey,
    committed_high_water: u128,
) -> Result<RowAtomOutcome, InternalError> {
    let (classification, value) = classify_identity_scalar(
        identity.accepted_kind(),
        decoded_key.primary_key_value().scalar_component(),
        committed_high_water,
    )?;
    if classification == IdentityScalarIntegrity::Clean {
        return Ok(RowAtomOutcome::Clean);
    }
    let kind = match classification {
        IdentityScalarIntegrity::Clean => return Err(InternalError::store_invariant()),
        IdentityScalarIntegrity::Invalid => IntegrityFindingKind::InvalidIdentityValue,
        IdentityScalarIntegrity::HighWaterExceeded => {
            IntegrityFindingKind::IdentityHighWaterExceeded
        }
    };
    let observed = value.map_or_else(
        || "wrong_unsigned_key_shape".to_string(),
        |value| value.to_string(),
    );
    let error = InternalError::identity_state_corruption();
    Ok(RowAtomOutcome::Finding(IntegrityFinding {
        diagnostic_code: error.diagnostic_code().error_code().raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: IntegrityVerifierFamily::IdentityState,
        physical_key: bounded_physical_key(raw_key)?,
        primary_key: primary_key_bytes(Some(decoded_key), raw_key),
        field_paths: vec![identity.field_name().to_string()],
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: Some(format!("1..={committed_high_water}")),
        observed: Some(observed),
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdentityScalarIntegrity {
    Clean,
    Invalid,
    HighWaterExceeded,
}

fn classify_identity_scalar(
    accepted_kind: &AcceptedFieldKind,
    component: Option<PrimaryKeyComponent>,
    committed_high_water: u128,
) -> Result<(IdentityScalarIntegrity, Option<u128>), InternalError> {
    let value = match (accepted_kind, component) {
        (
            AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64,
            Some(PrimaryKeyComponent::Nat64(value)),
        ) => Some(u128::from(value)),
        (AcceptedFieldKind::Nat128, Some(PrimaryKeyComponent::Nat128(value))) => Some(value),
        _ => None,
    };
    let maximum = identity_kind_maximum(accepted_kind)
        .ok_or_else(InternalError::identity_state_corruption)?;
    let classification = match value {
        Some(value) if value > 0 && value <= maximum && value <= committed_high_water => {
            IdentityScalarIntegrity::Clean
        }
        Some(value) if value > 0 && value <= maximum => IdentityScalarIntegrity::HighWaterExceeded,
        Some(_) | None => IdentityScalarIntegrity::Invalid,
    };
    Ok((classification, value))
}

fn start_atom_for_row(
    checkpoint: &PhysicalUnitCheckpoint,
    raw_key: &RawDataStoreKey,
    plan: &AcceptedInspectionPlan,
    relation_count: usize,
) -> Result<Option<RowAtom>, InternalError> {
    match checkpoint {
        PhysicalUnitCheckpoint::Within {
            physical_key,
            verifier_family,
            ordinal,
        } if physical_key.as_slice() == raw_key.as_bytes() => next_row_atom(
            RowAtom {
                family: *verifier_family,
                ordinal: *ordinal,
            },
            plan,
            relation_count,
        ),
        PhysicalUnitCheckpoint::Within { .. } => Err(InternalError::store_corruption()),
        PhysicalUnitCheckpoint::BeforeFirst | PhysicalUnitCheckpoint::After { .. } => {
            Ok(Some(RowAtom {
                family: IntegrityVerifierFamily::DataKey,
                ordinal: 0,
            }))
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive transition table keeps every verifier-family and ordinal handoff fail-closed"
)]
fn next_row_atom(
    atom: RowAtom,
    plan: &AcceptedInspectionPlan,
    relation_count: usize,
) -> Result<Option<RowAtom>, InternalError> {
    let next = match atom.family {
        IntegrityVerifierFamily::DataKey if atom.ordinal == 0 => {
            if plan.identity_inspection().is_some() {
                RowAtom {
                    family: IntegrityVerifierFamily::IdentityState,
                    ordinal: 0,
                }
            } else {
                RowAtom {
                    family: IntegrityVerifierFamily::RowEnvelope,
                    ordinal: 0,
                }
            }
        }
        IntegrityVerifierFamily::IdentityState
            if atom.ordinal == 0 && plan.identity_inspection().is_some() =>
        {
            RowAtom {
                family: IntegrityVerifierFamily::RowEnvelope,
                ordinal: 0,
            }
        }
        IntegrityVerifierFamily::RowEnvelope if atom.ordinal == 0 => {
            if plan.row_contract().field_count() == 0 {
                RowAtom {
                    family: IntegrityVerifierFamily::PrimaryKey,
                    ordinal: 0,
                }
            } else {
                RowAtom {
                    family: IntegrityVerifierFamily::FieldValue,
                    ordinal: 0,
                }
            }
        }
        IntegrityVerifierFamily::FieldValue => {
            let next = atom
                .ordinal
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            if usize::try_from(next).is_ok_and(|next| next < plan.row_contract().field_count()) {
                RowAtom {
                    family: IntegrityVerifierFamily::FieldValue,
                    ordinal: next,
                }
            } else {
                RowAtom {
                    family: IntegrityVerifierFamily::PrimaryKey,
                    ordinal: 0,
                }
            }
        }
        IntegrityVerifierFamily::PrimaryKey if atom.ordinal == 0 => RowAtom {
            family: if plan.write_constraints().integrity_constraint_count() == 0 {
                if plan.index_inspection().len() == 0 {
                    if relation_count == 0 {
                        return Ok(None);
                    }
                    IntegrityVerifierFamily::Relation
                } else {
                    IntegrityVerifierFamily::ForwardIndex
                }
            } else {
                IntegrityVerifierFamily::ValidatedConstraints
            },
            ordinal: 0,
        },
        IntegrityVerifierFamily::ValidatedConstraints => {
            let next = atom
                .ordinal
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            if usize::try_from(next)
                .is_ok_and(|next| next < plan.write_constraints().integrity_constraint_count())
            {
                RowAtom {
                    family: IntegrityVerifierFamily::ValidatedConstraints,
                    ordinal: next,
                }
            } else if plan.index_inspection().len() == 0 {
                if relation_count == 0 {
                    return Ok(None);
                }
                RowAtom {
                    family: IntegrityVerifierFamily::Relation,
                    ordinal: 0,
                }
            } else {
                RowAtom {
                    family: IntegrityVerifierFamily::ForwardIndex,
                    ordinal: 0,
                }
            }
        }
        IntegrityVerifierFamily::ForwardIndex => {
            let next = atom
                .ordinal
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            if usize::try_from(next).is_ok_and(|next| next < plan.index_inspection().len()) {
                RowAtom {
                    family: IntegrityVerifierFamily::ForwardIndex,
                    ordinal: next,
                }
            } else if relation_count == 0 {
                return Ok(None);
            } else {
                RowAtom {
                    family: IntegrityVerifierFamily::Relation,
                    ordinal: 0,
                }
            }
        }
        IntegrityVerifierFamily::Relation => {
            let next = atom
                .ordinal
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            if usize::try_from(next).is_ok_and(|next| next < relation_count) {
                RowAtom {
                    family: IntegrityVerifierFamily::Relation,
                    ordinal: next,
                }
            } else {
                return Ok(None);
            }
        }
        IntegrityVerifierFamily::RowEnvelope
        | IntegrityVerifierFamily::PrimaryKey
        | IntegrityVerifierFamily::IdentityState
        | IntegrityVerifierFamily::IndexEntry
        | IntegrityVerifierFamily::UniqueIndex
        | IntegrityVerifierFamily::ReverseRelationEntry
        | IntegrityVerifierFamily::JournalEnvelope
        | IntegrityVerifierFamily::JournalBatchIdentity => {
            return Err(InternalError::store_corruption());
        }
        IntegrityVerifierFamily::DataKey => return Err(InternalError::store_corruption()),
    };

    Ok(Some(next))
}

fn physical_error_finding(
    plan: &AcceptedInspectionPlan,
    raw_key: &RawDataStoreKey,
    decoded_key: Option<&DecodedDataStoreKey>,
    family: IntegrityVerifierFamily,
    kind: IntegrityFindingKind,
    field_paths: Vec<String>,
    error: InternalError,
) -> Result<RowAtomOutcome, InternalError> {
    let class = match error.class() {
        ErrorClass::Corruption => IntegrityFindingClass::Corruption,
        ErrorClass::IncompatiblePersistedFormat => {
            IntegrityFindingClass::IncompatiblePersistedFormat
        }
        ErrorClass::Conflict
        | ErrorClass::Internal
        | ErrorClass::InvariantViolation
        | ErrorClass::NotFound
        | ErrorClass::Unsupported => return Err(error),
    };
    Ok(RowAtomOutcome::Finding(IntegrityFinding {
        diagnostic_code: error.diagnostic_code().error_code().raw(),
        class,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: family,
        physical_key: bounded_physical_key(raw_key)?,
        primary_key: primary_key_bytes(decoded_key, raw_key),
        field_paths,
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: None,
        observed: None,
    }))
}

fn row_finding(
    plan: &AcceptedInspectionPlan,
    raw_key: &RawDataStoreKey,
    decoded_key: Option<&DecodedDataStoreKey>,
    family: IntegrityVerifierFamily,
    kind: IntegrityFindingKind,
    diagnostic_code: u16,
) -> Result<IntegrityFinding, InternalError> {
    Ok(IntegrityFinding {
        diagnostic_code,
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: family,
        physical_key: bounded_physical_key(raw_key)?,
        primary_key: primary_key_bytes(decoded_key, raw_key),
        field_paths: Vec::new(),
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: None,
        observed: None,
    })
}

fn index_finding(
    plan: &AcceptedInspectionPlan,
    raw_key: &RawDataStoreKey,
    decoded_key: &DecodedDataStoreKey,
    schema_index_id: u32,
    kind: IntegrityFindingKind,
    observed: &str,
) -> Result<IntegrityFinding, InternalError> {
    Ok(IntegrityFinding {
        diagnostic_code: icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: IntegrityVerifierFamily::ForwardIndex,
        physical_key: bounded_physical_key(raw_key)?,
        primary_key: primary_key_bytes(Some(decoded_key), raw_key),
        field_paths: Vec::new(),
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: Some(schema_index_id),
        relation_id: None,
        expected: Some("present".to_string()),
        observed: Some(observed.to_string()),
    })
}

fn relation_finding(
    plan: &AcceptedInspectionPlan,
    raw_key: &RawDataStoreKey,
    decoded_key: &DecodedDataStoreKey,
    relation_id: u32,
    kind: IntegrityFindingKind,
    observed: &str,
) -> Result<IntegrityFinding, InternalError> {
    let field_paths = relation_field_paths(plan, relation_id);
    Ok(IntegrityFinding {
        diagnostic_code: icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: IntegrityVerifierFamily::Relation,
        physical_key: bounded_physical_key(raw_key)?,
        primary_key: primary_key_bytes(Some(decoded_key), raw_key),
        field_paths,
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: Some(relation_id),
        expected: Some("present".to_string()),
        observed: Some(observed.to_string()),
    })
}

fn field_paths_for_slots(
    plan: &AcceptedInspectionPlan,
    slots: &[usize],
) -> Result<Vec<String>, InternalError> {
    slots
        .iter()
        .map(|slot| {
            plan.row_contract()
                .field_name(*slot)
                .map(ToString::to_string)
        })
        .collect()
}

fn bounded_physical_key(raw_key: &RawDataStoreKey) -> Result<Vec<u8>, InternalError> {
    if raw_key.as_bytes().len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE {
        return Err(InternalError::store_corruption());
    }

    Ok(raw_key.as_bytes().to_vec())
}

fn primary_key_bytes(
    decoded_key: Option<&DecodedDataStoreKey>,
    raw_key: &RawDataStoreKey,
) -> Option<Vec<u8>> {
    decoded_key.and_then(|_| raw_key.encoded_primary_key_bytes().map(<[u8]>::to_vec))
}

fn raw_key_in_range(range: &RawDataStoreKeyRange, key: &RawDataStoreKey) -> bool {
    key.as_bytes() >= range.lower_inclusive()
        && range
            .upper_exclusive()
            .is_none_or(|upper| key.as_bytes() < upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_row_classification_precedes_generic_row_checks() {
        assert_eq!(
            classify_identity_scalar(
                &AcceptedFieldKind::Nat64,
                Some(PrimaryKeyComponent::Nat64(9)),
                8,
            )
            .expect("Nat64 identity should classify"),
            (
                IdentityScalarIntegrity::HighWaterExceeded,
                Some(u128::from(9_u8)),
            ),
        );
        assert_eq!(
            classify_identity_scalar(
                &AcceptedFieldKind::Nat8,
                Some(PrimaryKeyComponent::Nat64(0)),
                8,
            )
            .expect("zero identity should classify"),
            (IdentityScalarIntegrity::Invalid, Some(0)),
        );
        assert_eq!(
            classify_identity_scalar(
                &AcceptedFieldKind::Nat128,
                Some(PrimaryKeyComponent::Nat128(8)),
                8,
            )
            .expect("Nat128 identity should classify"),
            (IdentityScalarIntegrity::Clean, Some(8)),
        );
        assert_eq!(
            classify_identity_scalar(
                &AcceptedFieldKind::Nat128,
                Some(PrimaryKeyComponent::Nat64(8)),
                8,
            )
            .expect("wrong key shape should classify"),
            (IdentityScalarIntegrity::Invalid, None),
        );
    }
}
