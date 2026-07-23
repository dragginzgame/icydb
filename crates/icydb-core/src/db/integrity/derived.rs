//! Module: db::integrity::derived
//! Responsibility: bounded physical verification of active accepted derived state.
//! Does not own: row point checks, activation staging, retired generations, or durable jobs.
//! Boundary: accepted inspection plan + exact active domain + private checkpoint -> bounded page.

use crate::{
    db::{
        Db,
        data::{DecodedDataStoreKey, StructuralSlotReader},
        direction::Direction,
        index::{AcceptedIndexInspectionDomain, IndexEntryValue, IndexKey, RawIndexStoreKey},
        integrity::{
            IntegrityEntityIdentity, IntegrityFinding, IntegrityFindingClass, IntegrityFindingKind,
            IntegrityPhase, IntegritySeverity, IntegrityVerifierFamily, PhysicalUnitCheckpoint,
            accepted_relation_projections, relation_field_paths,
        },
        relation::RelationConstraintProjection,
        schema::{AcceptedInspectionPlan, PersistedIndexSnapshot},
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::ops::Bound;

const MAX_DERIVED_ENTRIES_PER_PAGE: usize = 32;
const MAX_DERIVED_ATOMS_PER_PAGE: usize = 64;
const MAX_DERIVED_FINDINGS_PER_PAGE: usize = 64;
const MAX_DERIVED_DECODED_BYTES_PER_PAGE: usize =
    crate::db::codec::MAX_ROW_BYTES as usize + (64 * 1024);

/// Hard per-call bounds for one active derived-state page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct DerivedInspectionLimits {
    entries: usize,
    atoms: usize,
    findings: usize,
    decoded_bytes: usize,
}

impl DerivedInspectionLimits {
    /// Return the maintained production derived-state page bounds.
    #[must_use]
    pub(in crate::db) const fn standard() -> Self {
        Self {
            entries: MAX_DERIVED_ENTRIES_PER_PAGE,
            atoms: MAX_DERIVED_ATOMS_PER_PAGE,
            findings: MAX_DERIVED_FINDINGS_PER_PAGE,
            decoded_bytes: MAX_DERIVED_DECODED_BYTES_PER_PAGE,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn for_tests(
        entries: usize,
        atoms: usize,
        findings: usize,
        decoded_bytes: usize,
    ) -> Self {
        Self {
            entries,
            atoms,
            findings,
            decoded_bytes,
        }
    }

    fn validate(self) -> Result<Self, InternalError> {
        if self.entries == 0 || self.atoms == 0 || self.findings == 0 || self.decoded_bytes == 0 {
            return Err(InternalError::store_invariant());
        }

        Ok(self)
    }
}

/// One bounded page from an active forward-index or reverse-relation domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct DerivedIntegrityPage {
    checkpoint: PhysicalUnitCheckpoint,
    exhausted: bool,
    entries_started: u32,
    entries_completed: u32,
    atoms_classified: u32,
    decoded_bytes: u64,
    findings: Vec<IntegrityFinding>,
}

impl DerivedIntegrityPage {
    /// Borrow the private next checkpoint.
    #[must_use]
    pub(in crate::db) const fn checkpoint(&self) -> &PhysicalUnitCheckpoint {
        &self.checkpoint
    }

    /// Return whether the selected active physical domain was exhausted.
    #[must_use]
    pub(in crate::db) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// Return physical entries fully classified during this call.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn entries_completed(&self) -> u32 {
        self.entries_completed
    }

    /// Borrow the bounded finding page.
    #[must_use]
    pub(in crate::db) const fn findings(&self) -> &[IntegrityFinding] {
        self.findings.as_slice()
    }
}

struct DerivedPageAccumulator {
    checkpoint: PhysicalUnitCheckpoint,
    entries_started: usize,
    entries_completed: usize,
    atoms_classified: usize,
    decoded_bytes: usize,
    findings: Vec<IntegrityFinding>,
    stopped: bool,
}

impl DerivedPageAccumulator {
    const fn new(checkpoint: PhysicalUnitCheckpoint) -> Self {
        Self {
            checkpoint,
            entries_started: 0,
            entries_completed: 0,
            atoms_classified: 0,
            decoded_bytes: 0,
            findings: Vec::new(),
            stopped: false,
        }
    }

    fn can_start_entry(
        &self,
        raw_key: &RawIndexStoreKey,
        value: &IndexEntryValue,
        limits: DerivedInspectionLimits,
    ) -> bool {
        self.entries_started < limits.entries
            && self
                .decoded_bytes
                .checked_add(raw_key.as_bytes().len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .and_then(|bytes| bytes.checked_add(crate::db::codec::MAX_ROW_BYTES as usize))
                .is_some_and(|bytes| bytes <= limits.decoded_bytes)
    }

    const fn can_classify_atom(&self, limits: DerivedInspectionLimits) -> bool {
        self.atoms_classified < limits.atoms && self.findings.len() < limits.findings
    }

    fn consume_source_row(
        &mut self,
        row_len: usize,
        limits: DerivedInspectionLimits,
    ) -> Result<bool, InternalError> {
        let Some(next) = self.decoded_bytes.checked_add(row_len) else {
            return Err(InternalError::store_invariant());
        };
        if next > limits.decoded_bytes {
            self.stopped = true;
            return Ok(false);
        }
        self.decoded_bytes = next;
        Ok(true)
    }

    fn finish(self, exhausted: bool) -> Result<DerivedIntegrityPage, InternalError> {
        Ok(DerivedIntegrityPage {
            checkpoint: self.checkpoint,
            exhausted,
            entries_started: u32::try_from(self.entries_started)
                .map_err(|_| InternalError::store_invariant())?,
            entries_completed: u32::try_from(self.entries_completed)
                .map_err(|_| InternalError::store_invariant())?,
            atoms_classified: u32::try_from(self.atoms_classified)
                .map_err(|_| InternalError::store_invariant())?,
            decoded_bytes: u64::try_from(self.decoded_bytes)
                .map_err(|_| InternalError::store_invariant())?,
            findings: self.findings,
        })
    }
}

/// Execute one bounded page over one active accepted forward-index generation.
pub(in crate::db) fn execute_index_integrity_page<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    index_ordinal: usize,
    checkpoint: PhysicalUnitCheckpoint,
    limits: DerivedInspectionLimits,
) -> Result<DerivedIntegrityPage, InternalError> {
    let limits = limits.validate()?;
    let identity = plan.identity();
    let domain = plan
        .index_inspection()
        .domain(index_ordinal, identity.entity_tag())?;
    let index_store = db.recovered_store(domain.store_path())?;
    let source_store = db.recovered_store(identity.store_path())?;
    let bounds = domain.raw_bounds()?;
    let checkpoint_key = checkpoint.raw_index_key()?;
    validate_checkpoint_in_bounds(checkpoint_key.as_ref(), &bounds)?;
    let lower = resume_lower_bound(&checkpoint, checkpoint_key.as_ref(), &bounds.0)?;
    let expected_within_key = matches!(checkpoint, PhysicalUnitCheckpoint::Within { .. })
        .then(|| checkpoint_key.clone())
        .flatten();
    let mut observed_within_key = expected_within_key.is_none();
    let mut page = DerivedPageAccumulator::new(checkpoint);

    index_store.with_index(|store| {
        store.visit_raw_entries_in_range(
            (&lower, &bounds.1),
            Direction::Asc,
            |raw_key, raw_value| {
                if let Some(expected) = expected_within_key.as_ref()
                    && !observed_within_key
                {
                    if raw_key != expected {
                        return Err(InternalError::store_corruption());
                    }
                    observed_within_key = true;
                }
                if !page.can_start_entry(raw_key, raw_value, limits) {
                    page.stopped = true;
                    return Ok(true);
                }
                page.entries_started = page
                    .entries_started
                    .checked_add(1)
                    .ok_or_else(InternalError::store_invariant)?;
                page.decoded_bytes = page
                    .decoded_bytes
                    .checked_add(raw_key.as_bytes().len())
                    .and_then(|bytes| bytes.checked_add(raw_value.len()))
                    .ok_or_else(InternalError::store_invariant)?;

                inspect_index_entry(
                    plan,
                    &domain,
                    index_ordinal,
                    source_store,
                    store,
                    raw_key,
                    raw_value,
                    limits,
                    &mut page,
                )?;
                Ok(page.stopped)
            },
        )
    })?;

    if !observed_within_key {
        return Err(InternalError::store_corruption());
    }
    let exhausted = !page.stopped;
    page.finish(exhausted)
}

/// Execute one bounded page over one active source-owned reverse generation.
pub(in crate::db) fn execute_reverse_integrity_page<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    relation_ordinal: usize,
    checkpoint: PhysicalUnitCheckpoint,
    limits: DerivedInspectionLimits,
) -> Result<DerivedIntegrityPage, InternalError> {
    let limits = limits.validate()?;
    let identity = plan.identity();
    let relations = accepted_relation_projections(db, plan)?;
    let relation = relations
        .get(relation_ordinal)
        .ok_or_else(InternalError::store_invariant)?;
    let source_store = db.recovered_store(identity.store_path())?;
    let bounds = relation.raw_bounds()?;
    let checkpoint_key = checkpoint.raw_index_key()?;
    validate_checkpoint_in_bounds(checkpoint_key.as_ref(), &bounds)?;
    let lower = resume_lower_bound(&checkpoint, checkpoint_key.as_ref(), &bounds.0)?;
    let expected_within_key = matches!(checkpoint, PhysicalUnitCheckpoint::Within { .. })
        .then(|| checkpoint_key.clone())
        .flatten();
    let mut observed_within_key = expected_within_key.is_none();
    let mut page = DerivedPageAccumulator::new(checkpoint);

    relation.target_store().with_index(|store| {
        store.visit_raw_entries_in_range(
            (&lower, &bounds.1),
            Direction::Asc,
            |raw_key, raw_value| {
                if let Some(expected) = expected_within_key.as_ref()
                    && !observed_within_key
                {
                    if raw_key != expected {
                        return Err(InternalError::store_corruption());
                    }
                    observed_within_key = true;
                }
                if !page.can_start_entry(raw_key, raw_value, limits) {
                    page.stopped = true;
                    return Ok(true);
                }
                page.entries_started = page
                    .entries_started
                    .checked_add(1)
                    .ok_or_else(InternalError::store_invariant)?;
                page.decoded_bytes = page
                    .decoded_bytes
                    .checked_add(raw_key.as_bytes().len())
                    .and_then(|bytes| bytes.checked_add(raw_value.len()))
                    .ok_or_else(InternalError::store_invariant)?;

                inspect_reverse_entry(
                    plan,
                    relation,
                    source_store,
                    raw_key,
                    raw_value,
                    limits,
                    &mut page,
                )?;
                Ok(page.stopped)
            },
        )
    })?;

    if !observed_within_key {
        return Err(InternalError::store_corruption());
    }
    let exhausted = !page.stopped;
    page.finish(exhausted)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the physical entry boundary keeps accepted identity, source authority, store domain, bounds, and page state explicit"
)]
fn inspect_index_entry(
    plan: &AcceptedInspectionPlan,
    domain: &AcceptedIndexInspectionDomain,
    index_ordinal: usize,
    source_store: crate::db::registry::StoreHandle,
    index_store: &crate::db::index::IndexStore,
    raw_key: &RawIndexStoreKey,
    raw_value: &IndexEntryValue,
    limits: DerivedInspectionLimits,
    page: &mut DerivedPageAccumulator,
) -> Result<(), InternalError> {
    let start = start_index_atom(&page.checkpoint, raw_key, domain.unique())?;
    for family in start {
        if !page.can_classify_atom(limits) {
            page.stopped = true;
            return Ok(());
        }
        let finding = match family {
            IntegrityVerifierFamily::IndexEntry => inspect_index_witness(
                plan,
                domain,
                index_ordinal,
                source_store,
                raw_key,
                raw_value,
                limits,
                page,
            )?,
            IntegrityVerifierFamily::UniqueIndex => {
                inspect_unique_index_key(plan, domain, index_store, raw_key)?
            }
            _ => return Err(InternalError::store_corruption()),
        };
        if page.stopped {
            return Ok(());
        }
        page.atoms_classified = page
            .atoms_classified
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;
        if let Some(finding) = finding {
            page.findings.push(finding);
        }
        page.checkpoint = PhysicalUnitCheckpoint::Within {
            physical_key: bounded_index_key(raw_key)?,
            verifier_family: family,
            ordinal: 0,
        };
    }

    page.checkpoint = PhysicalUnitCheckpoint::After {
        physical_key: bounded_index_key(raw_key)?,
    };
    page.entries_completed = page
        .entries_completed
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one entry classifier keeps accepted identity, physical source/store facts, bounded accounting, and finding precedence visible at the trust boundary"
)]
fn inspect_index_witness(
    plan: &AcceptedInspectionPlan,
    domain: &AcceptedIndexInspectionDomain,
    index_ordinal: usize,
    source_store: crate::db::registry::StoreHandle,
    raw_key: &RawIndexStoreKey,
    raw_value: &IndexEntryValue,
    limits: DerivedInspectionLimits,
    page: &mut DerivedPageAccumulator,
) -> Result<Option<IntegrityFinding>, InternalError> {
    let Ok(key) = IndexKey::try_from_raw(raw_key) else {
        return derived_finding(
            plan,
            raw_key,
            IntegrityPhase::IndexEntries,
            IntegrityVerifierFamily::IndexEntry,
            IntegrityFindingKind::MalformedIndexEntry,
            Some(domain.schema_index_id().get()),
            None,
            "current_active_index_entry",
            "malformed_key",
        )
        .map(Some);
    };
    if !domain.contains_decoded_key(&key) || raw_value != &IndexEntryValue::presence() {
        return derived_finding(
            plan,
            raw_key,
            IntegrityPhase::IndexEntries,
            IntegrityVerifierFamily::IndexEntry,
            IntegrityFindingKind::MalformedIndexEntry,
            Some(domain.schema_index_id().get()),
            None,
            "current_active_index_entry",
            "wrong_identity_or_value",
        )
        .map(Some);
    }
    let Ok(primary_key) = key.primary_key_value() else {
        return derived_finding(
            plan,
            raw_key,
            IntegrityPhase::IndexEntries,
            IntegrityVerifierFamily::IndexEntry,
            IntegrityFindingKind::MalformedIndexEntry,
            Some(domain.schema_index_id().get()),
            None,
            "decodable_primary_key",
            "malformed_primary_key",
        )
        .map(Some);
    };
    let source_key =
        DecodedDataStoreKey::new(plan.identity().entity_tag(), &primary_key).to_raw()?;
    let Some(raw_row) = source_store.with_data(|store| store.get(&source_key)) else {
        return derived_finding(
            plan,
            raw_key,
            IntegrityPhase::IndexEntries,
            IntegrityVerifierFamily::IndexEntry,
            IntegrityFindingKind::OrphanIndexEntry,
            Some(domain.schema_index_id().get()),
            None,
            "authoritative_source_row",
            "missing",
        )
        .map(Some);
    };
    if !page.consume_source_row(raw_row.len(), limits)? {
        return Ok(None);
    }
    let Ok(reader) =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw_row, plan.row_contract())
    else {
        return derived_finding(
            plan,
            raw_key,
            IntegrityPhase::IndexEntries,
            IntegrityVerifierFamily::IndexEntry,
            IntegrityFindingKind::DivergentIndexEntry,
            Some(domain.schema_index_id().get()),
            None,
            "rederivable_source_row",
            "malformed_source_row",
        )
        .map(Some);
    };
    let expected = plan.index_inspection().project(
        index_ordinal,
        plan.identity().entity_tag(),
        &primary_key,
        &reader,
    )?;
    if expected.as_ref().is_some_and(|expected| {
        expected.store_path() == domain.store_path() && expected.raw_key() == raw_key
    }) {
        return Ok(None);
    }

    derived_finding(
        plan,
        raw_key,
        IntegrityPhase::IndexEntries,
        IntegrityVerifierFamily::IndexEntry,
        IntegrityFindingKind::DivergentIndexEntry,
        Some(domain.schema_index_id().get()),
        None,
        "exact_row_derived_witness",
        if expected.is_some() {
            "different_key"
        } else {
            "conditional_membership_absent"
        },
    )
    .map(Some)
}

fn inspect_unique_index_key(
    plan: &AcceptedInspectionPlan,
    domain: &AcceptedIndexInspectionDomain,
    index_store: &crate::db::index::IndexStore,
    raw_key: &RawIndexStoreKey,
) -> Result<Option<IntegrityFinding>, InternalError> {
    if !domain.unique() {
        return Ok(None);
    }
    let Ok(key) = IndexKey::try_from_raw(raw_key) else {
        return Ok(None);
    };
    let (lower, upper) = key
        .raw_bounds_for_all_components()
        .map_err(|_| InternalError::store_corruption())?;
    let bounds = (Bound::Included(lower), Bound::Included(upper));
    let mut witnesses = 0_u8;
    index_store.visit_raw_entries_in_range(
        (&bounds.0, &bounds.1),
        Direction::Asc,
        |_candidate, _value| {
            witnesses = witnesses
                .checked_add(1)
                .ok_or_else(InternalError::store_invariant)?;
            Ok(witnesses >= 2)
        },
    )?;
    if witnesses < 2 {
        return Ok(None);
    }

    derived_finding(
        plan,
        raw_key,
        IntegrityPhase::IndexEntries,
        IntegrityVerifierFamily::UniqueIndex,
        IntegrityFindingKind::DuplicateUniqueIndexKey,
        Some(domain.schema_index_id().get()),
        None,
        "one_row_witness",
        "multiple_row_witnesses",
    )
    .map(Some)
}

fn inspect_reverse_entry(
    plan: &AcceptedInspectionPlan,
    relation: &RelationConstraintProjection,
    source_store: crate::db::registry::StoreHandle,
    raw_key: &RawIndexStoreKey,
    raw_value: &IndexEntryValue,
    limits: DerivedInspectionLimits,
    page: &mut DerivedPageAccumulator,
) -> Result<(), InternalError> {
    if !page.can_classify_atom(limits) {
        page.stopped = true;
        return Ok(());
    }
    let finding = inspect_reverse_witness(
        plan,
        relation,
        source_store,
        raw_key,
        raw_value,
        limits,
        page,
    )?;
    if page.stopped {
        return Ok(());
    }
    page.atoms_classified = page
        .atoms_classified
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    if let Some(finding) = finding {
        page.findings.push(finding);
    }
    page.checkpoint = PhysicalUnitCheckpoint::After {
        physical_key: bounded_index_key(raw_key)?,
    };
    page.entries_completed = page
        .entries_completed
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    Ok(())
}

fn inspect_reverse_witness(
    plan: &AcceptedInspectionPlan,
    relation: &RelationConstraintProjection,
    source_store: crate::db::registry::StoreHandle,
    raw_key: &RawIndexStoreKey,
    raw_value: &IndexEntryValue,
    limits: DerivedInspectionLimits,
    page: &mut DerivedPageAccumulator,
) -> Result<Option<IntegrityFinding>, InternalError> {
    let Ok(key) = IndexKey::try_from_raw(raw_key) else {
        return reverse_finding(
            plan,
            relation,
            raw_key,
            IntegrityFindingKind::MalformedReverseRelationEntry,
            "current_active_reverse_entry",
            "malformed_key",
        )
        .map(Some);
    };
    if !relation.contains_decoded_key(&key) || raw_value != &IndexEntryValue::presence() {
        return reverse_finding(
            plan,
            relation,
            raw_key,
            IntegrityFindingKind::MalformedReverseRelationEntry,
            "current_active_reverse_entry",
            "wrong_identity_or_value",
        )
        .map(Some);
    }
    let Ok(source_primary_key) = key.primary_key_value() else {
        return reverse_finding(
            plan,
            relation,
            raw_key,
            IntegrityFindingKind::MalformedReverseRelationEntry,
            "decodable_source_primary_key",
            "malformed_source_primary_key",
        )
        .map(Some);
    };
    let source_key =
        DecodedDataStoreKey::new(plan.identity().entity_tag(), &source_primary_key).to_raw()?;
    let Some(raw_row) = source_store.with_data(|store| store.get(&source_key)) else {
        return reverse_finding(
            plan,
            relation,
            raw_key,
            IntegrityFindingKind::OrphanReverseRelationEntry,
            "authoritative_source_row",
            "missing",
        )
        .map(Some);
    };
    if !page.consume_source_row(raw_row.len(), limits)? {
        return Ok(None);
    }
    let Ok(reader) =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw_row, plan.row_contract())
    else {
        return reverse_finding(
            plan,
            relation,
            raw_key,
            IntegrityFindingKind::DivergentReverseRelationEntry,
            "rederivable_source_row",
            "malformed_source_row",
        )
        .map(Some);
    };
    let projected = relation.project_row(&source_primary_key, &reader, false)?;
    if projected.entries().iter().any(|entry| {
        entry.target_store_path() == relation.target_store_path() && entry.key() == raw_key
    }) {
        return Ok(None);
    }

    reverse_finding(
        plan,
        relation,
        raw_key,
        IntegrityFindingKind::DivergentReverseRelationEntry,
        "exact_source_derived_witness",
        "different_or_absent_edge",
    )
    .map(Some)
}

fn start_index_atom(
    checkpoint: &PhysicalUnitCheckpoint,
    raw_key: &RawIndexStoreKey,
    unique: bool,
) -> Result<Vec<IntegrityVerifierFamily>, InternalError> {
    match checkpoint {
        PhysicalUnitCheckpoint::Within {
            physical_key,
            verifier_family: IntegrityVerifierFamily::IndexEntry,
            ordinal: 0,
        } if physical_key == raw_key.as_bytes() && unique => {
            Ok(vec![IntegrityVerifierFamily::UniqueIndex])
        }
        PhysicalUnitCheckpoint::Within { .. } => Err(InternalError::store_corruption()),
        PhysicalUnitCheckpoint::BeforeFirst | PhysicalUnitCheckpoint::After { .. } => {
            let mut atoms = vec![IntegrityVerifierFamily::IndexEntry];
            if unique {
                atoms.push(IntegrityVerifierFamily::UniqueIndex);
            }
            Ok(atoms)
        }
    }
}

fn validate_checkpoint_in_bounds(
    checkpoint: Option<&RawIndexStoreKey>,
    bounds: &(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>),
) -> Result<(), InternalError> {
    if checkpoint.is_some_and(|key| !raw_key_in_bounds(key, bounds)) {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn resume_lower_bound(
    checkpoint: &PhysicalUnitCheckpoint,
    checkpoint_key: Option<&RawIndexStoreKey>,
    domain_lower: &Bound<RawIndexStoreKey>,
) -> Result<Bound<RawIndexStoreKey>, InternalError> {
    match (checkpoint, checkpoint_key) {
        (PhysicalUnitCheckpoint::BeforeFirst, None) => Ok(domain_lower.clone()),
        (PhysicalUnitCheckpoint::Within { .. }, Some(key)) => Ok(Bound::Included(key.clone())),
        (PhysicalUnitCheckpoint::After { .. }, Some(key)) => Ok(Bound::Excluded(key.clone())),
        _ => Err(InternalError::store_corruption()),
    }
}

fn raw_key_in_bounds(
    key: &RawIndexStoreKey,
    bounds: &(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>),
) -> bool {
    let after_lower = match &bounds.0 {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let before_upper = match &bounds.1 {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    after_lower && before_upper
}

fn reverse_finding(
    plan: &AcceptedInspectionPlan,
    relation: &RelationConstraintProjection,
    raw_key: &RawIndexStoreKey,
    kind: IntegrityFindingKind,
    expected: &str,
    observed: &str,
) -> Result<IntegrityFinding, InternalError> {
    let mut finding = derived_finding(
        plan,
        raw_key,
        IntegrityPhase::ReverseRelations,
        IntegrityVerifierFamily::ReverseRelationEntry,
        kind,
        None,
        Some(relation.relation_id().get()),
        expected,
        observed,
    )?;
    finding.store_path = relation.target_store_path().to_string();
    Ok(finding)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the finding carries phase, stable owner identity, and bounded expected/observed labels without a second diagnostic DTO"
)]
fn derived_finding(
    plan: &AcceptedInspectionPlan,
    raw_key: &RawIndexStoreKey,
    phase: IntegrityPhase,
    verifier_family: IntegrityVerifierFamily,
    kind: IntegrityFindingKind,
    schema_index_id: Option<u32>,
    relation_id: Option<u32>,
    expected: &str,
    observed: &str,
) -> Result<IntegrityFinding, InternalError> {
    let physical_store_path = schema_index_id
        .and_then(|schema_index_id| {
            plan.snapshot()
                .persisted_snapshot()
                .indexes()
                .iter()
                .find(|index| index.schema_id().get() == schema_index_id)
                .map(PersistedIndexSnapshot::store)
        })
        .unwrap_or_else(|| plan.identity().store_path());
    Ok(IntegrityFinding {
        diagnostic_code: icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: physical_store_path.to_string(),
        phase,
        verifier_family,
        physical_key: bounded_index_key(raw_key)?,
        primary_key: IndexKey::primary_key_value_and_bytes_from_raw(raw_key)
            .ok()
            .map(|(_, bytes)| bytes.to_vec()),
        field_paths: relation_id
            .map(|relation_id| relation_field_paths(plan, relation_id))
            .unwrap_or_default(),
        constraint_id: None,
        constraint_name: None,
        schema_index_id,
        relation_id,
        expected: Some(expected.to_string()),
        observed: Some(observed.to_string()),
    })
}

fn bounded_index_key(raw_key: &RawIndexStoreKey) -> Result<Vec<u8>, InternalError> {
    if raw_key.as_bytes().len() > IndexKey::MAX_STORED_SIZE_USIZE {
        return Err(InternalError::store_corruption());
    }
    Ok(raw_key.as_bytes().to_vec())
}
