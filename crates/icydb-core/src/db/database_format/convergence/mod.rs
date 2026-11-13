//! Module: db::database_format::convergence
//! Responsibility: initialize and reconcile the bounded current convergence authority.
//! Does not own: admission limits, online scheduling, or fold execution.
//! Boundary: generated allocation proposals + persisted commit/tail controls -> current format.

#[cfg(not(test))]
use crate::db::registry::open_committed_memory;
use crate::{
    db::{
        Db,
        commit::{
            MAX_PERSISTED_STORE_ALLOCATIONS, PersistedCommitControlObservation,
            PersistedStoreAllocation, PersistedStoreAllocationState, canonicalize_store_registry,
            inspect_persisted_commit_control, prepare_commit_control_replacement,
            replace_persisted_cursor_authentication_key,
        },
        integrity::generate_database_incarnation_id,
        journal::JournalTailStore,
        registry::{StoreAllocationIdentities, StoreAllocationIdentity, StoreHandle},
    },
    error::InternalError,
    traits::CanisterKind,
};
use ic_storage::{DefaultMemoryImpl, RuntimeMemory};
use std::{cell::RefCell, thread::LocalKey};

pub(in crate::db) const JOURNALED_STORE_ALLOCATION_WIDTH: usize = 4;

struct GeneratedStoreProposal {
    persisted: PersistedStoreAllocation,
    allocations: StoreAllocationIdentities,
    handle: StoreHandle,
}

pub(super) fn ensure_current_convergence_format<C: CanisterKind>(
    db: &Db<C>,
    control_memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
    fresh_database_boot: bool,
    boot_entropy: [u8; 32],
) -> Result<(), InternalError> {
    let proposals = generated_store_proposals::<C>(db)?;
    let result = if fresh_database_boot {
        require_fresh_proposal_roots(&proposals)?;
        let incarnation = generate_database_incarnation_id()?;
        let cursor_authentication_key =
            crate::db::database_format::entropy::cursor_key(boot_entropy, incarnation);
        let registry = proposals
            .iter()
            .map(|proposal| proposal.persisted.clone())
            .collect::<Vec<_>>();
        initialize_controls(
            control_memory,
            incarnation,
            cursor_authentication_key,
            0,
            registry,
            proposals.iter().map(|proposal| proposal.handle),
        )
    } else {
        match inspect_persisted_commit_control(control_memory.clone())? {
            PersistedCommitControlObservation::Uninitialized => {
                Err(InternalError::commit_corruption())
            }
            PersistedCommitControlObservation::Current {
                incarnation,
                cursor_authentication_key,
                database_commit_sequence,
                registry,
                marker_present,
            } => {
                validate_persisted_allocation_set::<C>(&registry)?;
                reconcile_current_registry(
                    control_memory,
                    incarnation,
                    cursor_authentication_key,
                    database_commit_sequence,
                    registry,
                    marker_present,
                    &proposals,
                )?;
                replace_persisted_cursor_authentication_key(
                    control_memory.clone(),
                    crate::db::database_format::entropy::cursor_key(boot_entropy, incarnation),
                )
            }
        }
    };
    result?;
    db.with_store_registry(crate::db::commit::register_runtime_journal_tails_for_backlog);
    Ok(())
}

fn initialize_controls(
    control_memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
    incarnation: crate::db::DatabaseIncarnationId,
    cursor_authentication_key: [u8; 32],
    database_commit_sequence: u64,
    mut registry: Vec<PersistedStoreAllocation>,
    handles: impl Iterator<Item = StoreHandle>,
) -> Result<(), InternalError> {
    canonicalize_store_registry(&mut registry)?;
    let handles = handles.collect::<Vec<_>>();
    let tails = handles
        .iter()
        .map(|handle| {
            handle
                .journal_tail_store()
                .ok_or_else(InternalError::store_invariant)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Fresh proposal admission proved every journal allocation is physically
    // virgin. Do not open its lazy store here: initialization writes a tree
    // header, which would make a later preparation error non-retryable.
    publish_control_replacement(
        control_memory,
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        &registry,
        tails,
        || super::publish_preflighted_current_boot_record(control_memory),
    )
}

fn reconcile_current_registry(
    control_memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
    incarnation: crate::db::DatabaseIncarnationId,
    cursor_authentication_key: [u8; 32],
    database_commit_sequence: u64,
    mut registry: Vec<PersistedStoreAllocation>,
    marker_present: bool,
    proposals: &[GeneratedStoreProposal],
) -> Result<(), InternalError> {
    let persisted_registry = registry.clone();
    let mut initialize = Vec::new();
    for entry in &mut registry {
        let proposal = proposals
            .iter()
            .find(|proposal| proposal.persisted.roles() == entry.roles());
        match (entry.state(), proposal) {
            (PersistedStoreAllocationState::Active, Some(proposal)) => {
                require_active_current_tail(proposal.handle)?;
            }
            (PersistedStoreAllocationState::Active, None) => {
                require_suspendable_tail(entry)?;
                *entry = entry.clone().dormant();
            }
            (PersistedStoreAllocationState::Dormant, Some(proposal)) => {
                // Exact quartet restoration reuses retained authority. Never
                // initialise, clear or reinterpret any of its physical roots.
                let tail = proposal
                    .handle
                    .journal_tail_store()
                    .ok_or_else(InternalError::store_invariant)?;
                if !tail
                    .with_borrow(JournalTailStore::validate_current_tail_authority)?
                    .is_empty()
                {
                    return Err(InternalError::store_unsupported());
                }
                *entry = entry.clone().reactivated();
            }
            (PersistedStoreAllocationState::Dormant, None) => {
                require_suspendable_tail(entry)?;
            }
        }
    }
    for proposal in proposals {
        if registry
            .iter()
            .any(|entry| entry.roles() == proposal.persisted.roles())
        {
            continue;
        }
        if registry.len() == MAX_PERSISTED_STORE_ALLOCATIONS {
            return Err(InternalError::store_unsupported());
        }
        require_proposal_roots_fresh(proposal)?;
        registry.push(proposal.persisted.clone());
        initialize.push(proposal.handle);
    }
    canonicalize_store_registry(&mut registry)?;
    if registry == persisted_registry {
        return Ok(());
    }
    // A pending generated application owns a store/entity activation. Keep its
    // topology available until the receipt is terminal; no orphaned record can
    // result from removal, addition, or an unrelated failed reconciliation.
    if marker_present
        || crate::db::schema::pending_application_blocks_registry_change(control_memory.clone())?
    {
        return Err(InternalError::store_unsupported());
    }
    let initialize = initialize
        .into_iter()
        .map(|handle| {
            handle
                .journal_tail_store()
                .ok_or_else(InternalError::store_invariant)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for tail in &initialize {
        tail.with_borrow(JournalTailStore::preflight_current_tail_control_initialization)?;
    }
    publish_control_replacement(
        control_memory,
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        &registry,
        initialize,
        || {},
    )
}

// Preparation must finish before any publication. Fresh bootstrap supplies its
// infallible boot write here; reconciliation has already preflighted new tails.
fn publish_control_replacement(
    control_memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
    incarnation: crate::db::DatabaseIncarnationId,
    cursor_authentication_key: [u8; 32],
    database_commit_sequence: u64,
    registry: &[PersistedStoreAllocation],
    tails: Vec<&'static LocalKey<RefCell<JournalTailStore>>>,
    before_tail_initialization: impl FnOnce(),
) -> Result<(), InternalError> {
    let replacement = prepare_commit_control_replacement(
        control_memory.clone(),
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        registry,
    )?;
    before_tail_initialization();
    for tail in tails {
        tail.with_borrow_mut(JournalTailStore::apply_current_tail_control_initialization);
    }
    replacement.apply();
    Ok(())
}

fn generated_store_proposals<C: CanisterKind>(
    db: &Db<C>,
) -> Result<Vec<GeneratedStoreProposal>, InternalError> {
    let mut proposals = db.with_store_registry(|registry| {
        registry
            .iter()
            .filter_map(|(_, handle)| {
                handle
                    .journal_tail_store()
                    .map(|_| (handle.allocation_identities(), handle))
            })
            .map(|(allocations, handle)| {
                Ok(GeneratedStoreProposal {
                    persisted: PersistedStoreAllocation::active(allocations)?,
                    allocations,
                    handle,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()
    })?;
    if proposals.len() > MAX_PERSISTED_STORE_ALLOCATIONS {
        return Err(InternalError::store_unsupported());
    }
    validate_persisted_allocation_set::<C>(proposals.iter().map(|proposal| &proposal.persisted))?;
    proposals.sort_by(|left, right| left.persisted.roles().cmp(right.persisted.roles()));
    Ok(proposals)
}

// Both authored proposals and retained records expose the same persisted roles.
fn validate_persisted_allocation_set<'a, C: CanisterKind>(
    registry: impl IntoIterator<Item = &'a PersistedStoreAllocation>,
) -> Result<(), InternalError> {
    let mut memory_ids = vec![
        C::commit_memory_id().map_err(|_| InternalError::store_internal())?,
        C::startup_memory_id().map_err(|_| InternalError::store_internal())?,
        C::integrity_progress_memory_id().map_err(|_| InternalError::store_internal())?,
    ];
    let mut stable_keys = vec![
        C::COMMIT_STABLE_KEY,
        C::STARTUP_STABLE_KEY,
        C::INTEGRITY_PROGRESS_STABLE_KEY,
    ];
    for entry in registry {
        for role in entry.roles() {
            if role.memory_id() == 0
                || memory_ids.contains(&role.memory_id())
                || stable_keys.contains(&role.stable_key())
            {
                return Err(InternalError::store_unsupported());
            }
            memory_ids.push(role.memory_id());
            stable_keys.push(role.stable_key());
        }
    }
    Ok(())
}

fn require_fresh_proposal_roots(proposals: &[GeneratedStoreProposal]) -> Result<(), InternalError> {
    for proposal in proposals {
        require_proposal_roots_fresh(proposal)?;
    }
    Ok(())
}

fn require_proposal_roots_fresh(proposal: &GeneratedStoreProposal) -> Result<(), InternalError> {
    for allocation in required_allocations(proposal.allocations)? {
        if allocation_pages(allocation)? != 0 {
            return Err(InternalError::store_unsupported());
        }
    }
    // Zero-sized physical roots cannot contain a tail. Borrowing its lazy
    // store to check absence would itself allocate the header we must avoid.
    Ok(())
}

fn require_active_current_tail(handle: StoreHandle) -> Result<(), InternalError> {
    handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?
        .with_borrow(|tail| tail.validate_current_tail_authority().map(|_| ()))
}

fn require_suspendable_tail(entry: &PersistedStoreAllocation) -> Result<(), InternalError> {
    if !dormant_journal_tail_control(entry)?.is_empty() {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
}

// Dormant allocations have no generated tree owner in the current installation.
// Open, validate and release their sole lease; never infer an empty tail from state.
pub(in crate::db) fn dormant_journal_tail_control(
    entry: &PersistedStoreAllocation,
) -> Result<crate::db::journal::JournalTailControl, InternalError> {
    let journal = entry.journal();
    JournalTailStore::init(store_memory_owned(
        journal.memory_id(),
        journal.stable_key(),
    )?)?
    .validate_current_tail_authority()
}

fn required_allocations(
    allocations: StoreAllocationIdentities,
) -> Result<[StoreAllocationIdentity; JOURNALED_STORE_ALLOCATION_WIDTH], InternalError> {
    Ok([
        allocations
            .data()
            .ok_or_else(InternalError::store_invariant)?,
        allocations
            .index()
            .ok_or_else(InternalError::store_invariant)?,
        allocations
            .schema()
            .ok_or_else(InternalError::store_invariant)?,
        allocations
            .journal()
            .ok_or_else(InternalError::store_invariant)?,
    ])
}

#[cfg(not(test))]
use crate::db::database_format::allocation_pages;

#[cfg(not(test))]
fn store_memory_owned(
    memory_id: u64,
    stable_key: &str,
) -> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
    open_committed_memory(stable_key, memory_id).map_err(|_| InternalError::store_internal())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
use helper_tests::allocation_pages;
#[cfg(test)]
use helper_tests::store_memory_owned;

#[cfg(test)]
pub(in crate::db) use helper_tests::CANISTER_CONTROL_ALLOCATION_COUNT;
