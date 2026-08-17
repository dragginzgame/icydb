//! Module: db::database_format::convergence
//! Responsibility: initialize and reconcile the bounded current convergence authority.
//! Does not own: admission limits, online scheduling, or fold execution.
//! Boundary: generated allocation proposals + persisted commit/tail controls -> current format.

use crate::{
    db::{
        Db,
        commit::{
            MAX_PERSISTED_STORE_ALLOCATIONS, PersistedCommitControlObservation,
            PersistedStoreAllocation, PersistedStoreAllocationState,
            apply_prepared_commit_control_replacement, canonicalize_store_registry,
            inspect_persisted_commit_control, prepare_commit_control_replacement,
        },
        integrity::{generate_cursor_authentication_key, generate_database_incarnation_id},
        journal::JournalTailStore,
        registry::{StoreAllocationIdentities, StoreAllocationIdentity, StoreHandle},
    },
    error::InternalError,
    traits::CanisterKind,
};
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use ic_stable_structures::{DefaultMemoryImpl, Memory, memory_manager::VirtualMemory};

pub(in crate::db) const APP_MEMORY_ID_MIN: u8 = 100;
pub(in crate::db) const APP_MEMORY_ID_MAX: u8 = 254;
pub(in crate::db) const CANISTER_CONTROL_ALLOCATION_COUNT: usize = 3;
pub(in crate::db) const JOURNALED_STORE_ALLOCATION_WIDTH: usize = 4;
pub(in crate::db) const MAX_DEPLOYMENT_STORE_ALLOCATIONS: usize =
    ((APP_MEMORY_ID_MAX as usize - APP_MEMORY_ID_MIN as usize + 1)
        - CANISTER_CONTROL_ALLOCATION_COUNT)
        / JOURNALED_STORE_ALLOCATION_WIDTH;

struct GeneratedStoreProposal {
    persisted: PersistedStoreAllocation,
    allocations: StoreAllocationIdentities,
    handle: StoreHandle,
}

pub(super) fn ensure_current_convergence_format<C: CanisterKind>(
    db: &Db<C>,
    control_memory: &VirtualMemory<DefaultMemoryImpl>,
    fresh_database_boot: bool,
) -> Result<(), InternalError> {
    let proposals = generated_store_proposals::<C>(db)?;
    let result = if fresh_database_boot {
        require_fresh_proposal_roots(&proposals)?;
        let incarnation = generate_database_incarnation_id()?;
        let cursor_authentication_key = generate_cursor_authentication_key()?;
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
                )
            }
        }
    };
    result?;
    #[cfg(test)]
    crate::db::commit::register_runtime_journal_tails_for_backlog(db)?;
    Ok(())
}

fn initialize_controls(
    control_memory: &VirtualMemory<DefaultMemoryImpl>,
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
    for tail in &tails {
        tail.with_borrow(JournalTailStore::preflight_current_tail_control_initialization)?;
    }
    let replacement = prepare_commit_control_replacement(
        control_memory.clone(),
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        &registry,
    )?;
    super::publish_preflighted_current_boot_record(control_memory);
    for tail in tails {
        tail.with_borrow_mut(JournalTailStore::apply_current_tail_control_initialization);
    }
    apply_prepared_commit_control_replacement(replacement);
    Ok(())
}

fn reconcile_current_registry(
    control_memory: &VirtualMemory<DefaultMemoryImpl>,
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
                require_retirable_tail(entry)?;
                *entry = entry.clone().retired();
            }
            (PersistedStoreAllocationState::Retired, Some(_)) => {
                return Err(InternalError::store_unsupported());
            }
            (PersistedStoreAllocationState::Retired, None) => {
                require_retirable_tail(entry)?;
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
    if marker_present {
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
    let replacement = prepare_commit_control_replacement(
        control_memory.clone(),
        incarnation,
        cursor_authentication_key,
        database_commit_sequence,
        &registry,
    )?;
    for tail in initialize {
        tail.with_borrow_mut(JournalTailStore::apply_current_tail_control_initialization);
    }
    apply_prepared_commit_control_replacement(replacement);
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
    if proposals.len() > MAX_DEPLOYMENT_STORE_ALLOCATIONS
        || MAX_DEPLOYMENT_STORE_ALLOCATIONS != MAX_PERSISTED_STORE_ALLOCATIONS
    {
        return Err(InternalError::store_unsupported());
    }
    validate_allocation_set::<C>(&proposals)?;
    proposals.sort_by(|left, right| left.persisted.roles().cmp(right.persisted.roles()));
    Ok(proposals)
}

fn validate_allocation_set<C: CanisterKind>(
    proposals: &[GeneratedStoreProposal],
) -> Result<(), InternalError> {
    let mut memory_ids = vec![
        C::COMMIT_MEMORY_ID,
        C::STARTUP_MEMORY_ID,
        C::INTEGRITY_PROGRESS_MEMORY_ID,
    ];
    let mut stable_keys = vec![
        C::COMMIT_STABLE_KEY,
        C::STARTUP_STABLE_KEY,
        C::INTEGRITY_PROGRESS_STABLE_KEY,
    ];
    for proposal in proposals {
        for role in proposal.persisted.roles() {
            if !(APP_MEMORY_ID_MIN..=APP_MEMORY_ID_MAX).contains(&role.memory_id())
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

fn validate_persisted_allocation_set<C: CanisterKind>(
    registry: &[PersistedStoreAllocation],
) -> Result<(), InternalError> {
    let mut memory_ids = vec![
        C::COMMIT_MEMORY_ID,
        C::STARTUP_MEMORY_ID,
        C::INTEGRITY_PROGRESS_MEMORY_ID,
    ];
    let mut stable_keys = vec![
        C::COMMIT_STABLE_KEY,
        C::STARTUP_STABLE_KEY,
        C::INTEGRITY_PROGRESS_STABLE_KEY,
    ];
    for entry in registry {
        for role in entry.roles() {
            if !(APP_MEMORY_ID_MIN..=APP_MEMORY_ID_MAX).contains(&role.memory_id())
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
        if store_memory(allocation)?.size() != 0 {
            return Err(InternalError::store_unsupported());
        }
    }
    require_absent_current_tail(proposal.handle)
}

fn require_absent_current_tail(handle: StoreHandle) -> Result<(), InternalError> {
    handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?
        .with_borrow(|tail| {
            if tail.has_stored_batch() || tail.has_current_tail_control() {
                return Err(InternalError::store_unsupported());
            }
            Ok(())
        })
}

fn require_active_current_tail(handle: StoreHandle) -> Result<(), InternalError> {
    handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?
        .with_borrow(|tail| tail.validate_current_tail_authority().map(|_| ()))
}

fn require_retirable_tail(entry: &PersistedStoreAllocation) -> Result<(), InternalError> {
    let journal = entry.journal();
    let tail = JournalTailStore::init(store_memory_owned(
        journal.memory_id(),
        journal.stable_key(),
    )?);
    if !tail.validate_current_tail_authority()?.is_empty() {
        return Err(InternalError::store_unsupported());
    }
    Ok(())
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

#[cfg(test)]
thread_local! {
    static TEST_CONVERGENCE_MEMORIES: std::cell::RefCell<
        Vec<(u8, String, VirtualMemory<DefaultMemoryImpl>)>
    > = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn store_memory(
    allocation: StoreAllocationIdentity,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    store_memory_owned(allocation.memory_id(), allocation.stable_key())
}

#[cfg(not(test))]
fn store_memory(
    allocation: StoreAllocationIdentity,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    store_memory_owned(allocation.memory_id(), allocation.stable_key())
}

#[cfg(test)]
fn store_memory_owned(
    memory_id: u8,
    stable_key: &str,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    TEST_CONVERGENCE_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories
            .iter()
            .find(|(id, key, _)| *id == memory_id && key == stable_key)
        {
            return Ok(memory.clone());
        }
        let memory = crate::testing::test_memory(memory_id);
        memories.push((memory_id, stable_key.to_string(), memory.clone()));
        Ok(memory)
    })
}

#[cfg(not(test))]
fn store_memory_owned(
    memory_id: u8,
    stable_key: &str,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    open_default_memory_manager_memory(stable_key, memory_id)
        .map_err(InternalError::database_format_memory_registration_failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            RequestExecutionRoot,
            data::DataStore,
            index::IndexStore,
            journal::{DatabaseCommitSequence, FoldWatermark, JournalBatch, JournalSequence},
            registry::{StoreRegistry, StoreRuntimeStorageCapabilities},
            schema::SchemaStore,
        },
        testing::test_memory,
        traits::Path,
    };
    use std::cell::RefCell;

    const FRESH_PATH: &str = "convergence_tests::Fresh";
    const LIFE_A_PATH: &str = "convergence_tests::LifeA";
    const LIFE_B_PATH: &str = "convergence_tests::LifeB";

    struct ConvergenceCanister;

    impl Path for ConvergenceCanister {
        const PATH: &'static str = "convergence_tests::Canister";
    }

    impl CanisterKind for ConvergenceCanister {
        const COMMIT_MEMORY_ID: u8 = 100;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.convergence.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 101;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.convergence.startup.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 102;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.convergence.integrity.v1";
    }

    const fn allocations(
        first_memory_id: u8,
        data_key: &'static str,
        index_key: &'static str,
        schema_key: &'static str,
        journal_key: &'static str,
    ) -> StoreAllocationIdentities {
        StoreAllocationIdentities::new_journaled(
            StoreAllocationIdentity::new(first_memory_id, data_key),
            StoreAllocationIdentity::new(first_memory_id + 1, index_key),
            StoreAllocationIdentity::new(first_memory_id + 2, schema_key),
            StoreAllocationIdentity::new(first_memory_id + 3, journal_key),
        )
    }

    thread_local! {
        static FRESH_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
            store_memory_owned(103, "icydb.test.convergence.fresh.data.v1").unwrap()
        ));
        static FRESH_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
            store_memory_owned(104, "icydb.test.convergence.fresh.index.v1").unwrap()
        ));
        static FRESH_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
            store_memory_owned(105, "icydb.test.convergence.fresh.schema.v1").unwrap()
        ));
        static FRESH_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
            store_memory_owned(106, "icydb.test.convergence.fresh.journal.v1").unwrap()
        ));
        static FRESH_REGISTRY: StoreRegistry = registry(
            FRESH_PATH,
            &FRESH_DATA,
            &FRESH_INDEX,
            &FRESH_SCHEMA,
            &FRESH_JOURNAL,
            allocations(
                103,
                "icydb.test.convergence.fresh.data.v1",
                "icydb.test.convergence.fresh.index.v1",
                "icydb.test.convergence.fresh.schema.v1",
                "icydb.test.convergence.fresh.journal.v1",
            ),
        );

        static LIFE_A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
            store_memory_owned(115, "icydb.test.convergence.lifea.data.v1").unwrap()
        ));
        static LIFE_A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
            store_memory_owned(116, "icydb.test.convergence.lifea.index.v1").unwrap()
        ));
        static LIFE_A_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
            store_memory_owned(117, "icydb.test.convergence.lifea.schema.v1").unwrap()
        ));
        static LIFE_A_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
            store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap()
        ));
        static LIFE_B_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
            store_memory_owned(119, "icydb.test.convergence.lifeb.data.v1").unwrap()
        ));
        static LIFE_B_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
            store_memory_owned(120, "icydb.test.convergence.lifeb.index.v1").unwrap()
        ));
        static LIFE_B_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
            store_memory_owned(121, "icydb.test.convergence.lifeb.schema.v1").unwrap()
        ));
        static LIFE_B_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
            store_memory_owned(122, "icydb.test.convergence.lifeb.journal.v1").unwrap()
        ));
        static LIFE_A_REGISTRY: StoreRegistry = registry(
            LIFE_A_PATH,
            &LIFE_A_DATA,
            &LIFE_A_INDEX,
            &LIFE_A_SCHEMA,
            &LIFE_A_JOURNAL,
            allocations(
                115,
                "icydb.test.convergence.lifea.data.v1",
                "icydb.test.convergence.lifea.index.v1",
                "icydb.test.convergence.lifea.schema.v1",
                "icydb.test.convergence.lifea.journal.v1",
            ),
        );
        static LIFE_B_REGISTRY: StoreRegistry = registry(
            LIFE_B_PATH,
            &LIFE_B_DATA,
            &LIFE_B_INDEX,
            &LIFE_B_SCHEMA,
            &LIFE_B_JOURNAL,
            allocations(
                119,
                "icydb.test.convergence.lifeb.data.v1",
                "icydb.test.convergence.lifeb.index.v1",
                "icydb.test.convergence.lifeb.schema.v1",
                "icydb.test.convergence.lifeb.journal.v1",
            ),
        );
        static LIFE_AB_REGISTRY: StoreRegistry = {
            let mut registry = registry(
                LIFE_A_PATH,
                &LIFE_A_DATA,
                &LIFE_A_INDEX,
                &LIFE_A_SCHEMA,
                &LIFE_A_JOURNAL,
                allocations(
                    115,
                    "icydb.test.convergence.lifea.data.v1",
                    "icydb.test.convergence.lifea.index.v1",
                    "icydb.test.convergence.lifea.schema.v1",
                    "icydb.test.convergence.lifea.journal.v1",
                ),
            );
            registry.register_journaled_store(
                LIFE_B_PATH,
                &LIFE_B_DATA,
                &LIFE_B_INDEX,
                &LIFE_B_SCHEMA,
                &LIFE_B_JOURNAL,
                allocations(
                    119,
                    "icydb.test.convergence.lifeb.data.v1",
                    "icydb.test.convergence.lifeb.index.v1",
                    "icydb.test.convergence.lifeb.schema.v1",
                    "icydb.test.convergence.lifeb.journal.v1",
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).unwrap();
            registry
        };
        static LIFE_EMPTY_REGISTRY: StoreRegistry = StoreRegistry::new();
    }

    fn registry(
        path: &'static str,
        data: &'static std::thread::LocalKey<RefCell<DataStore>>,
        index: &'static std::thread::LocalKey<RefCell<IndexStore>>,
        schema: &'static std::thread::LocalKey<RefCell<SchemaStore>>,
        journal: &'static std::thread::LocalKey<RefCell<JournalTailStore>>,
        allocations: StoreAllocationIdentities,
    ) -> StoreRegistry {
        let mut registry = StoreRegistry::new();
        registry
            .register_journaled_store(
                path,
                data,
                index,
                schema,
                journal,
                allocations,
                StoreRuntimeStorageCapabilities::journaled(),
            )
            .unwrap();
        registry
    }

    fn database(
        registry: &'static std::thread::LocalKey<StoreRegistry>,
    ) -> (RequestExecutionRoot, Db<ConvergenceCanister>) {
        let root = RequestExecutionRoot::__new_runtime_root();
        let db = Db::new(registry, root.scope());
        (root, db)
    }

    fn maximum_registry() -> Vec<PersistedStoreAllocation> {
        (0..MAX_PERSISTED_STORE_ALLOCATIONS)
            .map(|ordinal| {
                let first = APP_MEMORY_ID_MIN
                    + u8::try_from(CANISTER_CONTROL_ALLOCATION_COUNT + ordinal * 4).unwrap();
                let key = |role: &str| -> &'static str {
                    Box::leak(
                        format!("icydb.test.convergence.max.s{ordinal}.{role}.v1").into_boxed_str(),
                    )
                };
                PersistedStoreAllocation::active(allocations(
                    first,
                    key("data"),
                    key("index"),
                    key("schema"),
                    key("journal"),
                ))
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn deployment_store_bound_is_derived_from_canonical_allocation_shape() {
        assert_eq!(MAX_DEPLOYMENT_STORE_ALLOCATIONS, 38);
        assert_eq!(
            MAX_DEPLOYMENT_STORE_ALLOCATIONS,
            MAX_PERSISTED_STORE_ALLOCATIONS
        );

        let mut registry = maximum_registry();
        canonicalize_store_registry(&mut registry).unwrap();
        validate_persisted_allocation_set::<ConvergenceCanister>(&registry).unwrap();
        let memory = test_memory(127);
        super::super::write_current_boot_record(&memory).unwrap();
        let replacement = prepare_commit_control_replacement(
            memory.clone(),
            crate::db::DatabaseIncarnationId::for_tests(0x71),
            [0x72; 32],
            0,
            &registry,
        )
        .unwrap();
        apply_prepared_commit_control_replacement(replacement);
        let PersistedCommitControlObservation::Current {
            registry: reopened, ..
        } = inspect_persisted_commit_control(memory).unwrap()
        else {
            panic!("maximum registry must remain current");
        };
        assert_eq!(reopened, registry);

        registry.push(registry[0].clone());
        assert!(canonicalize_store_registry(&mut registry).is_err());
        let control_collision = vec![
            PersistedStoreAllocation::active(allocations(
                ConvergenceCanister::COMMIT_MEMORY_ID,
                "icydb.test.convergence.collision.data.v1",
                "icydb.test.convergence.collision.index.v1",
                "icydb.test.convergence.collision.schema.v1",
                "icydb.test.convergence.collision.journal.v1",
            ))
            .unwrap(),
        ];
        assert!(
            validate_persisted_allocation_set::<ConvergenceCanister>(&control_collision).is_err()
        );
    }

    #[test]
    fn fresh_initialization_publishes_exact_current_controls() {
        let fresh_control = test_memory(123);
        let (_root, fresh) = database(&FRESH_REGISTRY);
        ensure_current_convergence_format(&fresh, &fresh_control, true).unwrap();
        let PersistedCommitControlObservation::Current {
            database_commit_sequence,
            registry,
            ..
        } = inspect_persisted_commit_control(fresh_control).unwrap()
        else {
            panic!("fresh initialization must publish current control");
        };
        assert_eq!(database_commit_sequence, 0);
        assert_eq!(registry.len(), 1);
        assert_eq!(registry[0].state(), PersistedStoreAllocationState::Active);
        FRESH_JOURNAL.with_borrow(|tail| {
            assert!(tail.validate_current_tail_authority().unwrap().is_empty());
        });
    }

    #[test]
    fn current_registry_rejects_debt_retirement_and_never_reuses_retired_quartets() {
        let control = test_memory(126);
        let (_root, life_a) = database(&LIFE_A_REGISTRY);
        ensure_current_convergence_format(&life_a, &control, true).unwrap();
        let retained = JournalBatch::new_with_database_commit_sequence(
            [0x71; 16],
            [0x72; 16],
            JournalSequence::new(1),
            DatabaseCommitSequence::new(1),
            Vec::new(),
        )
        .unwrap();
        LIFE_A_JOURNAL.with_borrow_mut(|tail| tail.append_batch(&retained).unwrap());
        ensure_current_convergence_format(&life_a, &control, false)
            .expect("current startup must admit a bounded nonempty tail");

        let (_root, empty) = database(&LIFE_EMPTY_REGISTRY);
        assert!(ensure_current_convergence_format(&empty, &control, false).is_err());
        LIFE_A_JOURNAL.with_borrow_mut(|tail| {
            let retirement = tail
                .prepare_batch_retirement(&retained, FoldWatermark::new(JournalSequence::new(1), 1))
                .unwrap();
            tail.apply_prepared_batch_retirement(retirement);
        });
        ensure_current_convergence_format(&empty, &control, false).unwrap();

        let (_root, life_b) = database(&LIFE_B_REGISTRY);
        ensure_current_convergence_format(&life_b, &control, false).unwrap();
        let PersistedCommitControlObservation::Current { registry, .. } =
            inspect_persisted_commit_control(control.clone()).unwrap()
        else {
            panic!("lifecycle reconciliation must remain current");
        };
        assert_eq!(registry.len(), 2);
        assert_eq!(registry[0].state(), PersistedStoreAllocationState::Retired);
        assert_eq!(registry[1].state(), PersistedStoreAllocationState::Active);

        let (_root, combined_database) = database(&LIFE_AB_REGISTRY);
        assert!(ensure_current_convergence_format(&combined_database, &control, false).is_err());
        let PersistedCommitControlObservation::Current {
            registry: unchanged,
            ..
        } = inspect_persisted_commit_control(control).unwrap()
        else {
            panic!("rejected reuse must retain current authority");
        };
        assert_eq!(unchanged, registry);
    }
}
