//! Test-only constructors and adapters for the parent module.

use super::*;

impl InspectionProgressStore {
    pub(in crate::db) fn replace_mutation(
        &mut self,
        record: &MutationJobRecord,
    ) -> Result<(), MutationJobError> {
        record.validate()?;
        let key = ProgressRecordKey::from_mutation_job_id(record.state().job_id)?;
        if !self.map.contains_key(&key) {
            return Err(MutationJobError::NotFound);
        }
        self.map.insert(
            key,
            ProgressRecordBytes(encode_mutation_job_record(record)?),
        );
        Ok(())
    }
}

pub(super) fn progress_memory<C: CanisterKind>()
-> Result<RuntimeMemory<DefaultMemoryImpl>, IntegrityJobError> {
    let resolved_id = C::integrity_progress_memory_id().map_err(|_| IntegrityJobError::Internal)?;
    thread_local! {
        static MEMORIES: RefCell<
            Vec<(u64, &'static str, crate::testing::TestMemoryOwner)>
        > = const { RefCell::new(Vec::new()) };
    }

    MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories
            .iter()
            .find(|(id, key, _)| *id == resolved_id && *key == C::INTEGRITY_PROGRESS_STABLE_KEY)
        {
            return Ok(memory.open());
        }
        let owner = crate::testing::test_memory_owner(resolved_id);
        let memory = owner.open();
        memories.push((resolved_id, C::INTEGRITY_PROGRESS_STABLE_KEY, owner));
        Ok(memory)
    })
}
