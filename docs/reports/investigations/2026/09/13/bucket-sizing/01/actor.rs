use candid::CandidType;
use ic_memory::{
    AllocationPolicy, AllocationSlotDescriptor, MemoryManagerConfig, PolicyIdentity,
    PolicyIdentityError, RuntimeBootstrapPolicy, StableKey,
};
use icydb::db::query::FieldRef;
use icydb::db::{DynamicQuery, StructuralPatch, WriteCell};
use icydb::value::InputValue;
use std::convert::Infallible;

icydb::start!(participant);

struct ExperimentPolicy;
impl AllocationPolicy for ExperimentPolicy {
    type Error = Infallible;
    fn validate_key(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }
    fn validate_slot(
        &self,
        _: &StableKey,
        _: &AllocationSlotDescriptor,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn validate_reserved_slot(
        &self,
        _: &StableKey,
        _: &AllocationSlotDescriptor,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl RuntimeBootstrapPolicy for ExperimentPolicy {
    fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
        PolicyIdentity::new("icydb.bucket-sweep", 1)
    }
}

#[ic_cdk::init]
fn init(pages: u16) {
    assert!([1, 4, 8, 16, 32, 64, 128].contains(&pages));
    ic_memory::bootstrap_default_memory_manager_with_config(
        MemoryManagerConfig::new(pages).unwrap(),
        &ExperimentPolicy,
    )
    .unwrap();
    __icydb_lifecycle_participant::init();
}

#[ic_cdk::update]
fn ready() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| db().map(|_| ()))
}

#[derive(CandidType)]
struct Allocation {
    physical: u64,
    virtual_bytes: u64,
    slack: u64,
    buckets: u16,
    slots: Vec<(u8, u64, u64)>,
}
#[ic_cdk::query]
fn allocation() -> Allocation {
    let report = ic_memory::default_memory_manager_memory_allocations().unwrap();
    Allocation {
        physical: report.physical_extent.bytes,
        virtual_bytes: report.virtual_extent.bytes,
        slack: report.bucket_slack_bytes,
        buckets: report.allocated_buckets,
        slots: report
            .memories
            .iter()
            .filter(|m| m.allocated_buckets > 0)
            .map(|m| {
                (
                    m.memory_manager_id,
                    m.virtual_extent.bytes,
                    m.allocated_bytes,
                )
            })
            .collect(),
    }
}

fn name(id: u32, bytes: u32) -> String {
    format!("{id:08}{}", "x".repeat(bytes.saturating_sub(8) as usize))
}

#[ic_cdk::update]
fn insert(first: u32, count: u32, bytes: u32) -> Result<u64, icydb::Error> {
    assert!(count <= 32 && (8..=8192).contains(&bytes));
    let start = ic_cdk::api::performance_counter(1);
    icydb::db::with_request_execution(|| {
        let session = db()?;
        let patches = (first..first + count)
            .map(|id| {
                StructuralPatch::new()
                    .field("name", WriteCell::Value(InputValue::text(name(id, bytes))))
                    .field("age", WriteCell::Value(InputValue::int64(i64::from(id))))
                    .field(
                        "rank",
                        WriteCell::Value(InputValue::int64(i64::from(id % 16))),
                    )
            })
            .collect();
        session.execute_trusted_structural_insert_batch("SqlTestUser", patches)?;
        Ok::<_, icydb::Error>(())
    })?;
    Ok(ic_cdk::api::performance_counter(1) - start)
}

#[ic_cdk::query]
fn read(first: u32, count: u32, bytes: u32) -> Result<(u64, u32), icydb::Error> {
    assert!(count <= 32);
    let start = ic_cdk::api::performance_counter(1);
    let found = icydb::db::with_request_execution(|| {
        let session = db()?;
        let mut found = 0;
        for id in first..first + count {
            let expected = name(id, bytes);
            let query = DynamicQuery::new("SqlTestUser")
                .filter(FieldRef::new("name").eq(expected.clone()))
                .limit(1);
            let rows = session.execute_live_page(&query, None)?;
            assert_eq!(rows.len(), 1);
            let column = rows
                .columns
                .iter()
                .position(|field| field == "name")
                .unwrap();
            assert!(
                matches!(rows.rows[0][column].as_public(), icydb::value::PublicValue::Text(value) if value == &expected)
            );
            found += rows.len() as u32;
        }
        Ok::<_, icydb::Error>(found)
    })?;
    Ok((ic_cdk::api::performance_counter(1) - start, found))
}
