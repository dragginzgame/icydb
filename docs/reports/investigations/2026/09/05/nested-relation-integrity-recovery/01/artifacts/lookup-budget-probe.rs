// Isolated audit probe: the method below is copied verbatim from reverse_index.rs.
// Raw keys use ordered byte vectors and the error is a local stand-in. This proves
// counter behavior, not an end-to-end database mutation or stable-store timing.
type RawDataStoreKey = Vec<u8>;
#[derive(Debug, PartialEq)]
struct InternalError { limit: u64, observed: u64 }
mod icydb_diagnostic_code {
    pub enum DiagnosticExecutionBudgetResource { RowsVisited }
}
const MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS: u64 = 3_276;
fn relation_budget_error(
    _: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    limit: u64,
    observed: u64,
) -> InternalError { InternalError { limit, observed } }
#[derive(Default)]
struct RelationCommitBudget { validated_target_keys: Vec<RawDataStoreKey> }
impl RelationCommitBudget {
    fn validate_target_once(
        &mut self,
        key: RawDataStoreKey,
        validate: impl FnOnce(&RawDataStoreKey) -> Result<bool, InternalError>,
    ) -> Result<Option<RawDataStoreKey>, InternalError> {
        let Err(insertion_index) = self.validated_target_keys.binary_search(&key) else {
            return Ok(None);
        };
        let observed = u64::try_from(self.validated_target_keys.len())
            .map_or(u64::MAX, |count| count.saturating_add(1));
        if observed > MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
            return Err(relation_budget_error(
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
                MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS,
                MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS.saturating_add(1),
            ));
        }
        if !validate(&key)? {
            return Ok(Some(key));
        }
        self.validated_target_keys.insert(insertion_index, key);
        Ok(None)
    }

}
#[test]
fn successful_distinct_lookups_stop_at_the_limit() {
    let mut budget = RelationCommitBudget::default();
    let mut reads = 0;
    for key in 0_u64..=MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
        let result = budget.validate_target_once(key.to_be_bytes().to_vec(), |_| {
            reads += 1;
            Ok(true)
        });
        if key < MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
            assert_eq!(result, Ok(None));
        } else {
            assert_eq!(result, Err(InternalError {
                limit: MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS,
                observed: MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS + 1,
            }));
        }
    }
    assert_eq!(reads, 3_276);
    println!("hit control: {reads} reads, next distinct lookup rejected");
}
#[test]
fn reproduce_missing_target_lookups_exceeding_the_limit() {
    let mut budget = RelationCommitBudget::default();
    let mut reads = 0;
    for key in 0_u64..=MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS {
        let raw = key.to_be_bytes().to_vec();
        assert_eq!(budget.validate_target_once(raw.clone(), |_| {
            reads += 1;
            Ok(false)
        }), Ok(Some(raw)));
    }
    assert_eq!(reads, 3_277);
    assert_eq!(budget.validated_target_keys.len(), 0);
    println!("REPRODUCED: {reads} distinct missing-target reads accepted, counter stays zero");
}
