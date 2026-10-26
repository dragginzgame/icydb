//! Wire contracts for application-owned audit/test methods.
//!
//! The table is shared by raw fixture exports, artifact admission and native
//! callers. IcyDB's generated endpoints retain their product-owned contracts.

use canic_icydb::wire::Endpoint;
use ic_wire::Error;
use icydb::wire::{MAX_REPLY_BYTES, MAX_REQUEST_BYTES};
use icydb_schema::wire::limits;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Installed controls for post-handler reply failure and ordinary application errors.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WireRollbackAction {
    /// Commit one counter increment and return its value.
    Commit,
    /// Return a typed application error without changing the counter.
    ApplicationError,
    /// Increment the counter, then fail while serializing its reply.
    EncodingFailure,
}

/// Stable identity for the current fixture-owned byte contract.
#[must_use]
pub const fn method_id(name: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let bytes = name.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
        index += 1;
    }
    hash ^ 0x4943_5944_4257_4631
}

macro_rules! contracts {
    ($($marker:ident => ($name:literal, $query:literal)),* $(,)?) => {
        /// Declared fixture method names and their query/update modes.
        pub const METHODS: &[(&str, bool)] = &[$(($name, $query)),*];
        $(
            /// Identity of one fixture-owned method.
            pub struct $marker;
            impl $marker {
                /// Stable native frame identity.
                pub const ID: u64 = method_id($name);
            }
        )*
        /// Encode fixture or generated arguments through their current contract.
        pub fn encode_request<Q: Serialize>(name: &str, request: Q) -> Result<Vec<u8>, Error> {
            if let Some(endpoint) = Endpoint::from_name(name) { return endpoint.encode_request(request, icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)); }
            let id = match name {
                $($name => $marker::ID,)*
                _ => return Err(Error::Identity),
            };
            ic_wire::native::encode_request(id, &request, limits(MAX_REQUEST_BYTES))
        }
        /// Decode one fixture or generated request after resource admission.
        pub fn decode_request<Q: DeserializeOwned>(name: &str, bytes: &[u8]) -> Result<Q, Error> {
            if let Some(endpoint) = Endpoint::from_name(name) { return endpoint.decode_request(bytes, icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)); }
            match name {
                $($name => ic_wire::native::decode_request($marker::ID, bytes, limits(MAX_REQUEST_BYTES)),)*
                _ => Err(Error::Identity),
            }
        }
        /// Encode a fixture or generated response, including application errors.
        pub fn encode_response<R: Serialize>(name: &str, response: R) -> Result<Vec<u8>, Error> {
            if let Some(endpoint) = Endpoint::from_name(name) { return endpoint.encode_response(response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES)); }
            let id = match name {
                $($name => $marker::ID,)*
                _ => return Err(Error::Identity),
            };
            ic_wire::native::encode_response(id, &response, limits(MAX_REPLY_BYTES))
        }
        /// Decode a fixture or generated response after resource admission.
        pub fn decode_response<R: DeserializeOwned>(name: &str, bytes: &[u8]) -> Result<R, Error> {
            if let Some(endpoint) = Endpoint::from_name(name) { return endpoint.decode_response(bytes, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES)); }
            match name {
                $($name => ic_wire::native::decode_response($marker::ID, bytes, limits(MAX_REPLY_BYTES)),)*
                _ => Err(Error::Identity),
            }
        }
    };
}

contracts! {
    OpenDatabase => ("open_database", false),
    InsertProfile => ("insert_profile", false),
    ProfileRank => ("profile_rank", true),
    MeasureRelationCalibration => ("measure_relation_calibration", false),
    MeasureRelationTraversal => ("measure_relation_traversal", true),
    ExerciseRelationFlow => ("exercise_relation_flow", false),
    ExerciseShallowRelationFlow => ("exercise_shallow_relation_flow", false),
    ExerciseRepeatedRelationFlow => ("exercise_repeated_relation_flow", false),
    CommitRepeatedRelationBoundary => ("commit_repeated_relation_boundary", false),
    RequestFuture00 => ("request_future_00", true),
    RequestFuture01 => ("request_future_01", true),
    RequestFuture02 => ("request_future_02", true),
    RequestFuture03 => ("request_future_03", true),
    RequestFuture04 => ("request_future_04", true),
    RequestFuture05 => ("request_future_05", true),
    RequestFuture06 => ("request_future_06", true),
    RequestFuture07 => ("request_future_07", true),
    RequestFuture08 => ("request_future_08", true),
    RequestFuture09 => ("request_future_09", true),
    RequestFuture10 => ("request_future_10", true),
    RequestFuture11 => ("request_future_11", true),
    RequestFuture12 => ("request_future_12", true),
    RequestFuture13 => ("request_future_13", true),
    RequestFuture14 => ("request_future_14", true),
    RequestFuture15 => ("request_future_15", true),
    RequestFuture16 => ("request_future_16", true),
    RequestFuture17 => ("request_future_17", true),
    RequestFuture18 => ("request_future_18", true),
    RequestFuture19 => ("request_future_19", true),
    RequestFuture20 => ("request_future_20", true),
    RequestFuture21 => ("request_future_21", true),
    RequestFuture22 => ("request_future_22", true),
    RequestFuture23 => ("request_future_23", true),
    RequestFuture24 => ("request_future_24", true),
    RequestFuture25 => ("request_future_25", true),
    RequestFuture26 => ("request_future_26", true),
    RequestFuture27 => ("request_future_27", true),
    RequestFuture28 => ("request_future_28", true),
    RequestFuture29 => ("request_future_29", true),
    RequestFuture30 => ("request_future_30", true),
    RequestFuture31 => ("request_future_31", true),
    RequestFuture32 => ("request_future_32", true),
    RequestFuture33 => ("request_future_33", true),
    RequestFuture34 => ("request_future_34", true),
    RequestFuture35 => ("request_future_35", true),
    RequestFuture36 => ("request_future_36", true),
    RequestFuture37 => ("request_future_37", true),
    RequestFuture38 => ("request_future_38", true),
    RequestFuture39 => ("request_future_39", true),
    RequestFuture40 => ("request_future_40", true),
    RequestFuture41 => ("request_future_41", true),
    RequestFuture42 => ("request_future_42", true),
    RequestFuture43 => ("request_future_43", true),
    RequestFuture44 => ("request_future_44", true),
    RequestFuture45 => ("request_future_45", true),
    RequestFuture46 => ("request_future_46", true),
    RequestFuture47 => ("request_future_47", true),
    RequestFuture48 => ("request_future_48", true),
    RequestFuture49 => ("request_future_49", true),
    RequestFuture50 => ("request_future_50", true),
    RequestFuture51 => ("request_future_51", true),
    RequestFuture52 => ("request_future_52", true),
    RequestFuture53 => ("request_future_53", true),
    RequestFuture54 => ("request_future_54", true),
    RequestFuture55 => ("request_future_55", true),
    RequestFuture56 => ("request_future_56", true),
    RequestFuture57 => ("request_future_57", true),
    RequestFuture58 => ("request_future_58", true),
    RequestFuture59 => ("request_future_59", true),
    RequestFuture60 => ("request_future_60", true),
    RequestFuture61 => ("request_future_61", true),
    RequestFuture62 => ("request_future_62", true),
    RequestFuture63 => ("request_future_63", true),
    AcceptedSchemaDescriptions => ("accepted_schema_descriptions", true),
    AcknowledgeCollectionMutationScaleJob => ("acknowledge_collection_mutation_scale_job", false),
    AdvanceCollectionMutationScaleJob => ("advance_collection_mutation_scale_job", false),
    AdvanceJournaledUserMutationJob => ("advance_journaled_user_mutation_job", false),
    Allocations => ("allocations", true),
    AppendPromotionIndexFixturePage => ("append_promotion_index_fixture_page", false),
    ApplicationStartupContract => ("application_startup_contract", true),
    AuditMetricsQueryTrap => ("audit_metrics_query_trap", true),
    CancelJournaledUserMutationJob => ("cancel_journaled_user_mutation_job", false),
    CheckBoundSqlNullParity => ("check_bound_sql_null_parity", true),
    CheckCreatedQuest => ("check_created_quest", true),
    CheckCreatedQuestTargetDelete => ("check_created_quest_target_delete", false),
    CheckEntityRenameBindings => ("check_entity_rename_bindings", true),
    CheckEntityRenameDeletes => ("check_entity_rename_deletes", false),
    CollectionMutationScaleFact => ("collection_mutation_scale_fact", true),
    CollectionMutationScaleJobState => ("collection_mutation_scale_job_state", false),
    ControlFrame => ("control_frame", true),
    DdlGroupPath => ("ddl_group_path", false),
    EntityCreationLifecycleMetrics => ("entity_creation_lifecycle_metrics", true),
    EntityCreationStartupSnapshot => ("entity_creation_startup_snapshot", true),
    ExerciseReachableEntityRead => ("exercise_reachable_entity_read", true),
    ExerciseReachableEntityWrite => ("exercise_reachable_entity_write", false),
    InitializeStartupObservationFixture => ("initialize_startup_observation_fixture", false),
    InsertKeep => ("insert_keep", false),
    InsertRetiring => ("insert_retiring", false),
    KeepRowExists => ("keep_row_exists", true),
    LifecycleCompositionSnapshot => ("lifecycle_composition_snapshot", true),
    LifecycleDatabaseProbe => ("lifecycle_database_probe", true),
    LifecycleInsertProbeRow => ("lifecycle_insert_probe_row", false),
    LifecycleProbeRowExists => ("lifecycle_probe_row_exists", true),
    LoadAccountScaleFixture => ("load_account_scale_fixture", false),
    LoadBlobScaleFixture => ("load_blob_scale_fixture", false),
    LoadCardinalityProbeBoundaryFixture => ("load_cardinality_probe_boundary_fixture", false),
    LoadCardinalityTiebreakFixture => ("load_cardinality_tiebreak_fixture", false),
    LoadCollectionMutationScalePage => ("load_collection_mutation_scale_page", false),
    LoadConvergenceCloseoutDebt => ("load_convergence_closeout_debt", false),
    LoadGroupPathFixture => ("load_group_path_fixture", false),
    LoadHeapUserScaleFixture => ("load_heap_user_scale_fixture", false),
    LoadJointFanoutBoundaryFixture => ("load_joint_fanout_boundary_fixture", false),
    LoadJointThreeIndexBoundaryFixture => ("load_joint_three_index_boundary_fixture", false),
    LoadJointZeroIndexBoundaryFixture => ("load_joint_zero_index_boundary_fixture", false),
    LoadJournaledReentryProbeFixture => ("load_journaled_reentry_probe_fixture", false),
    LoadJournaledUserMutationByteFixture => ("load_journaled_user_mutation_byte_fixture", false),
    LoadJournaledUserScaleFixture => ("load_journaled_user_scale_fixture", false),
    LoadPatch1WideRowRecoveryFixture => ("load_patch1_wide_row_recovery_fixture", false),
    LoadRelationIntegrityFixture => ("load_relation_integrity_fixture", false),
    LoadSeekIntersectionFixture => ("load_seek_intersection_fixture", false),
    LoadStreamingExecutionContinuationFixture => ("load_streaming_execution_continuation_fixture", false),
    LoadStreamingExecutionFixture => ("load_streaming_execution_fixture", false),
    LoadTokenScaleFixture => ("load_token_scale_fixture", false),
    LoadUserScaleFixture => ("load_user_scale_fixture", false),
    LoadUserUniqueAgeScaleFixture => ("load_user_unique_age_scale_fixture", false),
    MeasureAcceptedSchemaReadInstructions => ("measure_accepted_schema_read_instructions", true),
    MeasureApplicationBehaviorPerf => ("measure_application_behavior_perf", true),
    MeasureBatchWorkload => ("measure_batch_workload", false),
    MeasureCatalogLabels => ("measure_catalog_labels", false),
    MeasureCatalogWorkload => ("measure_catalog_workload", false),
    MeasureCollectionWorkload => ("measure_collection_workload", false),
    MeasureDormantConvergenceCandidate => ("measure_dormant_convergence_candidate", false),
    MeasureDynamicKeyLoop => ("measure_dynamic_key_loop", true),
    MeasureEntityCreationSchemaApplication => ("measure_entity_creation_schema_application", false),
    MeasureEntityCreationStartupStep => ("measure_entity_creation_startup_step", false),
    MeasureExactKeyBatch => ("measure_exact_key_batch", true),
    MeasureHeapUserSqlWriteMaterializationPerf => ("measure_heap_user_sql_write_materialization_perf", false),
    MeasureHeapUserWriteMatrixPerf => ("measure_heap_user_write_matrix_perf", false),
    MeasureIdentityCloseoutPerf => ("measure_identity_closeout_perf", false),
    MeasureIntegritySqlPerf => ("measure_integrity_sql_perf", false),
    MeasureJournaledReentryPerf => ("measure_journaled_reentry_perf", false),
    MeasureJournaledUserCheckedWritePerf => ("measure_journaled_user_checked_write_perf", false),
    MeasureJournaledUserConstraintWritePerf => ("measure_journaled_user_constraint_write_perf", false),
    MeasureJournaledUserMutationForwardPerf => ("measure_journaled_user_mutation_forward_perf", false),
    MeasureJournaledUserSqlWriteMaterializationPerf => ("measure_journaled_user_sql_write_materialization_perf", false),
    MeasureJournaledUserWriteMatrixPerf => ("measure_journaled_user_write_matrix_perf", false),
    MeasureParameterizedPointQueries => ("measure_parameterized_point_queries", true),
    MeasureRepeatedGroupedQueries => ("measure_repeated_grouped_queries", true),
    MeasureRepeatedPointQueries => ("measure_repeated_point_queries", true),
    MeasureRepeatedScanQueries => ("measure_repeated_scan_queries", true),
    MeasureSchemaApplicationQuery => ("measure_schema_application_query", true),
    MeasureSchemaApplicationUpdate => ("measure_schema_application_update", false),
    MeasureSeekIntersectionPage => ("measure_seek_intersection_page", false),
    MeasureSqlDdlAdmissionInstructions => ("measure_sql_ddl_admission_instructions", false),
    MeasureSqlExplain => ("measure_sql_explain", true),
    MeasureSqlQueryInstructions => ("measure_sql_query_instructions", true),
    MeasureStartupObservation => ("measure_startup_observation", true),
    MeasureTrustedSqlExactUpdateInstructions => ("measure_trusted_sql_exact_update_instructions", false),
    MeasureTypedExplain => ("measure_typed_explain", true),
    MeasureTypedFilterPreparation => ("measure_typed_filter_preparation", false),
    MutateCardinalityCloseoutIndex => ("mutate_cardinality_closeout_index", false),
    MutateGroupPath => ("mutate_group_path", false),
    ObserveApplicationStartupForTests => ("observe_application_startup_for_tests", false),
    ProgressJobInventoryPerf => ("progress_job_inventory_perf", false),
    PublishPromotionIndexFixture => ("publish_promotion_index_fixture", false),
    QueryBlobWithPerf => ("query_blob_with_perf", true),
    QueryCardinalityTiebreakLivePageWithPerf => ("query_cardinality_tiebreak_live_page_with_perf", true),
    QueryGroupPath => ("query_group_path", true),
    QueryHeapUserWithPerf => ("query_heap_user_with_perf", true),
    QueryJournaledUserWithPerf => ("query_journaled_user_with_perf", true),
    QueryOneEntitySql => ("query_one_entity_sql", true),
    QueryOneEntityTypedLifecycleRows => ("query_one_entity_typed_lifecycle_rows", true),
    QueryOneEntityTypedRows => ("query_one_entity_typed_rows", true),
    QueryStreamingExecutionExhaustivePage => ("query_streaming_execution_exhaustive_page", true),
    QueryStreamingExecutionLivePage => ("query_streaming_execution_live_page", true),
    QueryStreamingExecutionLivePageDriverProbe => ("query_streaming_execution_live_page_driver_probe", true),
    QueryStreamingExecutionPublicLivePageDriverProbe => ("query_streaming_execution_public_live_page_driver_probe", true),
    QueryTenEntityTypedRows => ("query_ten_entity_typed_rows", true),
    QueryTokenWithPerf => ("query_token_with_perf", true),
    QueryUser => ("query_user", true),
    QueryUserDistinctBudgetFallbackWithPerf => ("query_user_distinct_budget_fallback_with_perf", true),
    QueryUserLoopWithPerf => ("query_user_loop_with_perf", true),
    QueryUserNumericBudgetFallbackWithPerf => ("query_user_numeric_budget_fallback_with_perf", true),
    QueryUserRangeBudgetFallbackWithPerf => ("query_user_range_budget_fallback_with_perf", true),
    QueryUserWithPerf => ("query_user_with_perf", true),
    ReadAuthorizationCost => ("read_authorization_cost", true),
    RecoverCollectionMutationScaleStore => ("recover_collection_mutation_scale_store", false),
    RejectJointFanoutOverBoundaryFixture => ("reject_joint_fanout_over_boundary_fixture", false),
    RestoreControlFrame => ("restore_control_frame", false),
    RetainedSqlRead => ("retained_sql_read", true),
    RetiringRowExists => ("retiring_row_exists", true),
    RetryConvergenceCloseoutRow => ("retry_convergence_closeout_row", false),
    SeedBatchWorkload => ("seed_batch_workload", false),
    SeedCatalogWorkload => ("seed_catalog_workload", false),
    SeedCollectionWorkload => ("seed_collection_workload", false),
    SeedConvergenceMetricsBacklog => ("seed_convergence_metrics_backlog", false),
    SeedEntityCreationTarget => ("seed_entity_creation_target", false),
    SeedEntityRename => ("seed_entity_rename", false),
    SeedOversizedSqlGroupName => ("seed_oversized_sql_group_name", false),
    SetSchemaReaderEnabled => ("set_schema_reader_enabled", false),
    SetSqlReaderEnabled => ("set_sql_reader_enabled", false),
    StartCollectionMutationScaleJob => ("start_collection_mutation_scale_job", false),
    StartJournaledUserMutationJob => ("start_journaled_user_mutation_job", false),
    StartupWatchdogArmed => ("startup_watchdog_armed", true),
    StartupWatchdogPerfSnapshot => ("startup_watchdog_perf_snapshot", true),
    State => ("state", true),
    Step => ("step", false),
    TrapAfterCompleteStartupRecovery => ("trap_after_complete_startup_recovery", false),
    TryCollectionEagerTierReset => ("try_collection_eager_tier_reset", false),
    UpdateJournaledUserAfterMutationJobStart => ("update_journaled_user_after_mutation_job_start", false),
    ValidateJournaledUserPerfCheck => ("validate_journaled_user_perf_check", false),
    VerifyJournaledUserMutationJobLifecycle => ("verify_journaled_user_mutation_job_lifecycle", false),
    WarmUserQueryWithPerf => ("warm_user_query_with_perf", false),
    WireRollbackApply => ("wire_rollback_apply", false),
    WireRollbackState => ("wire_rollback_state", true),
    WriteCreatedQuest => ("write_created_quest", false),
}

/// Serve a fixture method through the same raw admission owner as generated endpoints.
///
/// # Panics
/// Panics when called outside an IC canister entry point.
pub fn serve<Q, R>(id: u64, handler: impl FnOnce(Q) -> R)
where
    Q: DeserializeOwned,
    R: Serialize,
{
    // Keep runtime name dispatch on the native caller side. Each Wasm export
    // instantiates only its own request decoder and identity prefix.
    canic_icydb::serve_raw(
        MAX_REQUEST_BYTES,
        |bytes| ic_wire::native::decode_request(id, bytes, limits(MAX_REQUEST_BYTES)),
        |response| ic_wire::native::encode_response(id, &response, limits(MAX_REPLY_BYTES)),
        handler,
    );
}

/// Serve an asynchronous fixture handler using Canic's bounded transport.
///
/// # Panics
/// Panics outside an IC entrypoint; reply failure traps the current message.
pub async fn serve_async<Q, R, F>(id: u64, handler: impl FnOnce(Q) -> F)
where
    Q: DeserializeOwned,
    R: Serialize,
    F: std::future::Future<Output = R>,
{
    canic_icydb::serve_raw_async(
        MAX_REQUEST_BYTES,
        |bytes| ic_wire::native::decode_request(id, bytes, limits(MAX_REQUEST_BYTES)),
        |response| ic_wire::native::encode_response(id, &response, limits(MAX_REPLY_BYTES)),
        handler,
    )
    .await;
}

/// Declare one raw fixture export alongside its ordinary native Rust handler.
#[macro_export]
macro_rules! export_wire {
    ($(#[$cfg:meta])* $mode:ident, $name:literal, $marker:ident, $function:ident, ($($arg:ident: $ty:ty),* $(,)?), $return:ty) => {
        canic_icydb::wire_endpoint! {
            $(#[$cfg])*
            $mode, $name,
            $crate::fixture_wire::$marker::ID,
            {
                $crate::fixture_wire::serve::<_, _>($crate::fixture_wire::$marker::ID, |($($arg,)*): ($($ty,)*)| -> $return { $function($($arg),*) });
            }
        }
    };
}
