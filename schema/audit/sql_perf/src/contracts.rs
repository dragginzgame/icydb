//! Wire records shared by the SQL audit actor and its integration harness.
//!
//! These fixture-only records do not own production startup or metrics policy.

#[cfg(test)]
mod tests;

/// Exact schema-application work observed inside one IC message.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct SchemaApplicationPerfResult {
    /// Instructions consumed by this observed operation.
    pub local_instructions: u64,
    /// Schema reconciliation checks performed.
    pub reconcile_checks: u64,
    /// Initial schema creations performed.
    pub first_create: u64,
    /// Reconciliations that matched the accepted schema.
    pub exact_match: u64,
}

/// Pure startup-state work observed inside one IC query message.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct StartupObservationPerfResult {
    /// Observed engine startup state.
    pub state: icydb::db::DatabaseStartupState,
    /// Instructions consumed by this observed operation.
    pub local_instructions: u64,
}

/// Accepted catalog read observed after startup has reopened schema authority.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct AcceptedSchemaReadInstructionResult {
    /// Accepted entity schema returned by the read.
    pub description: icydb::db::EntitySchemaDescription,
    /// Instructions consumed by this observed operation.
    pub local_instructions: u64,
}

/// Canonical instruction and completion evidence returned by the SQL audit
/// canister's generated startup watchdog.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct StartupWatchdogPerfSnapshot {
    /// Scheduler callbacks observed by the timer runtime.
    pub scheduler_samples: u64,
    /// Total instructions consumed by scheduler callbacks.
    pub scheduler_total_instructions: u128,
    /// Maximum instructions consumed by one scheduler callback.
    pub scheduler_maximum_instructions: Option<u64>,
    /// IcyDB work callbacks observed inside scheduler callbacks.
    pub work_samples: u64,
    /// Total instructions consumed by IcyDB work callbacks.
    pub work_total_instructions: u128,
    /// Instructions consumed by the latest IcyDB work callback.
    pub work_latest_instructions: Option<u64>,
    /// Maximum instructions consumed by one IcyDB work callback.
    pub work_maximum_instructions: Option<u64>,
    /// IcyDB work callbacks that started.
    pub work_started: u64,
    /// IcyDB work callbacks that completed.
    pub work_completed: u64,
    /// Successful IcyDB work callbacks.
    pub succeeded: u64,
    /// Callbacks that completed normally while startup dependencies were pending.
    pub no_work: u64,
    /// Retryable IcyDB work callback failures.
    pub retryable_failures: u64,
    /// Invariant-failing IcyDB work callbacks.
    pub invariant_failures: u64,
}

/// Application lifecycle entry that began the current readiness observation.
#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum ApplicationStartupHook {
    /// Canister initialization.
    Init,
    /// Canister post-upgrade restoration.
    PostUpgrade,
}

/// Application-owned readiness and restoration evidence.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct ApplicationStartupSnapshot {
    /// Lifecycle hook that started this observation.
    pub hook: ApplicationStartupHook,
    /// Whether engine startup was registered before the application hook.
    pub engine_registered_before_hook: bool,
    /// Readiness observations attempted.
    pub observations: u32,
    /// Observations that found recovery pending.
    pub recovering_observations: u32,
    /// Observations that found the database ready.
    pub ready_observations: u32,
    /// Application restorations completed after readiness.
    pub restorations: u32,
    /// Whether another application readiness observation is scheduled.
    pub retry_scheduled: bool,
    /// Terminal startup failure, when present.
    pub failure: Option<icydb::db::StartupFailure>,
}

///
/// ScalePayloadProfile
///
/// Exact blob-payload distribution loaded by one SQL scale fixture.
/// Owned by the audit canister and returned to the host as fixture evidence.
///

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum ScalePayloadProfile {
    /// The selected surface has no blob payload fields.
    #[serde(rename = "not_applicable")]
    NotApplicable,

    /// Thumbnail lengths cycle through 32/64/128/256 bytes and chunk lengths
    /// cycle through 256/512/1,024/2,048 bytes.
    #[serde(rename = "blob_cycle_v1")]
    BlobCycleV1,
}

///
/// ScaleFixtureFacts
///
/// Realized deterministic distribution facts for one loaded scale surface.
/// Owned by the audit canister and validated by the host before sampling.
///

#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct ScaleFixtureFacts {
    /// Current hard-cut scale-fixture format version.
    pub profile_version: u32,

    /// Stable audit surface name loaded into the otherwise-empty canister.
    pub surface: String,

    /// Exact number of rows constructed and inserted for the surface.
    pub fixture_rows: u32,

    /// Rows matching the surface's declared impossible predicate.
    pub zero_match_rows: u32,

    /// Rows matching the surface's declared exact-key predicate.
    pub one_match_rows: u32,

    /// Rows matching the surface's declared quarter-selectivity predicate.
    pub quarter_match_rows: u32,

    /// Rows matching the surface's declared all-row predicate.
    pub all_match_rows: u32,

    /// Exact blob payload distribution, or typed non-applicability.
    pub payload_profile: ScalePayloadProfile,
}
