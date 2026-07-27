//! Module: query::read_intent
//! Responsibility: diagnostic classification for maintained query terminals.
//! Does not own: planner proof, executor routing, or admission policy.
//! Boundary: describes execution intent without granting authority.

use candid::CandidType;
use serde::Deserialize;

/// Semantic read intent selected by a caller-facing terminal.
///
/// This is diagnostic metadata only. It does not grant access, choose planner
/// routes, or configure admission policy.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum ReadIntentKind {
    /// No semantic read intent was attached to this diagnostic payload.
    #[default]
    Unspecified,

    /// Low-level execution over the effective bounded row window.
    BoundedRowWindow,

    /// Boolean existence check.
    ExistenceCheck,

    /// Request-owned public cursor page.
    PublicPage,

    /// Complete small-set read that fails instead of silently truncating.
    CompleteSmallSet,

    /// Exact aggregate terminal such as count, sum, min, max, or average.
    ExactAggregate,

    /// Trusted/admin cursor batch with engine-owned batch size.
    TrustedAdminBatch,
}
