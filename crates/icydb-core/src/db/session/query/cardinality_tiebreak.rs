//! Module: db::session::query::cardinality_tiebreak
//! Responsibility: bind optional exact-prefix evidence to one canonical query plan.
//! Does not own: candidate ranking, cardinality maintenance, cache policy, or execution.
//! Boundary: final planner tie set + store evidence -> one advisory plan selection.

#[cfg(all(test, feature = "sql"))]
mod lowering_tests;
#[cfg(all(test, feature = "sql"))]
mod probe_tests;
#[cfg(all(test, feature = "sql"))]
mod ranking_tests;

use crate::db::query::preparation::PreparationWork;

use crate::{
    db::{
        DbSession, QueryError,
        access::{LoweredAccessError, SemanticIndexAccessContract, lower_access_with_schema_info},
        executor::EntityAuthority,
        index::{IndexId, RawIndexStoreKey, UserIndexPrefixCardinalityKey},
        query::plan::{
            AccessPlannedQuery, CardinalityTiebreakCandidate, CardinalityTiebreakCandidateEvidence,
            CardinalityTiebreakRoutePin, CardinalityTiebreakState,
            ExactCardinalityTiebreakEvidence, apply_exact_cardinality_tiebreak_selection,
            exact_cardinality_tiebreak_candidates,
        },
        registry::ExactUserIndexPrefixEvidence,
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    traits::CanisterKind,
    types::EntityTag,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

const MAX_CARDINALITY_TIEBREAK_CANDIDATES: usize = 64;
const MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES: usize = 256;
const MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES: usize = 4 * 1024 * 1024;
const MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE: usize = 16;
const MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES: usize = 1024 * 1024;

struct PreparedCardinalityCandidate {
    candidate: CardinalityTiebreakCandidate,
    probe_start: usize,
    probe_end: usize,
}

// Candidate probe ranges and their single ordered key buffer travel together.
type PreparedCardinalityProbes = (
    Vec<PreparedCardinalityCandidate>,
    Vec<UserIndexPrefixCardinalityKey>,
);

enum CardinalityTiebreakAttempt {
    Exact {
        selected: CardinalityTiebreakCandidate,
        evidence: ExactCardinalityTiebreakEvidence,
    },
    Unavailable(crate::db::registry::ExactPrefixCardinalityLifecycleStamp),
    PolicyFallback,
}

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn apply_exact_cardinality_tiebreak(
        &self,
        authority: &EntityAuthority,
        semantic_indexes: &[SemanticIndexAccessContract],
        plan: AccessPlannedQuery,
        work: &PreparationWork<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?;
        let Some(candidates) =
            exact_cardinality_tiebreak_candidates(semantic_indexes, schema_info, &plan)
        else {
            return Ok(plan);
        };
        let fallback_route_pin =
            exact_selected_route_pin(candidates.as_slice(), &plan, authority.entity_tag())
                .ok_or_else(QueryError::invariant)?;

        let (selected_access, state) =
            match self.cardinality_tiebreak_attempt(authority, candidates, work)? {
                CardinalityTiebreakAttempt::Exact { selected, evidence } => (
                    Some(selected.into_access()),
                    CardinalityTiebreakState::ExactAtSelection(evidence),
                ),
                CardinalityTiebreakAttempt::Unavailable(lifecycle_stamp) => (
                    None,
                    CardinalityTiebreakState::Unavailable {
                        lifecycle_stamp,
                        route_pin: fallback_route_pin,
                    },
                ),
                CardinalityTiebreakAttempt::PolicyFallback => (
                    None,
                    CardinalityTiebreakState::PolicyFallback(fallback_route_pin),
                ),
            };

        let mut plan = apply_exact_cardinality_tiebreak_selection(
            plan,
            selected_access,
            state,
            schema_info,
            work,
        )?;
        plan.finalize_access_choice_with_semantic_indexes_and_schema(semantic_indexes, schema_info);

        Ok(plan)
    }

    pub(super) fn apply_pinned_cardinality_tiebreak(
        authority: &EntityAuthority,
        semantic_indexes: &[SemanticIndexAccessContract],
        plan: AccessPlannedQuery,
        route_pin: CardinalityTiebreakRoutePin,
        work: &PreparationWork<'_>,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?;
        let Some(candidates) =
            exact_cardinality_tiebreak_candidates(semantic_indexes, schema_info, &plan)
        else {
            return Ok(None);
        };
        let Some(selected) = exactly_one_matching_candidate(candidates, |candidate| {
            candidate.route_pin(authority.entity_tag()) == Some(route_pin)
        }) else {
            return Ok(None);
        };

        let mut plan = apply_exact_cardinality_tiebreak_selection(
            plan,
            Some(selected.into_access()),
            CardinalityTiebreakState::PinnedContinuation(route_pin),
            schema_info,
            work,
        )?;
        plan.finalize_access_choice_with_semantic_indexes_and_schema(semantic_indexes, schema_info);

        Ok(Some(plan))
    }

    fn cardinality_tiebreak_attempt(
        &self,
        authority: &EntityAuthority,
        candidates: Vec<CardinalityTiebreakCandidate>,
        work: &PreparationWork<'_>,
    ) -> Result<CardinalityTiebreakAttempt, QueryError> {
        if !cardinality_candidate_count_is_admitted(candidates.len()) {
            return Ok(CardinalityTiebreakAttempt::PolicyFallback);
        }
        let store = self
            .db
            .recovered_store(authority.store_path())
            .map_err(QueryError::execute)?;
        let accepted_schema = authority
            .accepted_schema_authority()
            .map_err(QueryError::execute)?;
        let accepted_root = CardinalityAcceptedRootIdentity::new(
            accepted_schema.revision(),
            accepted_schema.fingerprint(),
        )
        .map_err(QueryError::execute)?;
        let database_incarnation = authority
            .accepted_runtime_root_identity()
            .database_incarnation();
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?;
        let Some((prepared, keys)) =
            prepare_cardinality_candidates(authority.entity_tag(), schema_info, candidates, work)?
        else {
            return Ok(CardinalityTiebreakAttempt::PolicyFallback);
        };

        let counts = match store.exact_user_index_prefix_evidence_for_admitted_root(
            database_incarnation,
            accepted_root,
            keys.as_slice(),
        ) {
            ExactUserIndexPrefixEvidence::Exact(counts) if counts.len() == keys.len() => counts,
            ExactUserIndexPrefixEvidence::Exact(_) => {
                return Ok(CardinalityTiebreakAttempt::PolicyFallback);
            }
            ExactUserIndexPrefixEvidence::Unavailable(stamp) => {
                return Ok(CardinalityTiebreakAttempt::Unavailable(stamp));
            }
        };
        let Some((selected, evidence)) = rank_prepared_cardinality_candidates(
            authority.entity_tag(),
            prepared,
            counts.as_slice(),
            work,
        )?
        else {
            return Ok(CardinalityTiebreakAttempt::PolicyFallback);
        };

        Ok(CardinalityTiebreakAttempt::Exact { selected, evidence })
    }
}

// Admit every candidate's prefix count before constructing encoded bounds or
// output buffers. Shape facts are constant-size projections, not value walks.
fn admitted_cardinality_probe_count(
    candidates: &[CardinalityTiebreakCandidate],
    work: &PreparationWork<'_>,
) -> Result<Option<usize>, QueryError> {
    if !cardinality_candidate_count_is_admitted(candidates.len()) {
        return Ok(None);
    }
    let mut total = 0;
    for candidate in candidates {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(path) = candidate.access().as_path() else {
            return Ok(None);
        };
        let probes = path.shape_facts().index_prefix_spec_count();
        if probes == 0 {
            return Ok(None);
        }
        let Some((admitted, _)) = admit_cardinality_candidate_shape(total, probes, 0, 0, 0) else {
            return Ok(None);
        };
        total = admitted;
    }
    Ok(Some(total))
}

fn prepare_cardinality_candidates(
    entity_tag: EntityTag,
    schema_info: &crate::db::schema::SchemaInfo,
    candidates: Vec<CardinalityTiebreakCandidate>,
    work: &PreparationWork<'_>,
) -> Result<Option<PreparedCardinalityProbes>, QueryError> {
    let Some(admitted_probes) = admitted_cardinality_probe_count(&candidates, work)? else {
        return Ok(None);
    };
    let mut total_probes = 0usize;
    let mut total_lowered_bytes = 0usize;
    let mut prepared = work.vec_with_capacity(candidates.len())?;
    let mut keys = work.vec_with_capacity(admitted_probes)?;

    for candidate in candidates {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let lowered = match lower_access_with_schema_info(
            entity_tag,
            candidate.access(),
            schema_info,
            work,
        ) {
            Ok(lowered) => lowered,
            Err(LoweredAccessError::IndexPrefix | LoweredAccessError::IndexRange) => {
                return Ok(None);
            }
            Err(LoweredAccessError::Construction(error)) => return Err(QueryError::execute(error)),
        };
        let (prefix_specs, range_specs) = lowered.into_index_specs();
        if !range_specs.is_empty() || prefix_specs.is_empty() {
            return Ok(None);
        }
        let Some((component_bytes, transient_bytes)) =
            cardinality_lowered_bytes(&prefix_specs, work)?
        else {
            return Ok(None);
        };
        let Some(admitted) = admit_cardinality_candidate_shape(
            total_probes,
            prefix_specs.len(),
            total_lowered_bytes,
            component_bytes,
            transient_bytes,
        ) else {
            return Ok(None);
        };
        (total_probes, total_lowered_bytes) = admitted;
        // Do not grow the charged output buffer if lowered shape disagrees
        // with the semantic count admitted before encoding.
        if total_probes > admitted_probes {
            return Ok(None);
        }

        let index_id = IndexId::new_with_generation(
            entity_tag,
            candidate.index().ordinal(),
            candidate.index().physical_generation(),
        );
        let probe_start = keys.len();
        for spec in prefix_specs {
            let key = UserIndexPrefixCardinalityKey::new(index_id, spec.into_prefix_components());
            for prior in &keys {
                if cardinality_probe_keys_equal(prior, &key, work)? {
                    return Ok(None);
                }
            }
            keys.push(key);
        }
        prepared.push(PreparedCardinalityCandidate {
            candidate,
            probe_start,
            probe_end: keys.len(),
        });
    }
    if total_probes != admitted_probes {
        return Ok(None);
    }

    Ok(Some((prepared, keys)))
}

// Inspect encoded sizes once. Encoding itself is a separate, still-unmetered
// owner; these observations enforce the existing post-encoding byte policy.
fn cardinality_lowered_bytes(
    specs: &[crate::db::access::LoweredIndexPrefixSpec],
    work: &PreparationWork<'_>,
) -> Result<Option<(usize, usize)>, QueryError> {
    let mut components = 0usize;
    let mut bounds = 0usize;
    for spec in specs {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        for component in spec.prefix_components() {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            let Some(next) = components.checked_add(component.len()) else {
                return Ok(None);
            };
            components = next;
        }
        let Ok((lower, upper)) = spec.raw_bounds() else {
            return Ok(None);
        };
        let Some(next) = bounds
            .checked_add(RawIndexStoreKey::bound_backing_bytes(lower))
            .and_then(|total| total.checked_add(RawIndexStoreKey::bound_backing_bytes(upper)))
        else {
            return Ok(None);
        };
        bounds = next;
    }
    Ok(components
        .checked_add(bounds)
        .map(|transient| (components, transient)))
}

// Index identity short-circuits unrelated keys. For matching identities,
// admit each component's byte-comparison allowance before slice equality.
fn cardinality_probe_keys_equal(
    left: &UserIndexPrefixCardinalityKey,
    right: &UserIndexPrefixCardinalityKey,
    work: &PreparationWork<'_>,
) -> Result<bool, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if left.index_id() != right.index_id() {
        return Ok(false);
    }
    let left = left.prefix_components();
    let right = right.prefix_components();
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        if left.len() != right.len() {
            return Ok(false);
        }
        work.charge(Resource::PredicateExpressionSteps, left.len() as u64)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
fn rank_prepared_cardinality_candidates(
    entity_tag: EntityTag,
    prepared: Vec<PreparedCardinalityCandidate>,
    counts: &[u64],
    work: &PreparationWork<'_>,
) -> Result<
    Option<(
        CardinalityTiebreakCandidate,
        ExactCardinalityTiebreakEvidence,
    )>,
    QueryError,
> {
    let mut evidence = work.vec_with_capacity(prepared.len())?;
    let mut selected: Option<(CardinalityTiebreakCandidate, u64)> = None;
    for prepared_candidate in prepared {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(prefix_counts) =
            counts.get(prepared_candidate.probe_start..prepared_candidate.probe_end)
        else {
            return Ok(None);
        };
        let Some(exact_prefix_entries) = checked_exact_prefix_entries(prefix_counts, work)? else {
            return Ok(None);
        };
        evidence.push(CardinalityTiebreakCandidateEvidence::new(
            work.copy_text(prepared_candidate.candidate.index().name())?,
            exact_prefix_entries,
        ));
        // Keep the first candidate at the minimum, matching the planner's
        // deterministic tie order. Only the winner needs retained access data.
        let replace = if let Some((_, minimum)) = selected.as_ref() {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            exact_prefix_entries < *minimum
        } else {
            true
        };
        if replace {
            selected = Some((prepared_candidate.candidate, exact_prefix_entries));
        }
    }

    let Some((selected, _)) = selected else {
        return Ok(None);
    };
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    let Some(route_pin) = selected.route_pin(entity_tag) else {
        return Ok(None);
    };

    Ok(Some((
        selected,
        ExactCardinalityTiebreakEvidence::new(route_pin, evidence),
    )))
}

fn exact_selected_route_pin(
    candidates: &[CardinalityTiebreakCandidate],
    plan: &AccessPlannedQuery,
    entity_tag: EntityTag,
) -> Option<CardinalityTiebreakRoutePin> {
    let mut matching = candidates
        .iter()
        .filter(|candidate| candidate.access() == &plan.access)
        .filter_map(|candidate| candidate.route_pin(entity_tag));
    let selected = matching.next()?;

    matching.next().is_none().then_some(selected)
}

// A pinned cursor is valid only when its authenticated route identifies one
// and only one member of the normally eligible final tie set.
fn exactly_one_matching_candidate<T>(
    candidates: impl IntoIterator<Item = T>,
    mut is_match: impl FnMut(&T) -> bool,
) -> Option<T> {
    let mut matching = candidates
        .into_iter()
        .filter(|candidate| is_match(candidate));
    let selected = matching.next()?;

    matching.next().is_none().then_some(selected)
}

const fn cardinality_candidate_count_is_admitted(candidate_count: usize) -> bool {
    candidate_count <= MAX_CARDINALITY_TIEBREAK_CANDIDATES
}

fn admit_cardinality_candidate_shape(
    total_probes: usize,
    candidate_probes: usize,
    total_lowered_bytes: usize,
    candidate_component_bytes: usize,
    candidate_transient_bytes: usize,
) -> Option<(usize, usize)> {
    if candidate_probes > MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE
        || candidate_transient_bytes > MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES
    {
        return None;
    }
    let admitted_probes = total_probes.checked_add(candidate_probes)?;
    let admitted_lowered_bytes = total_lowered_bytes.checked_add(candidate_component_bytes)?;
    if admitted_probes > MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES
        || admitted_lowered_bytes > MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES
    {
        return None;
    }

    Some((admitted_probes, admitted_lowered_bytes))
}

fn checked_exact_prefix_entries(
    counts: &[u64],
    work: &PreparationWork<'_>,
) -> Result<Option<u64>, QueryError> {
    let mut total = 0u64;
    for count in counts {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(next) = total.checked_add(*count) else {
            return Ok(None);
        };
        total = next;
    }

    Ok(Some(total))
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES, MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES,
        MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
        MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES, admit_cardinality_candidate_shape,
        cardinality_candidate_count_is_admitted, checked_exact_prefix_entries,
        exactly_one_matching_candidate,
    };

    #[test]
    fn exact_prefix_entry_sum_is_checked() {
        crate::db::query::preparation::with_preparation_work(|work| {
            assert_eq!(
                checked_exact_prefix_entries(&[2, 3, 5], work).unwrap(),
                Some(10)
            );
            assert_eq!(
                checked_exact_prefix_entries(&[u64::MAX, 1], work).unwrap(),
                None
            );
        });
    }

    #[test]
    fn optional_policy_admits_exact_bounds_and_rejects_every_first_excess() {
        assert!(cardinality_candidate_count_is_admitted(64));
        assert!(!cardinality_candidate_count_is_admitted(65));
        assert_eq!(
            admit_cardinality_candidate_shape(
                MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES
                    - MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
                MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
                MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES
                    - MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
                MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
                MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
            ),
            Some((
                MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES,
                MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES,
            )),
        );
        assert!(
            admit_cardinality_candidate_shape(0, 17, 0, 0, 0).is_none(),
            "a seventeenth candidate prefix must fall back",
        );
        assert!(
            admit_cardinality_candidate_shape(MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES, 1, 0, 0, 0,)
                .is_none(),
            "the 257th proof must fall back",
        );
        assert!(
            admit_cardinality_candidate_shape(0, 1, MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES, 1, 0,)
                .is_none(),
            "the first byte above the cumulative envelope must fall back",
        );
        assert!(
            admit_cardinality_candidate_shape(
                0,
                1,
                0,
                0,
                MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES + 1,
            )
            .is_none(),
            "the first byte above the transient envelope must fall back",
        );
    }

    #[test]
    fn pinned_route_selection_requires_exactly_one_match() {
        assert_eq!(
            exactly_one_matching_candidate([1, 2, 3], |candidate| *candidate == 2),
            Some(2),
        );
        assert_eq!(
            exactly_one_matching_candidate([1, 2, 3], |candidate| *candidate == 4),
            None,
        );
        assert_eq!(
            exactly_one_matching_candidate([1, 2, 2], |candidate| *candidate == 2),
            None,
        );
    }
}
