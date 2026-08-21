//! Module: db::session::query::cardinality_tiebreak
//! Responsibility: bind optional exact-prefix evidence to one canonical query plan.
//! Does not own: candidate ranking, cardinality maintenance, cache policy, or execution.
//! Boundary: final planner tie set + store evidence -> one advisory plan selection.

use crate::{
    db::{
        DbSession, QueryError,
        access::{SemanticIndexAccessContract, lower_access_with_schema_info},
        executor::EntityAuthority,
        index::{IndexId, UserIndexPrefixCardinalityKey},
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
use std::ops::Bound;

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
            match self.cardinality_tiebreak_attempt(authority, candidates)? {
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

        let mut plan =
            apply_exact_cardinality_tiebreak_selection(plan, selected_access, state, schema_info)?;
        plan.finalize_access_choice_with_semantic_indexes_and_schema(semantic_indexes, schema_info);

        Ok(plan)
    }

    pub(super) fn apply_pinned_cardinality_tiebreak(
        authority: &EntityAuthority,
        semantic_indexes: &[SemanticIndexAccessContract],
        plan: AccessPlannedQuery,
        route_pin: CardinalityTiebreakRoutePin,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?;
        let Some(candidates) =
            exact_cardinality_tiebreak_candidates(semantic_indexes, schema_info, &plan)
        else {
            return Ok(None);
        };
        let mut matching = candidates
            .into_iter()
            .filter(|candidate| candidate.route_pin(authority.entity_tag()) == Some(route_pin));
        let Some(selected) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Ok(None);
        }

        let mut plan = apply_exact_cardinality_tiebreak_selection(
            plan,
            Some(selected.into_access()),
            CardinalityTiebreakState::PinnedContinuation(route_pin),
            schema_info,
        )?;
        plan.finalize_access_choice_with_semantic_indexes_and_schema(semantic_indexes, schema_info);

        Ok(Some(plan))
    }

    fn cardinality_tiebreak_attempt(
        &self,
        authority: &EntityAuthority,
        candidates: Vec<CardinalityTiebreakCandidate>,
    ) -> Result<CardinalityTiebreakAttempt, QueryError> {
        if candidates.len() > MAX_CARDINALITY_TIEBREAK_CANDIDATES {
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
            prepare_cardinality_candidates(authority.entity_tag(), schema_info, candidates)
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
        ) else {
            return Ok(CardinalityTiebreakAttempt::PolicyFallback);
        };

        Ok(CardinalityTiebreakAttempt::Exact { selected, evidence })
    }
}

fn prepare_cardinality_candidates(
    entity_tag: EntityTag,
    schema_info: &crate::db::schema::SchemaInfo,
    candidates: Vec<CardinalityTiebreakCandidate>,
) -> Option<(
    Vec<PreparedCardinalityCandidate>,
    Vec<UserIndexPrefixCardinalityKey>,
)> {
    let mut total_probes = 0usize;
    let mut total_lowered_bytes = 0usize;
    let mut prepared = Vec::with_capacity(candidates.len());
    let mut keys: Vec<UserIndexPrefixCardinalityKey> = Vec::new();

    for candidate in candidates {
        let Ok(lowered) =
            lower_access_with_schema_info(entity_tag, candidate.access(), schema_info)
        else {
            return None;
        };
        let (_executable, prefix_specs, range_specs) = lowered.into_executable_and_index_specs();
        if !range_specs.is_empty()
            || prefix_specs.is_empty()
            || prefix_specs.len() > MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE
        {
            return None;
        }
        total_probes = total_probes.checked_add(prefix_specs.len())?;
        if total_probes > MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES {
            return None;
        }
        let candidate_component_bytes = prefix_specs.iter().try_fold(0usize, |total, spec| {
            spec.prefix_components()
                .iter()
                .try_fold(total, |total, component| total.checked_add(component.len()))
        })?;
        let candidate_transient_bytes =
            prefix_specs
                .iter()
                .try_fold(candidate_component_bytes, |total, spec| {
                    let (lower, upper) = spec.raw_bounds().ok()?;
                    total
                        .checked_add(bound_key_bytes(lower))?
                        .checked_add(bound_key_bytes(upper))
                })?;
        if candidate_transient_bytes > MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES {
            return None;
        }
        total_lowered_bytes = total_lowered_bytes.checked_add(candidate_component_bytes)?;
        if total_lowered_bytes > MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES {
            return None;
        }

        let index_id = IndexId::new_with_generation(
            entity_tag,
            candidate.index().ordinal(),
            candidate.index().physical_generation(),
        );
        let probe_start = keys.len();
        for spec in prefix_specs {
            let key = UserIndexPrefixCardinalityKey::new(index_id, spec.into_prefix_components());
            if keys.iter().any(|prior| {
                prior.index_id() == key.index_id()
                    && prior.prefix_components() == key.prefix_components()
            }) {
                return None;
            }
            keys.push(key);
        }
        prepared.push(PreparedCardinalityCandidate {
            candidate,
            probe_start,
            probe_end: keys.len(),
        });
    }

    Some((prepared, keys))
}

fn rank_prepared_cardinality_candidates(
    entity_tag: EntityTag,
    prepared: Vec<PreparedCardinalityCandidate>,
    counts: &[u64],
) -> Option<(
    CardinalityTiebreakCandidate,
    ExactCardinalityTiebreakEvidence,
)> {
    let mut ranked = Vec::with_capacity(prepared.len());
    for prepared_candidate in prepared {
        let exact_prefix_entries = counts
            .get(prepared_candidate.probe_start..prepared_candidate.probe_end)
            .and_then(checked_exact_prefix_entries)?;
        ranked.push((prepared_candidate.candidate, exact_prefix_entries));
    }

    let selected_index = ranked
        .iter()
        .enumerate()
        .min_by_key(|(_index, (_candidate, count))| *count)
        .map(|(index, _)| index)?;
    let route_pin = ranked[selected_index].0.route_pin(entity_tag)?;
    let evidence = ranked
        .iter()
        .map(|(candidate, count)| {
            CardinalityTiebreakCandidateEvidence::new(candidate.index().name().to_string(), *count)
        })
        .collect();
    let selected = ranked.swap_remove(selected_index).0;

    Some((
        selected,
        ExactCardinalityTiebreakEvidence::new(route_pin, evidence),
    ))
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

fn bound_key_bytes(bound: &Bound<crate::db::access::LoweredKey>) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => key.as_bytes().len(),
        Bound::Unbounded => 0,
    }
}

fn checked_exact_prefix_entries(counts: &[u64]) -> Option<u64> {
    counts
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count))
}

#[cfg(test)]
mod tests {
    use super::checked_exact_prefix_entries;

    #[test]
    fn exact_prefix_entry_sum_is_checked() {
        assert_eq!(checked_exact_prefix_entries(&[2, 3, 5]), Some(10));
        assert_eq!(checked_exact_prefix_entries(&[u64::MAX, 1]), None);
    }
}
