//! Module: sql_harness::selection
//! Responsibility: deterministic bounded selection over declared scenario strata.
//! Does not own: scenario generation, SQL rendering, or evidence verdicts.
//! Boundary: selects scenarios exclusively from typed metadata and stable scenario identities.

use std::collections::{BTreeMap, BTreeSet};

use crate::sql_harness::{
    CorrectnessScenario, EligibleProvider, EvidenceStrength, MutationKind, NullabilityClass,
    PredicateFamily, QueryShape, RouteFamily, StatementFamily, ValueTypeFamily, WindowBehavior,
};

///
/// ScenarioStratum
///
/// One typed coverage dimension represented by a correctness scenario.
/// Owned by the shared selector and derived only from declared scenario metadata.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ScenarioStratum {
    EvidenceStrength(EvidenceStrength),
    Mutation(MutationKind),
    Nullability(NullabilityClass),
    Predicate(PredicateFamily),
    Provider(EligibleProvider),
    Route(RouteFamily),
    Shape(QueryShape),
    Statement(StatementFamily),
    ValueType(ValueTypeFamily),
    Window(WindowBehavior),
}

/// Derive every bounded-selection stratum from declared scenario metadata.
const fn scenario_strata<S>(scenario: &CorrectnessScenario<S>) -> [ScenarioStratum; 10] {
    let metadata = &scenario.metadata;
    [
        ScenarioStratum::Statement(metadata.statement),
        ScenarioStratum::Shape(metadata.shape),
        ScenarioStratum::ValueType(metadata.value_type),
        ScenarioStratum::Nullability(metadata.nullability),
        ScenarioStratum::Predicate(metadata.predicate),
        ScenarioStratum::Window(metadata.window.behavior),
        ScenarioStratum::Route(metadata.route.family()),
        ScenarioStratum::Mutation(metadata.mutation),
        ScenarioStratum::EvidenceStrength(metadata.evidence_strength),
        ScenarioStratum::Provider(metadata.provider),
    ]
}

///
/// SelectionError
///
/// Fail-closed rejection produced when deterministic stratified selection is invalid.
/// Owned by the shared selector and returned to correctness and performance runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SelectionError {
    DuplicateScenarioId(String),

    InsufficientBudget {
        budget: usize,
        uncovered: BTreeSet<ScenarioStratum>,
    },
}

/// Select a stable bounded scenario set that covers every declared stratum.
pub(crate) fn select_stratified<S>(
    scenarios: &[CorrectnessScenario<S>],
    budget: usize,
) -> Result<Vec<&CorrectnessScenario<S>>, SelectionError> {
    let mut by_id = BTreeMap::new();
    for scenario in scenarios {
        if by_id.insert(scenario.key.as_str(), scenario).is_some() {
            return Err(SelectionError::DuplicateScenarioId(scenario.key.clone()));
        }
    }
    if scenarios.is_empty() {
        return Ok(Vec::new());
    }

    let all_strata = scenarios
        .iter()
        .flat_map(scenario_strata)
        .collect::<BTreeSet<_>>();
    let mut uncovered = all_strata;
    let mut selected_ids = BTreeSet::new();

    while !uncovered.is_empty() && selected_ids.len() < budget {
        let candidate = by_id
            .iter()
            .filter(|(id, _)| !selected_ids.contains(**id))
            .map(|(id, scenario)| {
                let gain = scenario_strata(scenario)
                    .into_iter()
                    .filter(|stratum| uncovered.contains(stratum))
                    .count();
                (*id, *scenario, gain)
            })
            .filter(|(_, _, gain)| *gain > 0)
            .max_by(|left, right| left.2.cmp(&right.2).then_with(|| right.0.cmp(left.0)));
        let Some((id, scenario, _)) = candidate else {
            break;
        };
        selected_ids.insert(id);
        for stratum in scenario_strata(scenario) {
            uncovered.remove(&stratum);
        }
    }

    if !uncovered.is_empty() {
        return Err(SelectionError::InsufficientBudget { budget, uncovered });
    }

    for id in by_id.keys() {
        if selected_ids.len() == budget.min(scenarios.len()) {
            break;
        }
        selected_ids.insert(*id);
    }

    Ok(selected_ids
        .into_iter()
        .filter_map(|id| by_id.get(id).copied())
        .collect())
}
