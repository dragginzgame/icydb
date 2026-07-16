//! Module: sql_generator::shrink
//! Responsibility: deterministic typed SELECT fixture and AST minimization.
//! Does not own: mismatch discovery, product/reference execution, or replay serialization.
//! Boundary: accepts only revalidated candidates that preserve one structured signature exactly.

use crate::{
    GeneratedSelectCase, SelectMismatchSignature, SelectObservedOutcome, SelectReplayRecord,
    SqlGeneratorError,
};

///
/// SelectShrinkReport
///
/// Original failure, smallest signature-preserving case found, and deterministic
/// budget accounting. An incomplete report remains a failed test outcome.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectShrinkReport {
    original_case: GeneratedSelectCase,
    minimized_case: GeneratedSelectCase,
    signature: SelectMismatchSignature,
    minimization_complete: bool,
    shrink_candidates_attempted: u32,
    evaluations: u32,
}

impl SelectShrinkReport {
    /// Borrow the original failing case.
    #[must_use]
    pub const fn original_case(&self) -> &GeneratedSelectCase {
        &self.original_case
    }

    /// Borrow the smallest signature-preserving case found.
    #[must_use]
    pub const fn minimized_case(&self) -> &GeneratedSelectCase {
        &self.minimized_case
    }

    /// Borrow the mismatch signature preserved by every accepted candidate.
    #[must_use]
    pub const fn signature(&self) -> &SelectMismatchSignature {
        &self.signature
    }

    /// Return whether minimization reached a deterministic fixed point.
    #[must_use]
    pub const fn minimization_complete(&self) -> bool {
        self.minimization_complete
    }

    /// Return the number of valid candidates evaluated.
    #[must_use]
    pub const fn shrink_candidates_attempted(&self) -> u32 {
        self.shrink_candidates_attempted
    }

    /// Return complete subject-plus-provider evaluations.
    #[must_use]
    pub const fn evaluations(&self) -> u32 {
        self.evaluations
    }

    /// Convert this failure report into one bounded canonical replay unit.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error if the outcomes or embedded cases violate
    /// the current replay contract.
    pub fn into_replay_record(
        self,
        subject_outcome: SelectObservedOutcome,
        comparison_outcome: SelectObservedOutcome,
    ) -> Result<SelectReplayRecord, SqlGeneratorError> {
        SelectReplayRecord::try_new(
            self.original_case,
            self.minimized_case,
            self.signature,
            subject_outcome,
            comparison_outcome,
            self.minimization_complete,
            self.shrink_candidates_attempted,
            self.evaluations,
        )
    }
}

/// Minimize one known failing SELECT case under deterministic candidate and
/// complete-evaluation budgets.
///
/// The evaluator returns the candidate's mismatch signature, or `None` when
/// the candidate no longer fails. Only an exact signature match is accepted.
/// Budget exhaustion returns an incomplete failure report rather than success.
///
/// # Errors
///
/// Returns a typed error if the original case is invalid or the evaluator
/// cannot complete one required subject-plus-provider evaluation.
pub fn shrink_select_failure<F>(
    original_case: &GeneratedSelectCase,
    signature: &SelectMismatchSignature,
    mut evaluate: F,
) -> Result<SelectShrinkReport, SqlGeneratorError>
where
    F: FnMut(&GeneratedSelectCase) -> Result<Option<SelectMismatchSignature>, SqlGeneratorError>,
{
    original_case.validate()?;
    signature.validate()?;

    let budgets = original_case.budgets();
    let mut minimized_case = original_case.clone();
    let mut shrink_candidates_attempted = 0_u32;
    let mut evaluations = 0_u32;

    loop {
        let candidates = shrink_candidates(&minimized_case);
        let mut accepted_candidate = None;
        for candidate in candidates {
            if shrink_candidates_attempted >= budgets.max_shrink_candidates()
                || evaluations >= budgets.max_evaluations()
            {
                return Ok(SelectShrinkReport {
                    original_case: original_case.clone(),
                    minimized_case,
                    signature: signature.clone(),
                    minimization_complete: false,
                    shrink_candidates_attempted,
                    evaluations,
                });
            }
            shrink_candidates_attempted = shrink_candidates_attempted.saturating_add(1);
            evaluations = evaluations.saturating_add(1);
            if evaluate(&candidate)?.as_ref() == Some(signature) {
                accepted_candidate = Some(candidate);
                break;
            }
        }

        let Some(candidate) = accepted_candidate else {
            return Ok(SelectShrinkReport {
                original_case: original_case.clone(),
                minimized_case,
                signature: signature.clone(),
                minimization_complete: true,
                shrink_candidates_attempted,
                evaluations,
            });
        };
        minimized_case = candidate;
    }
}

fn shrink_candidates(case: &GeneratedSelectCase) -> Vec<GeneratedSelectCase> {
    let mut candidates = Vec::new();
    for row_index in 0..case.fixture().len() {
        if let Some(fixture) = case.fixture().without_row(row_index) {
            let candidate = case.with_fixture(fixture);
            if candidate.validate().is_ok() && !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
    }
    for query in case.query().shrink_candidates() {
        if let Ok(candidate) = case.with_query(query)
            && !candidates.contains(&candidate)
        {
            candidates.push(candidate);
        }
    }

    candidates
}
