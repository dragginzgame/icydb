//! Module: sql_generator::mutation::shrink
//! Responsibility: deterministic mutation statement and initial-fixture minimization.
//! Does not own: mismatch discovery, provider execution, or replay serialization.
//! Boundary: rebuilds every candidate from initial state and preserves one exact typed signature.

use crate::{
    GeneratedMutationSequence, MutationMismatchSignature, MutationObservedOutcome,
    MutationReplayRecord, SqlGeneratorError,
};

///
/// MutationShrinkReport
///
/// Original failure, smallest signature-preserving sequence, and deterministic budget accounting.
/// An incomplete report remains a failed test outcome.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationShrinkReport {
    original_sequence: GeneratedMutationSequence,
    minimized_sequence: GeneratedMutationSequence,
    signature: MutationMismatchSignature,
    minimization_complete: bool,
    shrink_candidates_attempted: u32,
    evaluations: u32,
}

impl MutationShrinkReport {
    /// Borrow the original failing sequence.
    #[must_use]
    pub const fn original_sequence(&self) -> &GeneratedMutationSequence {
        &self.original_sequence
    }

    /// Borrow the smallest signature-preserving sequence found.
    #[must_use]
    pub const fn minimized_sequence(&self) -> &GeneratedMutationSequence {
        &self.minimized_sequence
    }

    /// Borrow the mismatch signature preserved by every accepted candidate.
    #[must_use]
    pub const fn signature(&self) -> &MutationMismatchSignature {
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

    /// Convert this report into one bounded canonical replay unit.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error when outcomes or embedded sequences violate the current contract.
    pub fn into_replay_record(
        self,
        subject_outcome: MutationObservedOutcome,
        comparison_outcome: MutationObservedOutcome,
    ) -> Result<MutationReplayRecord, SqlGeneratorError> {
        MutationReplayRecord::try_new(
            self.original_sequence,
            self.minimized_sequence,
            self.signature,
            subject_outcome,
            comparison_outcome,
            self.minimization_complete,
            self.shrink_candidates_attempted,
            self.evaluations,
        )
    }
}

/// Minimize one known failing mutation sequence under deterministic candidate
/// and complete-evaluation budgets.
///
/// The evaluator returns the candidate's typed mismatch signature, or `None`
/// when the candidate no longer fails. Each candidate is rebuilt from its
/// initial fixture before evaluation.
///
/// # Errors
///
/// Returns a typed error if the original sequence is invalid or one required evaluation fails.
pub fn shrink_mutation_failure<F>(
    original_sequence: &GeneratedMutationSequence,
    signature: &MutationMismatchSignature,
    mut evaluate: F,
) -> Result<MutationShrinkReport, SqlGeneratorError>
where
    F: FnMut(
        &GeneratedMutationSequence,
    ) -> Result<Option<MutationMismatchSignature>, SqlGeneratorError>,
{
    original_sequence.validate()?;
    signature.validate()?;
    let budgets = original_sequence.budgets();
    let mut minimized_sequence = original_sequence.clone();
    let mut shrink_candidates_attempted = 0_u32;
    let mut evaluations = 0_u32;

    loop {
        let candidates = shrink_candidates(&minimized_sequence);
        let mut accepted_candidate = None;
        for candidate in candidates {
            if shrink_candidates_attempted >= budgets.max_shrink_candidates()
                || evaluations >= budgets.max_evaluations()
            {
                return Ok(MutationShrinkReport {
                    original_sequence: original_sequence.clone(),
                    minimized_sequence,
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
            return Ok(MutationShrinkReport {
                original_sequence: original_sequence.clone(),
                minimized_sequence,
                signature: signature.clone(),
                minimization_complete: true,
                shrink_candidates_attempted,
                evaluations,
            });
        };
        minimized_sequence = candidate;
    }
}

fn shrink_candidates(sequence: &GeneratedMutationSequence) -> Vec<GeneratedMutationSequence> {
    let mut candidates = Vec::new();
    let statements = sequence.statements();
    if statements.len() > 1 {
        for statement_index in 0..statements.len() {
            let mut candidate_statements = statements.clone();
            candidate_statements.remove(statement_index);
            if let Ok(candidate) =
                sequence.rebuilt(sequence.initial_rows().to_vec(), candidate_statements)
                && !candidates.contains(&candidate)
            {
                candidates.push(candidate);
            }
        }
    }
    for row_index in 0..sequence.initial_rows().len() {
        let mut candidate_rows = sequence.initial_rows().to_vec();
        candidate_rows.remove(row_index);
        if let Ok(candidate) = sequence.rebuilt(candidate_rows, statements.clone())
            && !candidates.contains(&candidate)
        {
            candidates.push(candidate);
        }
    }

    candidates
}
