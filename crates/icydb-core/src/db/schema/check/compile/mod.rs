//! Fingerprint-bound compilation and SQL three-valued check evaluation.

use super::{
    AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckExprV1Error,
    AcceptedCheckLiteralV1, AcceptedCheckValueExprV1, MAX_CHECK_EXPR_V1_NODES, slot_for_field,
    targeted::{AcceptedTargetPath, CompiledAcceptedTargetedRules, TargetedRuleSource},
};
use crate::{
    db::write_context::MutationMode,
    db::{
        commit::CommitSchemaFingerprint,
        data::{AcceptedFieldWriteProvenance, decode_validated_check_literal_payload},
        predicate::normalize_with_budget,
        resource::construction::ConstructionBudget,
        schema::{
            AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            ConstraintActivationKind, ConstraintId,
        },
    },
    error::{AcceptedConstraintFactContext, InternalError, MutationDiagnosticContext},
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{borrow::Cow, cmp::Ordering};

type CheckConstraintSource<'a> = (ConstraintId, &'a AcceptedCheckExprV1, bool);
type NotNullConstraintSource = (ConstraintId, crate::db::schema::FieldId, bool);

// Borrowed source buckets preserve the existing compilation phases. Only
// classification is shared; no rule payload or compiled program is duplicated.
#[derive(Default)]
struct RowConstraintSources<'a> {
    checks: Vec<CheckConstraintSource<'a>>,
    not_null: Vec<NotNullConstraintSource>,
    targeted: Vec<TargetedRuleSource<'a>>,
    // Integrity traverses accepted CHECK/targeted rules, never pending write gates.
    integrity_count: usize,
}

/// SQL three-valued truth returned by accepted check evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedCheckTruth {
    True,
    False,
    Unknown,
}

impl AcceptedCheckTruth {
    const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

/// Constraint kind carried by one final-after-image admission failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedRowConstraintViolationKind {
    /// A canonical accepted check expression evaluated to false.
    Check,
    /// An accepted or activating not-null rule observed an explicit null value.
    NotNull,
}

/// Typed failure from compiling or evaluating accepted row-constraint authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedRowConstraintEvaluationError {
    InvalidExpression(AcceptedCheckExprV1Error),
    LiteralCorrupt,
    FingerprintMismatch,
    MissingSlot,
    RuntimeValueMismatch,
    WorkBudgetExceeded,
    ValueDepthExceeded,
    ValueNodeBudgetExceeded,
    OperationBudgetExceeded,
    PathBudgetExceeded,
    TargetedRuleViolation {
        constraint_id: ConstraintId,
        path: AcceptedTargetPath,
    },
    Violation {
        constraint_id: ConstraintId,
        kind: AcceptedRowConstraintViolationKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledCheckValueExprV1 {
    Field(usize),
    Literal(Value),
    CharLength(usize),
    OctetLength(usize),
    Cardinality(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledCheckExprV1 {
    True,
    False,
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        left: CompiledCheckValueExprV1,
        op: AcceptedCheckCompareOpV1,
        right: CompiledCheckValueExprV1,
    },
    IsNull(CompiledCheckValueExprV1),
    IsNotNull(CompiledCheckValueExprV1),
}

impl CompiledCheckExprV1 {
    // Dependency collection consumes the resolved program, avoiding another
    // field-ID projection and intermediate per-expression dependency vectors.
    fn try_for_each_slot<E>(
        &self,
        visit: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut visit_value = |value: &CompiledCheckValueExprV1| match value {
            CompiledCheckValueExprV1::Field(slot)
            | CompiledCheckValueExprV1::CharLength(slot)
            | CompiledCheckValueExprV1::OctetLength(slot)
            | CompiledCheckValueExprV1::Cardinality(slot) => visit(*slot),
            CompiledCheckValueExprV1::Literal(_) => Ok(()),
        };
        match self {
            Self::True | Self::False => Ok(()),
            Self::Not(inner) => inner.try_for_each_slot(visit),
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.try_for_each_slot(visit)?;
                }
                Ok(())
            }
            Self::Compare { left, right, .. } => {
                visit_value(left)?;
                visit_value(right)
            }
            Self::IsNull(value) | Self::IsNotNull(value) => visit_value(value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledAcceptedCheck {
    id: ConstraintId,
    expression: CompiledCheckExprV1,
    validated: bool,
}

/// Reserved unique identity plus the slots whose authorship cannot safely
/// change while its planner-invisible candidate generation is incomplete.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledUniqueWriteBarrier {
    id: ConstraintId,
    dependency_slots: Vec<usize>,
}

impl CompiledUniqueWriteBarrier {
    /// Return the stable accepted constraint identity.
    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.id
    }
}

/// One compiled row-local constraint whose identity and slot bindings are frozen.
#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledAcceptedRowConstraint {
    /// One accepted check expression.
    Check(CompiledAcceptedCheck),
    /// One accepted or activating not-null rule.
    NotNull {
        id: ConstraintId,
        slot: usize,
        accepted: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompiledIntegrityRowConstraint {
    Check {
        id: ConstraintId,
        constraint_ordinal: usize,
    },
    TargetedRule {
        id: ConstraintId,
    },
}

impl CompiledIntegrityRowConstraint {
    const fn id(self) -> ConstraintId {
        match self {
            Self::Check { id, .. } | Self::TargetedRule { id } => id,
        }
    }
}

impl CompiledAcceptedRowConstraint {
    const fn id(&self) -> ConstraintId {
        match self {
            Self::Check(check) => check.id,
            Self::NotNull { id, .. } => *id,
        }
    }
}

/// Immutable row-local accepted constraints and activation gates.
///
/// The program is bound to one exact accepted schema fingerprint and evaluates
/// every entry in stable constraint-ID order over one decoded slot set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledAcceptedRowConstraints {
    fingerprint: CommitSchemaFingerprint,
    constraints: Vec<CompiledAcceptedRowConstraint>,
    integrity_constraints: Vec<CompiledIntegrityRowConstraint>,
    required_slots: Vec<usize>,
    unique_write_barriers: Vec<CompiledUniqueWriteBarrier>,
    targeted_rules: CompiledAcceptedTargetedRules,
    field_count: usize,
}

impl CompiledAcceptedRowConstraints {
    /// Compile every accepted row-local constraint and pending write gate.
    pub(in crate::db) fn compile(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        work: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        let snapshot = schema.persisted_snapshot();
        let RowConstraintSources {
            checks,
            not_null,
            targeted,
            integrity_count,
        } = row_constraint_sources(snapshot, work)?;
        let targeted_rules =
            CompiledAcceptedTargetedRules::compile_sources(schema, value_catalog, targeted, work)?;
        // Source classification counts both exact destinations. Admit their
        // backing before filling them; integrity entries share no source buffer.
        let backing = (
            work.vec_with_capacity(checks.len() + not_null.len())?,
            work.vec_with_capacity(integrity_count)?,
        );
        // These two sorts run inside semantic compilation. Admit their exact
        // scratch before entering its expression-error mapping; budget facts
        // must not be flattened into accepted-program corruption.
        work.admit_compact_sort(checks.len() + not_null.len())?;
        work.admit_compact_sort(integrity_count)?;
        // Validate bounds before recursively admitting tree construction. Keep
        // budget refusal outside the following semantic-error mapping.
        validate_check_sources(&checks, snapshot, value_catalog.composite_catalog())
            .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?;
        admit_row_program_construction(&checks, &not_null, snapshot, work)?;
        let mut compiled = Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            checks,
            not_null,
            targeted_rules,
            backing,
        )
        .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?
        .with_required_slots(
            |slots| {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                work.reserve_vec(slots, 1)
            },
            |entries| {
                work.admit_compact_sort(entries)?;
                // Deduplication visits adjacent slots and may move every
                // retained shell. Admit that linear pass before mutation.
                work.charge(
                    Resource::PredicateExpressionSteps,
                    (entries as u64).saturating_mul(2),
                )
            },
        )?;
        debug_assert_eq!(compiled.integrity_constraints.len(), integrity_count);
        // All activations share caller authority. Build locally and return no
        // partial program if any dependency admission or semantic check fails.
        work.charge(
            Resource::PredicateExpressionSteps,
            snapshot.constraint_activations().len() as u64,
        )?;
        for activation in snapshot.constraint_activations() {
            let index_id = match activation.kind() {
                ConstraintActivationKind::Unique { index_id } => *index_id,
                ConstraintActivationKind::Check { .. }
                | ConstraintActivationKind::NotNull { .. }
                | ConstraintActivationKind::TargetedRule { .. }
                | ConstraintActivationKind::Relation { .. } => continue,
            };
            work.charge(
                Resource::PredicateExpressionSteps,
                snapshot.candidate_indexes().len() as u64,
            )?;
            let mut matching = snapshot
                .candidate_indexes()
                .iter()
                .filter(|index| index.schema_id() == index_id);
            let index = matching
                .next()
                .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)?;
            if matching.next().is_some() {
                return Err(InternalError::accepted_row_constraint_program_corrupt());
            }
            let dependency_slots =
                unique_index_dependency_slots(schema, index, value_catalog, work)?;
            work.reserve_vec(&mut compiled.unique_write_barriers, 1)?;
            compiled
                .unique_write_barriers
                .push(CompiledUniqueWriteBarrier {
                    id: activation.id(),
                    dependency_slots,
                });
        }
        work.admit_compact_sort(compiled.unique_write_barriers.len())?;
        icydb_schema::compact_sort_unstable_by(
            &mut compiled.unique_write_barriers,
            |left, right| left.id.cmp(&right.id),
        );
        Ok(compiled)
    }

    /// Compile one pending CHECK or NOT NULL gate for historical validation.
    /// The caller's expected kind must agree with the accepted activation.
    pub(in crate::db) fn compile_activation(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        activation_id: ConstraintId,
        required_kind: AcceptedRowConstraintViolationKind,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let activation = schema
            .persisted_snapshot()
            .constraint_activations()
            .iter()
            .find(|activation| activation.id() == activation_id)
            .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::UnknownField,
            ))?;
        let (checks, not_null) = match (required_kind, activation.kind()) {
            (
                AcceptedRowConstraintViolationKind::Check,
                ConstraintActivationKind::Check { expression },
            ) => (
                vec![(activation.id(), expression.as_ref(), false)],
                Vec::new(),
            ),
            (
                AcceptedRowConstraintViolationKind::NotNull,
                ConstraintActivationKind::NotNull { field_id },
            ) => (Vec::new(), vec![(activation.id(), *field_id, false)]),
            _ => {
                return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                    AcceptedCheckExprV1Error::OperandKindMismatch,
                ));
            }
        };
        validate_check_sources(
            &checks,
            schema.persisted_snapshot(),
            value_catalog.composite_catalog(),
        )
        .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)?;
        Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            checks,
            not_null,
            CompiledAcceptedTargetedRules::empty(),
            (Vec::with_capacity(1), Vec::new()),
        )
        .and_then(|compiled| compiled.with_required_slots(|_| Ok(()), |_| Ok(())))
    }

    /// Compile exactly one pending targeted rule for historical validation.
    pub(in crate::db) fn compile_targeted_rule_activation(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        activation_id: ConstraintId,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let targeted_rules =
            CompiledAcceptedTargetedRules::compile_activation(schema, value_catalog, activation_id)
                .map_err(AcceptedRowConstraintEvaluationError::from)?;
        Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            Vec::new(),
            Vec::new(),
            targeted_rules,
            (Vec::new(), Vec::new()),
        )
        .and_then(|compiled| compiled.with_required_slots(|_| Ok(()), |_| Ok(())))
    }

    fn compile_sources(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        check_sources: Vec<CheckConstraintSource<'_>>,
        not_null_sources: Vec<NotNullConstraintSource>,
        targeted_rules: CompiledAcceptedTargetedRules,
        (mut constraints, integrity_constraints): (
            Vec<CompiledAcceptedRowConstraint>,
            Vec<CompiledIntegrityRowConstraint>,
        ),
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        // Callers validate CHECK shape/bounds and supply empty exact backing.
        // Preserve CHECK-before-NOT-NULL compilation while filling it once.
        debug_assert!(constraints.is_empty());
        debug_assert!(constraints.capacity() >= check_sources.len() + not_null_sources.len());
        for &(id, expression, validated) in &check_sources {
            constraints.push(CompiledAcceptedRowConstraint::Check(
                CompiledAcceptedCheck {
                    id,
                    expression: compile_expr(expression, snapshot, value_catalog)?,
                    validated,
                },
            ));
        }
        for &(id, field_id, accepted) in &not_null_sources {
            let slot = slot_for_field(snapshot, field_id)
                .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)?;
            constraints.push(CompiledAcceptedRowConstraint::NotNull {
                id,
                slot: usize::from(slot.get()),
                accepted,
            });
        }
        icydb_schema::compact_sort_unstable_by(&mut constraints, |left, right| {
            left.id().cmp(&right.id())
        });
        let integrity_constraints =
            compile_integrity_constraints(snapshot, &constraints, integrity_constraints);
        Ok(Self {
            fingerprint,
            constraints,
            integrity_constraints,
            required_slots: Vec::new(),
            unique_write_barriers: Vec::new(),
            targeted_rules,
            field_count: snapshot.row_layout().allocated_slot_count(),
        })
    }

    // Whole-schema callers admit every growth under their existing authority.
    // Single-activation callers retain their separate allocation contract. The
    // destination is local until complete, then sorted/deduplicated in place.
    fn with_required_slots<E>(
        mut self,
        mut admit: impl FnMut(&mut Vec<usize>) -> Result<(), E>,
        admit_finish: impl FnOnce(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut slots = Vec::new();
        let mut push = |slot| {
            admit(&mut slots)?;
            slots.push(slot);
            Ok(())
        };
        for constraint in &self.constraints {
            match constraint {
                CompiledAcceptedRowConstraint::Check(check) => {
                    check.expression.try_for_each_slot(&mut push)?;
                }
                CompiledAcceptedRowConstraint::NotNull { slot, .. } => push(*slot)?,
            }
        }
        for slot in self.targeted_rules.required_slots() {
            push(slot)?;
        }
        admit_finish(slots.len())?;
        icydb_schema::compact_sort_unstable_by(&mut slots, Ord::cmp);
        slots.dedup();
        self.required_slots = slots;
        Ok(self)
    }

    /// Return whether this schema has any row-local constraints or gates.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.constraints.is_empty()
            && self.unique_write_barriers.is_empty()
            && self.targeted_rules.is_empty()
    }

    /// Borrow the sorted unique row slots read by this compiled program.
    #[must_use]
    pub(in crate::db) const fn required_slots(&self) -> &[usize] {
        self.required_slots.as_slice()
    }

    /// Reject an explicit null before a non-null physical slot is encoded.
    ///
    /// Pending NOT NULL gates remain nullable physically and are evaluated
    /// over the canonical after-image by [`Self::evaluate`]. Accepted NOT NULL
    /// fields cannot encode `NULL`, so this narrow logical pre-encoding lane
    /// evaluates the same compiled identity before the storage contract would
    /// otherwise flatten the failure into a generic admission error.
    pub(in crate::db) fn evaluate_accepted_not_null_before_encoding(
        &self,
        current_fingerprint: CommitSchemaFingerprint,
        slot: usize,
    ) -> Result<(), AcceptedRowConstraintEvaluationError> {
        if current_fingerprint != self.fingerprint {
            return Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch);
        }
        let Some(CompiledAcceptedRowConstraint::NotNull { id, .. }) =
            self.constraints.iter().find(|constraint| {
                matches!(
                    constraint,
                    CompiledAcceptedRowConstraint::NotNull {
                        slot: constrained_slot,
                        accepted: true,
                        ..
                    } if *constrained_slot == slot
                )
            })
        else {
            return Ok(());
        };

        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: *id,
            kind: AcceptedRowConstraintViolationKind::NotNull,
        })
    }

    /// Return validated accepted row-local constraints in stable ID order.
    ///
    /// This is the row-integrity sub-unit count. Write admission continues to
    /// use [`Self::evaluate`] and reject the first violation.
    #[must_use]
    pub(in crate::db) const fn integrity_constraint_count(&self) -> usize {
        self.integrity_constraints.len()
    }

    /// Evaluate exactly one validated accepted row-local constraint.
    ///
    /// Pending activation gates remain in the shared write program but are
    /// excluded from this accepted-corruption lane. An invalid ordinal is
    /// corrupt program state.
    pub(in crate::db) fn evaluate_integrity_constraint(
        &self,
        ordinal: usize,
        current_fingerprint: CommitSchemaFingerprint,
        values_by_slot: &[Option<Cow<'_, Value>>],
    ) -> Result<(), AcceptedRowConstraintEvaluationError> {
        if current_fingerprint != self.fingerprint {
            return Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch);
        }
        let Some(constraint) = self.integrity_constraints.get(ordinal).copied() else {
            return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::UnknownField,
            ));
        };
        match constraint {
            CompiledIntegrityRowConstraint::Check {
                constraint_ordinal, ..
            } => {
                let Some(CompiledAcceptedRowConstraint::Check(check)) =
                    self.constraints.get(constraint_ordinal)
                else {
                    return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                        AcceptedCheckExprV1Error::UnknownField,
                    ));
                };
                let mut remaining_work = u32::from(MAX_CHECK_EXPR_V1_NODES);
                if evaluate_expr(&check.expression, values_by_slot, &mut remaining_work)?
                    == AcceptedCheckTruth::False
                {
                    return Err(AcceptedRowConstraintEvaluationError::Violation {
                        constraint_id: check.id,
                        kind: AcceptedRowConstraintViolationKind::Check,
                    });
                }
            }
            CompiledIntegrityRowConstraint::TargetedRule { id } => {
                if let Some(violation) = self
                    .targeted_rules
                    .evaluate_constraint(id, values_by_slot)
                    .map_err(AcceptedRowConstraintEvaluationError::from)?
                {
                    return Err(targeted_rule_violation_error(violation));
                }
            }
        }

        Ok(())
    }

    /// Return the first incomplete unique activation that blocks this write.
    pub(in crate::db) fn unique_activation_write_blocker(
        &self,
        mode: MutationMode,
        provenance: &[Option<AcceptedFieldWriteProvenance>],
    ) -> Result<Option<&CompiledUniqueWriteBarrier>, AcceptedRowConstraintEvaluationError> {
        if provenance.len() != self.field_count {
            return Err(AcceptedRowConstraintEvaluationError::MissingSlot);
        }
        Ok(self
            .unique_write_barriers
            .iter()
            .find(|barrier| match mode {
                MutationMode::Insert | MutationMode::Replace => true,
                MutationMode::Update => barrier.dependency_slots.iter().any(|slot| {
                    !matches!(
                        provenance.get(*slot).copied().flatten(),
                        Some(
                            AcceptedFieldWriteProvenance::Preserved
                                | AcceptedFieldWriteProvenance::HistoricalFill
                                | AcceptedFieldWriteProvenance::PreservedReplacementIdentity
                        )
                    )
                }),
            }))
    }

    /// Evaluate row constraints in stable ID order and reject the first violation.
    /// Slot ownership is immaterial: readers lend cached values, while integrity
    /// inspection owns its snapshot. Neither path copies operands during evaluation.
    pub(in crate::db) fn evaluate(
        &self,
        current_fingerprint: CommitSchemaFingerprint,
        values_by_slot: &[Option<Cow<'_, Value>>],
    ) -> Result<(), AcceptedRowConstraintEvaluationError> {
        if current_fingerprint != self.fingerprint {
            return Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch);
        }
        // The targeted artifact shares one bounded walk per root. It is
        // side-effect free, so evaluation may run once before the scalar
        // schedule while violation selection still follows the sole stable
        // accepted constraint-ID order below.
        let mut targeted_violation = self
            .targeted_rules
            .evaluate(values_by_slot)
            .map_err(AcceptedRowConstraintEvaluationError::from)?;
        let per_check = u32::from(MAX_CHECK_EXPR_V1_NODES);
        let check_count = u32::try_from(
            self.constraints
                .iter()
                .filter(|constraint| matches!(constraint, CompiledAcceptedRowConstraint::Check(_)))
                .count(),
        )
        .map_err(|_| AcceptedRowConstraintEvaluationError::WorkBudgetExceeded)?;
        let mut remaining_work = per_check
            .checked_mul(check_count)
            .ok_or(AcceptedRowConstraintEvaluationError::WorkBudgetExceeded)?;
        for constraint in &self.constraints {
            if targeted_violation
                .as_ref()
                .is_some_and(|violation| violation.constraint_id < constraint.id())
            {
                let violation = targeted_violation
                    .take()
                    .ok_or(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch)?;
                return Err(targeted_rule_violation_error(violation));
            }
            match constraint {
                CompiledAcceptedRowConstraint::Check(check) => {
                    let truth =
                        evaluate_expr(&check.expression, values_by_slot, &mut remaining_work)?;
                    if truth == AcceptedCheckTruth::False {
                        return Err(AcceptedRowConstraintEvaluationError::Violation {
                            constraint_id: check.id,
                            kind: AcceptedRowConstraintViolationKind::Check,
                        });
                    }
                }
                CompiledAcceptedRowConstraint::NotNull { id, slot, .. } => {
                    let value = values_by_slot
                        .get(*slot)
                        .and_then(Option::as_deref)
                        .ok_or(AcceptedRowConstraintEvaluationError::MissingSlot)?;
                    if matches!(value, Value::Null) {
                        return Err(AcceptedRowConstraintEvaluationError::Violation {
                            constraint_id: *id,
                            kind: AcceptedRowConstraintViolationKind::NotNull,
                        });
                    }
                }
            }
        }
        if let Some(violation) = targeted_violation {
            return Err(targeted_rule_violation_error(violation));
        }
        Ok(())
    }
}

fn compile_integrity_constraints(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    constraints: &[CompiledAcceptedRowConstraint],
    mut compiled: Vec<CompiledIntegrityRowConstraint>,
) -> Vec<CompiledIntegrityRowConstraint> {
    debug_assert!(compiled.is_empty());
    compiled.extend(
        constraints
            .iter()
            .enumerate()
            .filter_map(|(ordinal, constraint)| {
                let CompiledAcceptedRowConstraint::Check(CompiledAcceptedCheck {
                    id,
                    validated: true,
                    ..
                }) = constraint
                else {
                    return None;
                };
                Some(CompiledIntegrityRowConstraint::Check {
                    id: *id,
                    constraint_ordinal: ordinal,
                })
            })
            .chain(snapshot.constraints().iter().filter_map(|constraint| {
                matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { .. }
                )
                .then_some(CompiledIntegrityRowConstraint::TargetedRule {
                    id: constraint.id(),
                })
            })),
    );
    icydb_schema::compact_sort_unstable_by(&mut compiled, |left, right| left.id().cmp(&right.id()));
    compiled
}

fn targeted_rule_violation_error(
    violation: super::targeted::AcceptedTargetedRuleViolation,
) -> AcceptedRowConstraintEvaluationError {
    AcceptedRowConstraintEvaluationError::TargetedRuleViolation {
        constraint_id: violation.constraint_id,
        path: violation.path,
    }
}

// Each catalog is classified once, retaining accepted sources before pending
// sources within every family. Admit each visit and any new borrowed-source
// backing before construction; downstream compilers keep their existing order.
fn row_constraint_sources<'a>(
    snapshot: &'a crate::db::schema::PersistedSchemaSnapshot,
    work: &dyn ConstructionBudget,
) -> Result<RowConstraintSources<'a>, InternalError> {
    let mut sources = RowConstraintSources::default();
    for constraint in snapshot.constraints() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        match constraint.kind() {
            AcceptedConstraintKind::Check { expression } => {
                work.reserve_vec(&mut sources.checks, 1)?;
                sources
                    .checks
                    .push((constraint.id(), expression.as_ref(), true));
                sources.integrity_count += 1;
            }
            AcceptedConstraintKind::NotNull { field_id } => {
                work.reserve_vec(&mut sources.not_null, 1)?;
                sources.not_null.push((constraint.id(), *field_id, true));
            }
            AcceptedConstraintKind::TargetedRule { target, operation } => {
                work.reserve_vec(&mut sources.targeted, 1)?;
                sources
                    .targeted
                    .push((constraint.id(), *target, operation.as_ref(), true));
                sources.integrity_count += 1;
            }
            AcceptedConstraintKind::PrimaryKey
            | AcceptedConstraintKind::Unique { .. }
            | AcceptedConstraintKind::Relation { .. } => {}
        }
    }
    for activation in snapshot.constraint_activations() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        match activation.kind() {
            ConstraintActivationKind::Check { expression } => {
                work.reserve_vec(&mut sources.checks, 1)?;
                sources
                    .checks
                    .push((activation.id(), expression.as_ref(), false));
            }
            ConstraintActivationKind::NotNull { field_id } => {
                work.reserve_vec(&mut sources.not_null, 1)?;
                sources.not_null.push((activation.id(), *field_id, false));
            }
            ConstraintActivationKind::TargetedRule { target, operation } => {
                work.reserve_vec(&mut sources.targeted, 1)?;
                sources
                    .targeted
                    .push((activation.id(), *target, operation.as_ref(), false));
            }
            ConstraintActivationKind::Unique { .. } | ConstraintActivationKind::Relation { .. } => {
            }
        }
    }
    Ok(sources)
}

/// Map one row-program result into the shared accepted write diagnostic.
///
/// Logical pre-encoding not-null evaluation and canonical after-image
/// evaluation both use this owner so frontend or physical representability
/// cannot change accepted identity or error classification.
pub(in crate::db) fn accepted_row_constraint_write_error(
    fingerprint_method: u8,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    entity_tag: u64,
    mutation: Option<MutationDiagnosticContext>,
    error: AcceptedRowConstraintEvaluationError,
) -> InternalError {
    match error {
        AcceptedRowConstraintEvaluationError::Violation {
            constraint_id,
            kind,
        } => InternalError::mutation_constraint_violation(
            AcceptedConstraintFactContext::write_admission(
                fingerprint_method,
                accepted_schema_fingerprint,
                entity_tag,
                constraint_id.get(),
                match kind {
                    AcceptedRowConstraintViolationKind::Check => {
                        icydb_diagnostic_code::DiagnosticConstraintKind::Check
                    }
                    AcceptedRowConstraintViolationKind::NotNull => {
                        icydb_diagnostic_code::DiagnosticConstraintKind::NotNull
                    }
                },
                mutation,
                None,
            ),
        ),
        AcceptedRowConstraintEvaluationError::TargetedRuleViolation {
            constraint_id,
            path,
        } => InternalError::mutation_constraint_violation(
            AcceptedConstraintFactContext::write_admission(
                fingerprint_method,
                accepted_schema_fingerprint,
                entity_tag,
                constraint_id.get(),
                icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule,
                mutation,
                Some(path.into_constraint_value_path()),
            ),
        ),
        AcceptedRowConstraintEvaluationError::InvalidExpression(_)
        | AcceptedRowConstraintEvaluationError::LiteralCorrupt
        | AcceptedRowConstraintEvaluationError::FingerprintMismatch
        | AcceptedRowConstraintEvaluationError::MissingSlot
        | AcceptedRowConstraintEvaluationError::RuntimeValueMismatch
        | AcceptedRowConstraintEvaluationError::WorkBudgetExceeded
        | AcceptedRowConstraintEvaluationError::ValueDepthExceeded
        | AcceptedRowConstraintEvaluationError::ValueNodeBudgetExceeded
        | AcceptedRowConstraintEvaluationError::OperationBudgetExceeded
        | AcceptedRowConstraintEvaluationError::PathBudgetExceeded => {
            InternalError::accepted_row_constraint_program_corrupt()
        }
    }
}

fn validate_check_sources(
    sources: &[CheckConstraintSource<'_>],
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedCheckExprV1Error> {
    for (_, expression, _) in sources {
        expression.validate(snapshot, catalog)?;
    }
    Ok(())
}

// Reserve the bounded CHECK/NOT NULL construction phases before semantic
// compilation. Validation, decoding and targeted-program work remain separate.
fn admit_row_program_construction(
    checks: &[CheckConstraintSource<'_>],
    not_null: &[NotNullConstraintSource],
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    work: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    let fields = snapshot.fields().len();
    for (_, expression, _) in checks {
        admit_check_tree(expression, fields, work)?;
    }
    // NOT NULL dispatch and field lookup; integrity then visits the compiled
    // schedule and the accepted catalog, including entries it filters out.
    let not_null_steps = (not_null.len() as u64).saturating_mul((fields as u64).saturating_add(1));
    let integrity_steps = checks
        .len()
        .saturating_add(not_null.len())
        .saturating_add(snapshot.constraints().len()) as u64;
    work.charge(
        Resource::PredicateExpressionSteps,
        not_null_steps.saturating_add(integrity_steps),
    )
}

// Shape/bounds validation runs first. Literal decoding, validation scratch and
// recursive stack cost retain their own scope.
fn admit_check_tree(
    expression: &AcceptedCheckExprV1,
    fields: usize,
    work: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    // Admission, compilation and the compiled dependency walk each visit it.
    work.charge(Resource::PredicateExpressionSteps, 3)?;
    match expression {
        AcceptedCheckExprV1::True | AcceptedCheckExprV1::False => Ok(()),
        AcceptedCheckExprV1::Not(inner) => {
            work.charge(
                Resource::TemporaryBytes,
                size_of::<CompiledCheckExprV1>() as u64,
            )?;
            admit_check_tree(inner, fields, work)
        }
        AcceptedCheckExprV1::And(children) | AcceptedCheckExprV1::Or(children) => {
            work.charge(
                Resource::TemporaryBytes,
                (children.len() as u64).saturating_mul(size_of::<CompiledCheckExprV1>() as u64),
            )?;
            for child in children {
                admit_check_tree(child, fields, work)?;
            }
            Ok(())
        }
        AcceptedCheckExprV1::Compare { left, right, .. } => {
            admit_check_value(left, fields, work)?;
            admit_check_value(right, fields, work)
        }
        AcceptedCheckExprV1::IsNull(value) | AcceptedCheckExprV1::IsNotNull(value) => {
            admit_check_value(value, fields, work)
        }
    }
}

fn admit_check_value(
    value: &AcceptedCheckValueExprV1,
    fields: usize,
    work: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    // Reserve the full metadata scan even when field lookup can stop early.
    // Literal dispatch has no field lookup. Admit its encoded input before
    // entering semantic compilation and its corruption-error translation.
    let resolution = match value {
        AcceptedCheckValueExprV1::Literal(_) => 0,
        AcceptedCheckValueExprV1::Field(_)
        | AcceptedCheckValueExprV1::CharLength(_)
        | AcceptedCheckValueExprV1::OctetLength(_)
        | AcceptedCheckValueExprV1::Cardinality(_) => fields as u64,
    };
    work.charge(
        Resource::PredicateExpressionSteps,
        resolution.saturating_add(3),
    )?;
    if let AcceptedCheckValueExprV1::Literal(literal) = value {
        literal.admit_decode_input(work)?;
    }
    Ok(())
}

fn compile_expr(
    expression: &AcceptedCheckExprV1,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<CompiledCheckExprV1, AcceptedRowConstraintEvaluationError> {
    match expression {
        AcceptedCheckExprV1::True => Ok(CompiledCheckExprV1::True),
        AcceptedCheckExprV1::False => Ok(CompiledCheckExprV1::False),
        AcceptedCheckExprV1::Not(inner) => Ok(CompiledCheckExprV1::Not(Box::new(compile_expr(
            inner,
            snapshot,
            value_catalog,
        )?))),
        AcceptedCheckExprV1::And(children) => {
            compile_check_children(children, snapshot, value_catalog).map(CompiledCheckExprV1::And)
        }
        AcceptedCheckExprV1::Or(children) => {
            compile_check_children(children, snapshot, value_catalog).map(CompiledCheckExprV1::Or)
        }
        AcceptedCheckExprV1::Compare { left, op, right } => Ok(CompiledCheckExprV1::Compare {
            left: compile_value(left, snapshot, value_catalog)?,
            op: *op,
            right: compile_value(right, snapshot, value_catalog)?,
        }),
        AcceptedCheckExprV1::IsNull(value) => {
            compile_value(value, snapshot, value_catalog).map(CompiledCheckExprV1::IsNull)
        }
        AcceptedCheckExprV1::IsNotNull(value) => {
            compile_value(value, snapshot, value_catalog).map(CompiledCheckExprV1::IsNotNull)
        }
    }
}

fn compile_check_children(
    children: &[AcceptedCheckExprV1],
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Vec<CompiledCheckExprV1>, AcceptedRowConstraintEvaluationError> {
    // One exact destination matches structural admission; fallible collection
    // would use a separate geometric-growth policy and could exceed that backing.
    let mut compiled = Vec::with_capacity(children.len());
    for child in children {
        compiled.push(compile_expr(child, snapshot, value_catalog)?);
    }
    Ok(compiled)
}

fn compile_value(
    value: &AcceptedCheckValueExprV1,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<CompiledCheckValueExprV1, AcceptedRowConstraintEvaluationError> {
    match value {
        AcceptedCheckValueExprV1::Field(field_id) => slot_for_field(snapshot, *field_id)
            .map(|slot| CompiledCheckValueExprV1::Field(usize::from(slot.get())))
            .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression),
        AcceptedCheckValueExprV1::Literal(literal) => {
            decode_literal(literal, value_catalog).map(CompiledCheckValueExprV1::Literal)
        }
        AcceptedCheckValueExprV1::CharLength(field_id) => slot_for_field(snapshot, *field_id)
            .map(|slot| CompiledCheckValueExprV1::CharLength(usize::from(slot.get())))
            .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression),
        AcceptedCheckValueExprV1::OctetLength(field_id) => slot_for_field(snapshot, *field_id)
            .map(|slot| CompiledCheckValueExprV1::OctetLength(usize::from(slot.get())))
            .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression),
        AcceptedCheckValueExprV1::Cardinality(field_id) => slot_for_field(snapshot, *field_id)
            .map(|slot| CompiledCheckValueExprV1::Cardinality(usize::from(slot.get())))
            .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression),
    }
}

pub(in crate::db::schema) fn decode_literal(
    literal: &AcceptedCheckLiteralV1,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Value, AcceptedRowConstraintEvaluationError> {
    decode_literal_from_catalogs(
        literal,
        value_catalog.enum_catalog(),
        value_catalog.composite_catalog(),
    )
}

fn decode_literal_from_catalogs(
    literal: &AcceptedCheckLiteralV1,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<Value, AcceptedRowConstraintEvaluationError> {
    let field = AcceptedFieldDecodeContract::new(
        "__icydb_check_literal",
        literal.kind(),
        false,
        literal.storage_decode(),
        literal.leaf_codec(),
    );
    decode_validated_check_literal_payload(
        enum_catalog,
        composite_catalog,
        field,
        literal.payload(),
    )
    .map_err(|_| AcceptedRowConstraintEvaluationError::LiteralCorrupt)
}

fn evaluate_expr(
    expression: &CompiledCheckExprV1,
    values: &[Option<Cow<'_, Value>>],
    remaining_work: &mut u32,
) -> Result<AcceptedCheckTruth, AcceptedRowConstraintEvaluationError> {
    *remaining_work = remaining_work
        .checked_sub(1)
        .ok_or(AcceptedRowConstraintEvaluationError::WorkBudgetExceeded)?;
    match expression {
        CompiledCheckExprV1::True => Ok(AcceptedCheckTruth::True),
        CompiledCheckExprV1::False => Ok(AcceptedCheckTruth::False),
        CompiledCheckExprV1::Not(inner) => {
            evaluate_expr(inner, values, remaining_work).map(AcceptedCheckTruth::not)
        }
        CompiledCheckExprV1::And(children) => {
            let mut result = AcceptedCheckTruth::True;
            for child in children {
                match evaluate_expr(child, values, remaining_work)? {
                    AcceptedCheckTruth::False => return Ok(AcceptedCheckTruth::False),
                    AcceptedCheckTruth::Unknown => result = AcceptedCheckTruth::Unknown,
                    AcceptedCheckTruth::True => {}
                }
            }
            Ok(result)
        }
        CompiledCheckExprV1::Or(children) => {
            let mut result = AcceptedCheckTruth::False;
            for child in children {
                match evaluate_expr(child, values, remaining_work)? {
                    AcceptedCheckTruth::True => return Ok(AcceptedCheckTruth::True),
                    AcceptedCheckTruth::Unknown => result = AcceptedCheckTruth::Unknown,
                    AcceptedCheckTruth::False => {}
                }
            }
            Ok(result)
        }
        CompiledCheckExprV1::Compare { left, op, right } => {
            let left = evaluate_value(left, values)?;
            let right = evaluate_value(right, values)?;
            compare_values(left.as_ref(), *op, right.as_ref())
        }
        CompiledCheckExprV1::IsNull(value) => Ok(
            if matches!(evaluate_value(value, values)?.as_ref(), Value::Null) {
                AcceptedCheckTruth::True
            } else {
                AcceptedCheckTruth::False
            },
        ),
        CompiledCheckExprV1::IsNotNull(value) => Ok(
            if matches!(evaluate_value(value, values)?.as_ref(), Value::Null) {
                AcceptedCheckTruth::False
            } else {
                AcceptedCheckTruth::True
            },
        ),
    }
}

fn evaluate_value<'a>(
    expression: &'a CompiledCheckValueExprV1,
    values: &'a [Option<Cow<'_, Value>>],
) -> Result<Cow<'a, Value>, AcceptedRowConstraintEvaluationError> {
    let value_at = |slot: usize| {
        values
            .get(slot)
            .and_then(Option::as_deref)
            .map(Cow::Borrowed)
            .ok_or(AcceptedRowConstraintEvaluationError::MissingSlot)
    };
    match expression {
        CompiledCheckValueExprV1::Field(slot) => value_at(*slot),
        CompiledCheckValueExprV1::Literal(value) => Ok(Cow::Borrowed(value)),
        CompiledCheckValueExprV1::CharLength(slot) => evaluate_length_value(
            value_at(*slot)?.as_ref(),
            AcceptedValueLengthKind::Characters,
        ),
        CompiledCheckValueExprV1::OctetLength(slot) => {
            evaluate_length_value(value_at(*slot)?.as_ref(), AcceptedValueLengthKind::Octets)
        }
        CompiledCheckValueExprV1::Cardinality(slot) => evaluate_length_value(
            value_at(*slot)?.as_ref(),
            AcceptedValueLengthKind::Cardinality,
        ),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AcceptedValueLengthKind {
    Characters,
    Octets,
    Cardinality,
}

fn evaluate_length_value(
    value: &Value,
    kind: AcceptedValueLengthKind,
) -> Result<Cow<'static, Value>, AcceptedRowConstraintEvaluationError> {
    if matches!(value, Value::Null) {
        return Ok(Cow::Owned(Value::Null));
    }
    accepted_value_length(value, kind)
        .map(Value::Nat64)
        .map(Cow::Owned)
}

pub(super) fn accepted_value_length(
    value: &Value,
    kind: AcceptedValueLengthKind,
) -> Result<u64, AcceptedRowConstraintEvaluationError> {
    let length = match (kind, value) {
        (AcceptedValueLengthKind::Characters, Value::Text(value)) => value.chars().count(),
        (AcceptedValueLengthKind::Octets, Value::Blob(value)) => value.len(),
        (AcceptedValueLengthKind::Cardinality, Value::List(value)) => value.len(),
        (AcceptedValueLengthKind::Cardinality, Value::Map(value)) => value.len(),
        _ => return Err(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch),
    };
    u64::try_from(length).map_err(|_| AcceptedRowConstraintEvaluationError::RuntimeValueMismatch)
}

pub(super) fn compare_values(
    left: &Value,
    op: AcceptedCheckCompareOpV1,
    right: &Value,
) -> Result<AcceptedCheckTruth, AcceptedRowConstraintEvaluationError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(AcceptedCheckTruth::Unknown);
    }
    let result = match op {
        AcceptedCheckCompareOpV1::Eq => left == right,
        AcceptedCheckCompareOpV1::Ne => left != right,
        AcceptedCheckCompareOpV1::Lt
        | AcceptedCheckCompareOpV1::Lte
        | AcceptedCheckCompareOpV1::Gt
        | AcceptedCheckCompareOpV1::Gte => {
            let ordering = Value::strict_order_cmp(left, right)
                .ok_or(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch)?;
            match op {
                AcceptedCheckCompareOpV1::Lt => ordering == Ordering::Less,
                AcceptedCheckCompareOpV1::Lte => ordering != Ordering::Greater,
                AcceptedCheckCompareOpV1::Gt => ordering == Ordering::Greater,
                AcceptedCheckCompareOpV1::Gte => ordering != Ordering::Less,
                AcceptedCheckCompareOpV1::Eq | AcceptedCheckCompareOpV1::Ne => false,
            }
        }
    };
    Ok(if result {
        AcceptedCheckTruth::True
    } else {
        AcceptedCheckTruth::False
    })
}

fn unique_index_dependency_slots(
    schema: &AcceptedSchemaSnapshot,
    index: &crate::db::schema::PersistedIndexSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    work: &dyn ConstructionBudget,
) -> Result<Vec<usize>, InternalError> {
    let snapshot = schema.persisted_snapshot();
    let slot_count = snapshot.row_layout().allocated_slot_count();
    work.charge(Resource::PredicateExpressionSteps, slot_count as u64)?;
    let mut required = work.vec_with_capacity(slot_count)?;
    required.resize(slot_count, false);
    work.charge(
        Resource::PredicateExpressionSteps,
        snapshot.fields().len() as u64,
    )?;
    for field in snapshot.fields() {
        if index.key().references_field(field.id()) {
            let slot = snapshot
                .row_layout()
                .slot_for_field(field.id())
                .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)?;
            let required_slot = required
                .get_mut(usize::from(slot.get()))
                .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)?;
            *required_slot = true;
        }
    }
    if let Some(bound) = index.predicate() {
        // Snapshot acceptance already validates the encoding. Dependency selection
        // needs only the budgeted semantic projection, not a serialized key.
        let predicate = bound
            .to_predicate(snapshot.fields(), value_catalog, Some(work))
            .map_err(|error| {
                if error.class() == crate::error::ErrorClass::Corruption {
                    InternalError::accepted_row_constraint_program_corrupt()
                } else {
                    error
                }
            })?;
        // Project the accepted tree for the existing normalization owner so
        // discarded branches do not gain independent write barriers.
        normalize_with_budget(predicate, work)?.try_for_each_field(&mut |name| {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            let slot = snapshot
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .and_then(|field| snapshot.row_layout().slot_for_field(field.id()))
                .and_then(|slot| required.get_mut(usize::from(slot.get())))
                .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)?;
            *slot = true;
            Ok::<(), InternalError>(())
        })?;
    }
    // Count and emit in slot order, admitting both bitmap passes and only the
    // backing needed for actual dependencies. Normalization preserves which
    // authored fields survive simplification; no new dependency policy lives here.
    work.charge(
        Resource::PredicateExpressionSteps,
        2 * required.len() as u64,
    )?;
    let mut slots = work.vec_with_capacity(required.iter().filter(|needed| **needed).count())?;
    for (slot, required) in required.into_iter().enumerate() {
        if required {
            slots.push(slot);
        }
    }
    Ok(slots)
}

pub(in crate::db::schema) fn validate_accepted_check_literals(
    schema: &AcceptedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedRowConstraintEvaluationError> {
    for constraint in schema.persisted_snapshot().constraints() {
        if let AcceptedConstraintKind::Check { expression } = constraint.kind() {
            expression
                .validate(schema.persisted_snapshot(), composite_catalog)
                .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)?;
            validate_expression_literals(expression, enum_catalog, composite_catalog)?;
        }
    }
    Ok(())
}

pub(in crate::db::schema) fn validate_accepted_rule_operation_literals(
    operation: &crate::db::schema::AcceptedRuleOperation,
    resolved_kind: &crate::db::schema::AcceptedFieldKind,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedRowConstraintEvaluationError> {
    let decode = |literal: &AcceptedCheckLiteralV1| {
        if literal.kind() != resolved_kind {
            return Err(AcceptedRowConstraintEvaluationError::LiteralCorrupt);
        }
        decode_literal_from_catalogs(literal, enum_catalog, composite_catalog)
    };
    match operation {
        crate::db::schema::AcceptedRuleOperation::LengthRangeInclusive { .. } => Ok(()),
        crate::db::schema::AcceptedRuleOperation::MultipleOf { divisor } => {
            let divisor = decode(divisor)?;
            if !matches!(exact_numeric_is_zero(&divisor), Some(false)) {
                return Err(AcceptedRowConstraintEvaluationError::LiteralCorrupt);
            }
            Ok(())
        }
        crate::db::schema::AcceptedRuleOperation::NumericMaximumInclusive { value }
        | crate::db::schema::AcceptedRuleOperation::NumericMinimumInclusive { value } => {
            let _ = decode(value)?;
            Ok(())
        }
        crate::db::schema::AcceptedRuleOperation::NumericRangeInclusive { min, max } => {
            let min = decode(min)?;
            let max = decode(max)?;
            if Value::strict_order_cmp(&min, &max)
                .is_none_or(|ordering| ordering == Ordering::Greater)
            {
                return Err(AcceptedRowConstraintEvaluationError::LiteralCorrupt);
            }
            Ok(())
        }
    }
}

pub(super) fn exact_numeric_is_zero(value: &Value) -> Option<bool> {
    match value {
        Value::Decimal(value) => Some(value.is_zero()),
        Value::Int64(value) => Some(*value == 0),
        Value::Int128(value) => Some(*value == 0),
        Value::Nat64(value) => Some(*value == 0),
        Value::Nat128(value) => Some(*value == 0),
        Value::Account(_)
        | Value::Blob(_)
        | Value::Bool(_)
        | Value::Date(_)
        | Value::Duration(_)
        | Value::Enum(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::List(_)
        | Value::Map(_)
        | Value::Null
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Text(_)
        | Value::Timestamp(_)
        | Value::Ulid(_)
        | Value::Unit => None,
        Value::U256(value) => Some(*value == crate::types::U256::ZERO),
    }
}

pub(super) fn exact_numeric_is_multiple(value: &Value, divisor: &Value) -> Option<bool> {
    if exact_numeric_is_zero(divisor) != Some(false) {
        return None;
    }
    match (value, divisor) {
        (Value::Decimal(value), Value::Decimal(divisor)) => value
            .checked_rem(*divisor)
            .map(|remainder| remainder.is_zero()),
        (Value::Int64(value), Value::Int64(divisor)) => {
            if *divisor == -1 {
                return Some(true);
            }
            value.checked_rem(*divisor).map(|remainder| remainder == 0)
        }
        (Value::Int128(value), Value::Int128(divisor)) => {
            if *divisor == -1 {
                return Some(true);
            }
            value.checked_rem(*divisor).map(|remainder| remainder == 0)
        }
        (Value::Nat64(value), Value::Nat64(divisor)) => Some(value % divisor == 0),
        (Value::Nat128(value), Value::Nat128(divisor)) => Some(value % divisor == 0),
        _ => None,
    }
}

fn validate_expression_literals(
    expression: &AcceptedCheckExprV1,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedRowConstraintEvaluationError> {
    match expression {
        AcceptedCheckExprV1::True | AcceptedCheckExprV1::False => Ok(()),
        AcceptedCheckExprV1::Not(inner) => {
            validate_expression_literals(inner, enum_catalog, composite_catalog)
        }
        AcceptedCheckExprV1::And(children) | AcceptedCheckExprV1::Or(children) => {
            for child in children {
                validate_expression_literals(child, enum_catalog, composite_catalog)?;
            }
            Ok(())
        }
        AcceptedCheckExprV1::Compare { left, right, .. } => {
            validate_value_literal(left, enum_catalog, composite_catalog)?;
            validate_value_literal(right, enum_catalog, composite_catalog)
        }
        AcceptedCheckExprV1::IsNull(value) | AcceptedCheckExprV1::IsNotNull(value) => {
            validate_value_literal(value, enum_catalog, composite_catalog)
        }
    }
}

fn validate_value_literal(
    value: &AcceptedCheckValueExprV1,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedRowConstraintEvaluationError> {
    if let AcceptedCheckValueExprV1::Literal(literal) = value {
        let _ = decode_literal_from_catalogs(literal, enum_catalog, composite_catalog)?;
    }
    Ok(())
}

#[cfg(test)]
mod helper_tests;
