//! Fingerprint-bound compilation and SQL three-valued check evaluation.

use super::{
    AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckExprV1Error,
    AcceptedCheckLiteralV1, AcceptedCheckValueExprV1, MAX_CHECK_EXPR_V1_NODES, slot_for_field,
};
use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::{
            AcceptedFieldWriteProvenance, StructuralRowContract,
            decode_validated_check_literal_payload,
        },
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        schema::{
            AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, ConstraintActivationKind, ConstraintId,
        },
    },
    sanitize::SanitizeWriteMode,
    value::Value,
};
use std::{borrow::Cow, cmp::Ordering};

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
    /// A pending not-null gate observed an explicit null value.
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
    Violation {
        constraint_id: ConstraintId,
        constraint_name: String,
        kind: AcceptedRowConstraintViolationKind,
        field_paths: Vec<String>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledAcceptedCheck {
    id: ConstraintId,
    name: String,
    field_paths: Vec<String>,
    expression: CompiledCheckExprV1,
}

/// Reserved unique identity plus the slots whose authorship cannot safely
/// change while its planner-invisible candidate generation is incomplete.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledUniqueWriteBarrier {
    id: ConstraintId,
    name: String,
    dependency_slots: Vec<usize>,
    field_paths: Vec<String>,
}

impl CompiledUniqueWriteBarrier {
    /// Return the stable accepted constraint identity.
    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.id
    }

    /// Borrow the stable accepted constraint name.
    #[must_use]
    pub(in crate::db) const fn constraint_name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow bounded accepted field paths guarded by this activation.
    #[must_use]
    pub(in crate::db) const fn field_paths(&self) -> &[String] {
        self.field_paths.as_slice()
    }
}

/// One compiled row-local constraint whose identity and slot bindings are frozen.
#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledAcceptedRowConstraint {
    /// One accepted check expression.
    Check(CompiledAcceptedCheck),
    /// One pending not-null write gate.
    NotNull {
        id: ConstraintId,
        name: String,
        slot: usize,
        field_path: String,
    },
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
    required_slots: Vec<usize>,
    unique_write_barriers: Vec<CompiledUniqueWriteBarrier>,
    field_count: usize,
}

impl CompiledAcceptedRowConstraints {
    /// Compile only validated accepted row-local constraints for integrity use.
    ///
    /// Pending activations are intentionally excluded: historical violations
    /// against them are migration findings, not accepted-state corruption.
    /// Accepted not-null fields remain owned by the structural row contract,
    /// which rejects an explicit null while validating every declared slot.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "0.211 exposes the accepted-only check program for the 0.212 integrity consumer"
        )
    )]
    pub(in crate::db) fn compile_validated_checks(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let check_sources = schema
            .persisted_snapshot()
            .constraints()
            .iter()
            .filter_map(|constraint| match constraint.kind() {
                AcceptedConstraintKind::Check { expression } => {
                    Some((constraint.id(), constraint.name(), expression.as_ref()))
                }
                AcceptedConstraintKind::PrimaryKey
                | AcceptedConstraintKind::NotNull { .. }
                | AcceptedConstraintKind::Unique { .. }
                | AcceptedConstraintKind::Relation { .. } => None,
            })
            .collect();
        Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            check_sources,
            Vec::new(),
        )
    }

    /// Compile every accepted row-local constraint and pending write gate.
    pub(in crate::db) fn compile(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let check_sources = snapshot
            .constraints()
            .iter()
            .filter_map(|constraint| match constraint.kind() {
                AcceptedConstraintKind::Check { expression } => {
                    Some((constraint.id(), constraint.name(), expression.as_ref()))
                }
                AcceptedConstraintKind::PrimaryKey
                | AcceptedConstraintKind::NotNull { .. }
                | AcceptedConstraintKind::Unique { .. }
                | AcceptedConstraintKind::Relation { .. } => None,
            })
            .chain(
                snapshot
                    .constraint_activations()
                    .iter()
                    .filter_map(|activation| match activation.kind() {
                        ConstraintActivationKind::Check { expression } => {
                            Some((activation.id(), activation.name(), expression.as_ref()))
                        }
                        ConstraintActivationKind::NotNull { .. }
                        | ConstraintActivationKind::Unique { .. }
                        | ConstraintActivationKind::Relation { .. } => None,
                    }),
            )
            .collect::<Vec<_>>();
        let not_null_sources = snapshot
            .constraint_activations()
            .iter()
            .filter_map(|activation| match activation.kind() {
                ConstraintActivationKind::NotNull { field_id } => {
                    Some((activation.id(), activation.name(), *field_id))
                }
                ConstraintActivationKind::Unique { .. }
                | ConstraintActivationKind::Relation { .. }
                | ConstraintActivationKind::Check { .. } => None,
            })
            .collect::<Vec<_>>();
        let mut compiled = Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            check_sources,
            not_null_sources,
        )?;
        compiled.unique_write_barriers = snapshot
            .constraint_activations()
            .iter()
            .filter_map(|activation| match activation.kind() {
                ConstraintActivationKind::Unique { index_id } => {
                    Some((activation.id(), activation.name(), *index_id))
                }
                ConstraintActivationKind::Check { .. }
                | ConstraintActivationKind::NotNull { .. }
                | ConstraintActivationKind::Relation { .. } => None,
            })
            .map(|(id, name, index_id)| {
                let mut matching = snapshot
                    .candidate_indexes()
                    .iter()
                    .filter(|index| index.schema_id() == index_id);
                let index = matching.next().ok_or(
                    AcceptedRowConstraintEvaluationError::InvalidExpression(
                        AcceptedCheckExprV1Error::UnknownField,
                    ),
                )?;
                if matching.next().is_some() {
                    return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                        AcceptedCheckExprV1Error::UnknownField,
                    ));
                }
                let dependency_slots = unique_index_dependency_slots(schema, value_catalog, index)?;
                Ok(CompiledUniqueWriteBarrier {
                    id,
                    name: name.to_string(),
                    field_paths: field_paths_for_slots(snapshot, dependency_slots.as_slice())?,
                    dependency_slots,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        compiled
            .unique_write_barriers
            .sort_unstable_by_key(|barrier| barrier.id);
        Ok(compiled)
    }

    /// Compile exactly one pending check for historical activation validation.
    pub(in crate::db) fn compile_check_activation(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        activation_id: ConstraintId,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let activation = snapshot
            .constraint_activations()
            .iter()
            .find(|activation| activation.id() == activation_id)
            .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::UnknownField,
            ))?;
        let ConstraintActivationKind::Check { expression } = activation.kind() else {
            return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::OperandKindMismatch,
            ));
        };
        Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            vec![(activation.id(), activation.name(), expression.as_ref())],
            Vec::new(),
        )
    }

    /// Compile exactly one pending not-null gate for historical validation.
    pub(in crate::db) fn compile_not_null_activation(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        activation_id: ConstraintId,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let activation = snapshot
            .constraint_activations()
            .iter()
            .find(|activation| activation.id() == activation_id)
            .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::UnknownField,
            ))?;
        let ConstraintActivationKind::NotNull { field_id } = activation.kind() else {
            return Err(AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::OperandKindMismatch,
            ));
        };
        Self::compile_sources(
            schema,
            value_catalog,
            fingerprint,
            Vec::new(),
            vec![(activation.id(), activation.name(), *field_id)],
        )
    }

    fn compile_sources(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        fingerprint: CommitSchemaFingerprint,
        check_sources: Vec<(ConstraintId, &str, &AcceptedCheckExprV1)>,
        not_null_sources: Vec<(ConstraintId, &str, crate::db::schema::FieldId)>,
    ) -> Result<Self, AcceptedRowConstraintEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let checks = check_sources
            .iter()
            .copied()
            .map(|(id, name, expression)| {
                expression
                    .validate(snapshot)
                    .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)?;
                Ok(CompiledAcceptedRowConstraint::Check(
                    CompiledAcceptedCheck {
                        id,
                        name: name.to_string(),
                        field_paths: expression
                            .dependencies()
                            .into_iter()
                            .map(|field_id| {
                                snapshot
                                    .fields()
                                    .iter()
                                    .find(|field| field.id() == field_id)
                                    .map(|field| field.name().to_string())
                                    .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                                        AcceptedCheckExprV1Error::UnknownField,
                                    ))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        expression: compile_expr(expression, snapshot, value_catalog)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>, AcceptedRowConstraintEvaluationError>>()?;
        let not_null_constraints = not_null_sources
            .iter()
            .map(|(id, name, field_id)| {
                let slot = slot_for_field(snapshot, *field_id)
                    .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)?;
                let field_path = snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == *field_id)
                    .map(|field| field.name().to_string())
                    .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                        AcceptedCheckExprV1Error::UnknownField,
                    ))?;
                Ok(CompiledAcceptedRowConstraint::NotNull {
                    id: *id,
                    name: (*name).to_string(),
                    slot: usize::from(slot.get()),
                    field_path,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut constraints = checks
            .into_iter()
            .chain(not_null_constraints)
            .collect::<Vec<_>>();
        constraints.sort_unstable_by_key(CompiledAcceptedRowConstraint::id);
        let mut required_slots = check_sources
            .iter()
            .flat_map(|(_, _, expression)| expression.dependencies())
            .map(|field_id| {
                slot_for_field(snapshot, field_id)
                    .map(|slot| usize::from(slot.get()))
                    .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)
            })
            .chain(not_null_sources.iter().map(|(_, _, field_id)| {
                slot_for_field(snapshot, *field_id)
                    .map(|slot| usize::from(slot.get()))
                    .map_err(AcceptedRowConstraintEvaluationError::InvalidExpression)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        required_slots.sort_unstable();
        required_slots.dedup();

        Ok(Self {
            fingerprint,
            constraints,
            required_slots,
            unique_write_barriers: Vec::new(),
            field_count: snapshot.row_layout().allocated_slot_count(),
        })
    }

    /// Return whether this schema has any row-local constraints or gates.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.constraints.is_empty() && self.unique_write_barriers.is_empty()
    }

    /// Borrow the sorted unique row slots read by this compiled program.
    #[must_use]
    pub(in crate::db) const fn required_slots(&self) -> &[usize] {
        self.required_slots.as_slice()
    }

    /// Return the first incomplete unique activation that blocks this write.
    pub(in crate::db) fn unique_activation_write_blocker(
        &self,
        mode: SanitizeWriteMode,
        provenance: &[Option<AcceptedFieldWriteProvenance>],
    ) -> Result<Option<&CompiledUniqueWriteBarrier>, AcceptedRowConstraintEvaluationError> {
        if provenance.len() != self.field_count {
            return Err(AcceptedRowConstraintEvaluationError::MissingSlot);
        }
        Ok(self
            .unique_write_barriers
            .iter()
            .find(|barrier| match mode {
                SanitizeWriteMode::Insert | SanitizeWriteMode::Replace => true,
                SanitizeWriteMode::Update => barrier.dependency_slots.iter().any(|slot| {
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
    pub(in crate::db) fn evaluate(
        &self,
        current_fingerprint: CommitSchemaFingerprint,
        values_by_slot: &[Option<Value>],
    ) -> Result<(), AcceptedRowConstraintEvaluationError> {
        if current_fingerprint != self.fingerprint {
            return Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch);
        }
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
            match constraint {
                CompiledAcceptedRowConstraint::Check(check) => {
                    let truth =
                        evaluate_expr(&check.expression, values_by_slot, &mut remaining_work)?;
                    if truth == AcceptedCheckTruth::False {
                        return Err(AcceptedRowConstraintEvaluationError::Violation {
                            constraint_id: check.id,
                            constraint_name: check.name.clone(),
                            kind: AcceptedRowConstraintViolationKind::Check,
                            field_paths: check.field_paths.clone(),
                        });
                    }
                }
                CompiledAcceptedRowConstraint::NotNull {
                    id,
                    name,
                    slot,
                    field_path,
                } => {
                    let value = values_by_slot
                        .get(*slot)
                        .and_then(Option::as_ref)
                        .ok_or(AcceptedRowConstraintEvaluationError::MissingSlot)?;
                    if matches!(value, Value::Null) {
                        return Err(AcceptedRowConstraintEvaluationError::Violation {
                            constraint_id: *id,
                            constraint_name: name.clone(),
                            kind: AcceptedRowConstraintViolationKind::NotNull,
                            field_paths: vec![field_path.clone()],
                        });
                    }
                }
            }
        }
        Ok(())
    }
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
        AcceptedCheckExprV1::And(children) => children
            .iter()
            .map(|child| compile_expr(child, snapshot, value_catalog))
            .collect::<Result<Vec<_>, _>>()
            .map(CompiledCheckExprV1::And),
        AcceptedCheckExprV1::Or(children) => children
            .iter()
            .map(|child| compile_expr(child, snapshot, value_catalog))
            .collect::<Result<Vec<_>, _>>()
            .map(CompiledCheckExprV1::Or),
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

pub(super) fn decode_literal(
    literal: &AcceptedCheckLiteralV1,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Value, AcceptedRowConstraintEvaluationError> {
    let field = AcceptedFieldDecodeContract::new(
        "__icydb_check_literal",
        literal.kind(),
        false,
        literal.storage_decode(),
        literal.leaf_codec(),
    );
    decode_validated_check_literal_payload(
        value_catalog.enum_catalog(),
        value_catalog.composite_catalog(),
        field,
        literal.payload(),
    )
    .map_err(|_| AcceptedRowConstraintEvaluationError::LiteralCorrupt)
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
    values: &[Option<Value>],
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
    values: &'a [Option<Value>],
) -> Result<Cow<'a, Value>, AcceptedRowConstraintEvaluationError> {
    let value_at = |slot: usize| {
        values
            .get(slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed)
            .ok_or(AcceptedRowConstraintEvaluationError::MissingSlot)
    };
    match expression {
        CompiledCheckValueExprV1::Field(slot) => value_at(*slot),
        CompiledCheckValueExprV1::Literal(value) => Ok(Cow::Borrowed(value)),
        CompiledCheckValueExprV1::CharLength(slot) => match value_at(*slot)?.as_ref() {
            Value::Null => Ok(Cow::Owned(Value::Null)),
            Value::Text(value) => usize_to_nat64(value.chars().count()),
            _ => Err(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch),
        },
        CompiledCheckValueExprV1::OctetLength(slot) => match value_at(*slot)?.as_ref() {
            Value::Null => Ok(Cow::Owned(Value::Null)),
            Value::Blob(value) => usize_to_nat64(value.len()),
            _ => Err(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch),
        },
        CompiledCheckValueExprV1::Cardinality(slot) => match value_at(*slot)?.as_ref() {
            Value::Null => Ok(Cow::Owned(Value::Null)),
            Value::List(value) => usize_to_nat64(value.len()),
            Value::Map(value) => usize_to_nat64(value.len()),
            _ => Err(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch),
        },
    }
}

fn usize_to_nat64(
    value: usize,
) -> Result<Cow<'static, Value>, AcceptedRowConstraintEvaluationError> {
    u64::try_from(value)
        .map(Value::Nat64)
        .map(Cow::Owned)
        .map_err(|_| AcceptedRowConstraintEvaluationError::RuntimeValueMismatch)
}

fn compare_values(
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
    value_catalog: &AcceptedValueCatalogHandle,
    index: &crate::db::schema::PersistedIndexSnapshot,
) -> Result<Vec<usize>, AcceptedRowConstraintEvaluationError> {
    let snapshot = schema.persisted_snapshot();
    let mut required = vec![false; snapshot.row_layout().allocated_slot_count()];
    for field in snapshot.fields() {
        if index.key().references_field(field.id()) {
            let slot = snapshot.row_layout().slot_for_field(field.id()).ok_or(
                AcceptedRowConstraintEvaluationError::InvalidExpression(
                    AcceptedCheckExprV1Error::UnknownField,
                ),
            )?;
            let required_slot = required.get_mut(usize::from(slot.get())).ok_or(
                AcceptedRowConstraintEvaluationError::InvalidExpression(
                    AcceptedCheckExprV1Error::UnknownField,
                ),
            )?;
            *required_slot = true;
        }
    }
    if let Some(predicate_sql) = index.predicate_sql() {
        let runtime =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(schema).map_err(|_| {
                AcceptedRowConstraintEvaluationError::InvalidExpression(
                    AcceptedCheckExprV1Error::UnknownField,
                )
            })?;
        let decode_contract = runtime.row_decode_contract(value_catalog.clone());
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            "accepted unique index",
            decode_contract,
        );
        let predicate = parse_sql_predicate(predicate_sql).map_err(|_| {
            AcceptedRowConstraintEvaluationError::InvalidExpression(
                AcceptedCheckExprV1Error::OperandKindMismatch,
            )
        })?;
        let program =
            PredicateProgram::compile_with_row_contract(&row_contract, &normalize(&predicate));
        program.mark_referenced_slots(required.as_mut_slice());
    }
    Ok(required
        .into_iter()
        .enumerate()
        .filter_map(|(slot, required)| required.then_some(slot))
        .collect())
}

fn field_paths_for_slots(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    slots: &[usize],
) -> Result<Vec<String>, AcceptedRowConstraintEvaluationError> {
    slots
        .iter()
        .map(|slot| {
            snapshot
                .fields()
                .iter()
                .find(|field| {
                    snapshot
                        .row_layout()
                        .slot_for_field(field.id())
                        .is_some_and(|field_slot| usize::from(field_slot.get()) == *slot)
                })
                .map(|field| field.name().to_string())
                .ok_or(AcceptedRowConstraintEvaluationError::InvalidExpression(
                    AcceptedCheckExprV1Error::UnknownField,
                ))
        })
        .collect()
}

pub(in crate::db::schema) fn validate_accepted_check_literals(
    schema: &AcceptedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<(), AcceptedRowConstraintEvaluationError> {
    for constraint in schema.persisted_snapshot().constraints() {
        if let AcceptedConstraintKind::Check { expression } = constraint.kind() {
            validate_expression_literals(expression, enum_catalog, composite_catalog)?;
        }
    }
    Ok(())
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
