//! Bounded accepted-target traversal and closed rule evaluation.

use super::{
    AcceptedCheckCompareOpV1, AcceptedCheckExprV1Error,
    compile::{
        AcceptedCheckTruth, AcceptedValueLengthKind, accepted_value_length, compare_values,
        decode_literal, exact_numeric_is_multiple,
    },
};
use crate::{
    db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedFieldKind,
        AcceptedNamedTypeIdentity, AcceptedRuleOperation, AcceptedRuleTarget,
        AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, ConstraintActivationKind, ConstraintId,
        FieldId, MAX_ACCEPTED_RECURSIVE_DEPTH_U16,
        composite_catalog::{
            AcceptedCompositeField, AcceptedCompositeShape, CompositeFieldId, CompositeTypeId,
        },
        enum_catalog::{
            AcceptedEnumVariantBody, EnumTypeId, EnumVariantId, MAX_ACCEPTED_VALUE_BYTES,
        },
    },
    error::{ConstraintValuePath, ConstraintValuePathComponent},
    value::{CanonicalEnumBody, Value},
};
use std::collections::BTreeMap;

pub(in crate::db) const MAX_ACCEPTED_TARGET_PATH_COMPONENTS: usize =
    MAX_ACCEPTED_RECURSIVE_DEPTH_U16 as usize;

/// Stable coordinate in one finite accepted value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedTargetPathComponent {
    RootField(FieldId),
    RecordMember {
        composite_type_id: CompositeTypeId,
        member_id: CompositeFieldId,
    },
    TupleElement {
        composite_type_id: CompositeTypeId,
        ordinal: u32,
    },
    Newtype {
        composite_type_id: CompositeTypeId,
    },
    EnumVariant {
        enum_type_id: EnumTypeId,
        variant_id: EnumVariantId,
    },
    ListElement {
        index: u32,
    },
    SetElement {
        index: u32,
    },
    MapEntryKey {
        index: u32,
    },
    MapEntryValue {
        index: u32,
    },
}

/// Bounded identity-only path to one deterministic targeted-rule occurrence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedTargetPath(Vec<AcceptedTargetPathComponent>);

impl AcceptedTargetPath {
    #[must_use]
    pub(in crate::db) const fn new(components: Vec<AcceptedTargetPathComponent>) -> Self {
        Self(components)
    }

    pub(in crate::db) fn into_constraint_value_path(self) -> ConstraintValuePath {
        ConstraintValuePath::new(
            self.0
                .into_iter()
                .map(AcceptedTargetPathComponent::into_constraint_component)
                .collect(),
        )
    }

    #[must_use]
    pub(in crate::db) const fn components(&self) -> &[AcceptedTargetPathComponent] {
        self.0.as_slice()
    }

    pub(in crate::db) fn to_constraint_value_path(&self) -> ConstraintValuePath {
        ConstraintValuePath::new(
            self.0
                .iter()
                .cloned()
                .map(AcceptedTargetPathComponent::into_constraint_component)
                .collect(),
        )
    }
}

impl AcceptedTargetPathComponent {
    const fn into_constraint_component(self) -> ConstraintValuePathComponent {
        match self {
            Self::RootField(field_id) => ConstraintValuePathComponent::RootField {
                field_id: field_id.get(),
            },
            Self::RecordMember {
                composite_type_id,
                member_id,
            } => ConstraintValuePathComponent::RecordMember {
                composite_type_id: composite_type_id.get(),
                member_id: member_id.get(),
            },
            Self::TupleElement {
                composite_type_id,
                ordinal,
            } => ConstraintValuePathComponent::TupleElement {
                composite_type_id: composite_type_id.get(),
                ordinal,
            },
            Self::Newtype { composite_type_id } => ConstraintValuePathComponent::Newtype {
                composite_type_id: composite_type_id.get(),
            },
            Self::EnumVariant {
                enum_type_id,
                variant_id,
            } => ConstraintValuePathComponent::EnumVariant {
                enum_type_id: enum_type_id.get(),
                variant_id: variant_id.get(),
            },
            Self::ListElement { index } => ConstraintValuePathComponent::ListElement { index },
            Self::SetElement { index } => ConstraintValuePathComponent::SetElement { index },
            Self::MapEntryKey { index } => ConstraintValuePathComponent::MapEntryKey { index },
            Self::MapEntryValue { index } => ConstraintValuePathComponent::MapEntryValue { index },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledAcceptedRuleOperation {
    LengthRange {
        length_kind: AcceptedValueLengthKind,
        min: u64,
        max: u64,
    },
    MultipleOf {
        divisor: Value,
    },
    NumericMaximum {
        value: Value,
    },
    NumericMinimum {
        value: Value,
    },
    NumericRange {
        min: Value,
        max: Value,
    },
}

impl CompiledAcceptedRuleOperation {
    fn evaluate(&self, value: &Value) -> Result<bool, AcceptedTargetedRuleEvaluationError> {
        if matches!(value, Value::Null) {
            return Ok(true);
        }
        match self {
            Self::LengthRange {
                length_kind,
                min,
                max,
            } => {
                let length = accepted_value_length(value, *length_kind)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                Ok((*min..=*max).contains(&length))
            }
            Self::MultipleOf { divisor } => exact_numeric_is_multiple(value, divisor)
                .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch),
            Self::NumericMaximum { value: maximum } => {
                compare_values(value, AcceptedCheckCompareOpV1::Lte, maximum)
                    .map(|truth| truth == AcceptedCheckTruth::True)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)
            }
            Self::NumericMinimum { value: minimum } => {
                compare_values(value, AcceptedCheckCompareOpV1::Gte, minimum)
                    .map(|truth| truth == AcceptedCheckTruth::True)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)
            }
            Self::NumericRange { min, max } => {
                let above_min = compare_values(value, AcceptedCheckCompareOpV1::Gte, min)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                let below_max = compare_values(value, AcceptedCheckCompareOpV1::Lte, max)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                Ok(above_min == AcceptedCheckTruth::True && below_max == AcceptedCheckTruth::True)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledAcceptedTargetedRule {
    id: ConstraintId,
    target_type: AcceptedNamedTypeIdentity,
    operation: CompiledAcceptedRuleOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledAcceptedTargetedRoot {
    field_id: FieldId,
    slot: usize,
    root_kind: AcceptedFieldKind,
    rules: Vec<CompiledAcceptedTargetedRule>,
    rule_ordinals_by_target: BTreeMap<AcceptedNamedTypeIdentity, Vec<usize>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CompiledAcceptedTargetedRules {
    roots: Vec<CompiledAcceptedTargetedRoot>,
    record_orders: BTreeMap<CompositeTypeId, Vec<usize>>,
    value_catalog: Option<Box<AcceptedValueCatalogHandle>>,
}

impl CompiledAcceptedTargetedRules {
    pub(super) const fn empty() -> Self {
        Self {
            roots: Vec::new(),
            record_orders: BTreeMap::new(),
            value_catalog: None,
        }
    }

    pub(super) fn compile(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Self, AcceptedTargetedRuleEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let accepted = snapshot.constraints().iter().filter_map(|constraint| {
            let AcceptedConstraintKind::TargetedRule { target, operation } = constraint.kind()
            else {
                return None;
            };
            Some((constraint.id(), *target, operation.as_ref()))
        });
        let activating = snapshot
            .constraint_activations()
            .iter()
            .filter_map(|activation| {
                let ConstraintActivationKind::TargetedRule { target, operation } =
                    activation.kind()
                else {
                    return None;
                };
                Some((activation.id(), *target, operation.as_ref()))
            });
        Self::compile_sources(schema, value_catalog, accepted.chain(activating))
    }

    pub(super) fn compile_activation(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        activation_id: ConstraintId,
    ) -> Result<Self, AcceptedTargetedRuleEvaluationError> {
        let activation = schema
            .persisted_snapshot()
            .constraint_activations()
            .iter()
            .find(|activation| activation.id() == activation_id)
            .ok_or(AcceptedTargetedRuleEvaluationError::InvalidTarget)?;
        let ConstraintActivationKind::TargetedRule { target, operation } = activation.kind() else {
            return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
        };
        Self::compile_sources(
            schema,
            value_catalog,
            std::iter::once((activation.id(), *target, operation.as_ref())),
        )
    }

    fn compile_sources<'a>(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        sources: impl Iterator<Item = (ConstraintId, AcceptedRuleTarget, &'a AcceptedRuleOperation)>,
    ) -> Result<Self, AcceptedTargetedRuleEvaluationError> {
        let snapshot = schema.persisted_snapshot();
        let mut roots = BTreeMap::<FieldId, CompiledAcceptedTargetedRoot>::new();
        for (constraint_id, target, operation) in sources {
            let field = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == target.root_field_id())
                .ok_or(AcceptedTargetedRuleEvaluationError::InvalidTarget)?;
            let slot = snapshot
                .row_layout()
                .slot_for_field(field.id())
                .ok_or(AcceptedTargetedRuleEvaluationError::InvalidTarget)?;
            let compiled_operation =
                compile_operation(target.target_type(), operation, value_catalog)?;
            let root = roots
                .entry(field.id())
                .or_insert_with(|| CompiledAcceptedTargetedRoot {
                    field_id: field.id(),
                    slot: usize::from(slot.get()),
                    root_kind: field.kind().clone(),
                    rules: Vec::new(),
                    rule_ordinals_by_target: BTreeMap::new(),
                });
            if root.slot != usize::from(slot.get()) || root.root_kind != *field.kind() {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            root.rules.push(CompiledAcceptedTargetedRule {
                id: constraint_id,
                target_type: target.target_type(),
                operation: compiled_operation,
            });
        }

        if roots.is_empty() {
            return Ok(Self::empty());
        }
        let mut roots = roots.into_values().collect::<Vec<_>>();
        for root in &mut roots {
            root.rules.sort_unstable_by_key(|rule| rule.id);
            for (ordinal, rule) in root.rules.iter().enumerate() {
                root.rule_ordinals_by_target
                    .entry(rule.target_type)
                    .or_default()
                    .push(ordinal);
            }
        }
        let record_orders = compile_record_orders(value_catalog.composite_catalog());
        Ok(Self {
            roots,
            record_orders,
            value_catalog: Some(Box::new(value_catalog.clone())),
        })
    }

    #[must_use]
    pub(super) const fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    pub(super) fn required_slots(&self) -> impl Iterator<Item = usize> + '_ {
        self.roots.iter().map(|root| root.slot)
    }

    pub(super) fn evaluate(
        &self,
        values_by_slot: &[Option<Value>],
    ) -> Result<Option<AcceptedTargetedRuleViolation>, AcceptedTargetedRuleEvaluationError> {
        self.evaluate_with_limits(values_by_slot, TargetedEvaluationLimits::standard(), None)
    }

    pub(super) fn evaluate_constraint(
        &self,
        constraint_id: ConstraintId,
        values_by_slot: &[Option<Value>],
    ) -> Result<Option<AcceptedTargetedRuleViolation>, AcceptedTargetedRuleEvaluationError> {
        self.evaluate_with_limits(
            values_by_slot,
            TargetedEvaluationLimits::standard(),
            Some(constraint_id),
        )
    }

    #[cfg(test)]
    pub(super) fn evaluate_with_limits_for_tests(
        &self,
        values_by_slot: &[Option<Value>],
        limits: TargetedEvaluationLimits,
    ) -> Result<Option<AcceptedTargetedRuleViolation>, AcceptedTargetedRuleEvaluationError> {
        self.evaluate_with_limits(values_by_slot, limits, None)
    }

    fn evaluate_with_limits(
        &self,
        values_by_slot: &[Option<Value>],
        limits: TargetedEvaluationLimits,
        only_constraint: Option<ConstraintId>,
    ) -> Result<Option<AcceptedTargetedRuleViolation>, AcceptedTargetedRuleEvaluationError> {
        if self.roots.is_empty() {
            return Ok(None);
        }
        let value_catalog = self
            .value_catalog
            .as_deref()
            .ok_or(AcceptedTargetedRuleEvaluationError::InvalidTarget)?;
        let mut budget = TargetedEvaluationBudget::new(limits);
        let mut first_violation = None;
        for root in &self.roots {
            let value = values_by_slot
                .get(root.slot)
                .and_then(Option::as_ref)
                .ok_or(AcceptedTargetedRuleEvaluationError::MissingSlot)?;
            let violations =
                self.evaluate_root(root, value_catalog, value, &mut budget, only_constraint)?;
            for (rule, path) in root.rules.iter().zip(violations) {
                let Some(path) = path else {
                    continue;
                };
                let violation = AcceptedTargetedRuleViolation {
                    constraint_id: rule.id,
                    path,
                };
                if first_violation
                    .as_ref()
                    .is_none_or(|current: &AcceptedTargetedRuleViolation| {
                        violation.constraint_id < current.constraint_id
                    })
                {
                    first_violation = Some(violation);
                }
            }
        }
        Ok(first_violation)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the iterative DFS state machine stays exhaustive and auditable in one owner"
    )]
    fn evaluate_root<'a>(
        &'a self,
        root: &'a CompiledAcceptedTargetedRoot,
        value_catalog: &'a AcceptedValueCatalogHandle,
        value: &'a Value,
        budget: &mut TargetedEvaluationBudget,
        only_constraint: Option<ConstraintId>,
    ) -> Result<Vec<Option<AcceptedTargetPath>>, AcceptedTargetedRuleEvaluationError> {
        let mut violations = vec![None; root.rules.len()];
        let mut path = vec![AcceptedTargetPathComponent::RootField(root.field_id)];
        let mut stack = vec![TargetTraversalFrame::Visit {
            kind: &root.root_kind,
            value,
            component: None,
            depth: 0,
        }];
        while let Some(frame) = stack.pop() {
            match frame {
                TargetTraversalFrame::Visit {
                    kind,
                    value,
                    component,
                    depth,
                } => {
                    budget.visit_node(depth)?;
                    if let Some(component) = component {
                        budget.push_path(path.len())?;
                        path.push(component);
                        stack.push(TargetTraversalFrame::PopPath);
                    }
                    if let Some(target_type) = named_type_identity(kind)
                        && let Some(ordinals) = root.rule_ordinals_by_target.get(&target_type)
                    {
                        for ordinal in ordinals {
                            if violations[*ordinal].is_some() {
                                continue;
                            }
                            if only_constraint.is_some_and(|constraint_id| {
                                root.rules[*ordinal].id != constraint_id
                            }) {
                                continue;
                            }
                            budget.evaluate_operation()?;
                            if !root.rules[*ordinal].operation.evaluate(value)? {
                                violations[*ordinal] = Some(AcceptedTargetPath(path.clone()));
                            }
                        }
                    }
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    self.push_children(kind, value, depth, value_catalog, &mut stack)?;
                }
                TargetTraversalFrame::PopPath => {
                    path.pop()
                        .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                }
                TargetTraversalFrame::Record {
                    composite_type_id,
                    fields,
                    entries,
                    order,
                    next,
                    depth,
                } => {
                    let Some(index) = order.get(next).copied() else {
                        continue;
                    };
                    let field = fields
                        .get(index)
                        .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                    let (_, value) = entries
                        .get(index)
                        .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                    stack.push(TargetTraversalFrame::Record {
                        composite_type_id,
                        fields,
                        entries,
                        order,
                        next: next.saturating_add(1),
                        depth,
                    });
                    stack.push(TargetTraversalFrame::Visit {
                        kind: field.contract().kind(),
                        value,
                        component: Some(AcceptedTargetPathComponent::RecordMember {
                            composite_type_id,
                            member_id: field.id(),
                        }),
                        depth: depth.saturating_add(1),
                    });
                }
                TargetTraversalFrame::Tuple {
                    composite_type_id,
                    elements,
                    values,
                    next,
                    depth,
                } => {
                    let Some((element, value)) = elements.get(next).zip(values.get(next)) else {
                        continue;
                    };
                    let ordinal = u32::try_from(next)
                        .map_err(|_| AcceptedTargetedRuleEvaluationError::PathBudgetExceeded)?;
                    stack.push(TargetTraversalFrame::Tuple {
                        composite_type_id,
                        elements,
                        values,
                        next: next.saturating_add(1),
                        depth,
                    });
                    stack.push(TargetTraversalFrame::Visit {
                        kind: element.kind(),
                        value,
                        component: Some(AcceptedTargetPathComponent::TupleElement {
                            composite_type_id,
                            ordinal,
                        }),
                        depth: depth.saturating_add(1),
                    });
                }
                TargetTraversalFrame::Collection {
                    kind,
                    values,
                    next,
                    depth,
                    is_set,
                } => {
                    let Some(value) = values.get(next) else {
                        continue;
                    };
                    let index = u32::try_from(next)
                        .map_err(|_| AcceptedTargetedRuleEvaluationError::PathBudgetExceeded)?;
                    let component = if is_set {
                        AcceptedTargetPathComponent::SetElement { index }
                    } else {
                        AcceptedTargetPathComponent::ListElement { index }
                    };
                    stack.push(TargetTraversalFrame::Collection {
                        kind,
                        values,
                        next: next.saturating_add(1),
                        depth,
                        is_set,
                    });
                    stack.push(TargetTraversalFrame::Visit {
                        kind,
                        value,
                        component: Some(component),
                        depth: depth.saturating_add(1),
                    });
                }
                TargetTraversalFrame::Map {
                    key_kind,
                    value_kind,
                    entries,
                    next,
                    side,
                    depth,
                } => {
                    let Some((key, value)) = entries.get(next) else {
                        continue;
                    };
                    let index = u32::try_from(next)
                        .map_err(|_| AcceptedTargetedRuleEvaluationError::PathBudgetExceeded)?;
                    match side {
                        MapEntrySide::Key => {
                            stack.push(TargetTraversalFrame::Map {
                                key_kind,
                                value_kind,
                                entries,
                                next,
                                side: MapEntrySide::Value,
                                depth,
                            });
                            stack.push(TargetTraversalFrame::Visit {
                                kind: key_kind,
                                value: key,
                                component: Some(AcceptedTargetPathComponent::MapEntryKey { index }),
                                depth: depth.saturating_add(1),
                            });
                        }
                        MapEntrySide::Value => {
                            stack.push(TargetTraversalFrame::Map {
                                key_kind,
                                value_kind,
                                entries,
                                next: next.saturating_add(1),
                                side: MapEntrySide::Key,
                                depth,
                            });
                            stack.push(TargetTraversalFrame::Visit {
                                kind: value_kind,
                                value,
                                component: Some(AcceptedTargetPathComponent::MapEntryValue {
                                    index,
                                }),
                                depth: depth.saturating_add(1),
                            });
                        }
                    }
                }
            }
        }
        Ok(violations)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive accepted-kind match makes every structural traversal edge explicit"
    )]
    fn push_children<'a>(
        &'a self,
        kind: &'a AcceptedFieldKind,
        value: &'a Value,
        depth: u16,
        value_catalog: &'a AcceptedValueCatalogHandle,
        stack: &mut Vec<TargetTraversalFrame<'a>>,
    ) -> Result<(), AcceptedTargetedRuleEvaluationError> {
        match kind {
            AcceptedFieldKind::Composite { type_id } => {
                let definition = value_catalog
                    .composite_catalog()
                    .composite_type(*type_id)
                    .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                match definition.shape() {
                    AcceptedCompositeShape::Record(fields) => {
                        let Value::Map(entries) = value else {
                            return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                        };
                        validate_record_shape(fields, entries)?;
                        let order = self
                            .record_orders
                            .get(type_id)
                            .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                        stack.push(TargetTraversalFrame::Record {
                            composite_type_id: *type_id,
                            fields,
                            entries,
                            order,
                            next: 0,
                            depth,
                        });
                    }
                    AcceptedCompositeShape::Tuple(elements) => {
                        let Value::List(values) = value else {
                            return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                        };
                        if elements.len() != values.len() {
                            return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                        }
                        stack.push(TargetTraversalFrame::Tuple {
                            composite_type_id: *type_id,
                            elements,
                            values,
                            next: 0,
                            depth,
                        });
                    }
                    AcceptedCompositeShape::Newtype(inner) => {
                        stack.push(TargetTraversalFrame::Visit {
                            kind: inner.kind(),
                            value,
                            component: Some(AcceptedTargetPathComponent::Newtype {
                                composite_type_id: *type_id,
                            }),
                            depth: depth.saturating_add(1),
                        });
                    }
                }
            }
            AcceptedFieldKind::Enum { type_id } => {
                let Value::Enum(enum_value) = value else {
                    return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                };
                if enum_value.type_id() != *type_id {
                    return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                }
                let definition = value_catalog
                    .enum_catalog()
                    .enum_type(*type_id)
                    .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                let variant = definition
                    .variant(enum_value.variant_id())
                    .ok_or(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch)?;
                match (variant.body(), enum_value.body()) {
                    (AcceptedEnumVariantBody::Unit, CanonicalEnumBody::Unit) => {}
                    (
                        AcceptedEnumVariantBody::Payload { contract },
                        CanonicalEnumBody::Payload(payload),
                    ) => {
                        stack.push(TargetTraversalFrame::Visit {
                            kind: contract.kind(),
                            value: payload,
                            component: Some(AcceptedTargetPathComponent::EnumVariant {
                                enum_type_id: *type_id,
                                variant_id: enum_value.variant_id(),
                            }),
                            depth: depth.saturating_add(1),
                        });
                    }
                    _ => return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch),
                }
            }
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
                let Value::List(values) = value else {
                    return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                };
                stack.push(TargetTraversalFrame::Collection {
                    kind: inner,
                    values,
                    next: 0,
                    depth,
                    is_set: matches!(kind, AcceptedFieldKind::Set(_)),
                });
            }
            AcceptedFieldKind::Map {
                key,
                value: value_kind,
            } => {
                let Value::Map(entries) = value else {
                    return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
                };
                stack.push(TargetTraversalFrame::Map {
                    key_kind: key,
                    value_kind,
                    entries,
                    next: 0,
                    side: MapEntrySide::Key,
                    depth,
                });
            }
            AcceptedFieldKind::Relation { .. }
            | AcceptedFieldKind::Account
            | AcceptedFieldKind::Blob { .. }
            | AcceptedFieldKind::Bool
            | AcceptedFieldKind::Date
            | AcceptedFieldKind::Decimal { .. }
            | AcceptedFieldKind::Duration
            | AcceptedFieldKind::Float32
            | AcceptedFieldKind::Float64
            | AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Int128
            | AcceptedFieldKind::IntBig { .. }
            | AcceptedFieldKind::Principal
            | AcceptedFieldKind::Subaccount
            | AcceptedFieldKind::Text { .. }
            | AcceptedFieldKind::Timestamp
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Nat128
            | AcceptedFieldKind::NatBig { .. }
            | AcceptedFieldKind::Ulid
            | AcceptedFieldKind::Unit => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AcceptedTargetedRuleViolation {
    pub(super) constraint_id: ConstraintId,
    pub(super) path: AcceptedTargetPath,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AcceptedTargetedRuleEvaluationError {
    InvalidTarget,
    LiteralCorrupt,
    MissingSlot,
    RuntimeValueMismatch,
    ValueDepthExceeded,
    ValueNodeBudgetExceeded,
    OperationBudgetExceeded,
    PathBudgetExceeded,
}

#[derive(Clone, Copy)]
pub(in crate::db) struct TargetedEvaluationLimits {
    depth: u16,
    value_nodes: u32,
    operations: u32,
    path_components: usize,
}

impl TargetedEvaluationLimits {
    const fn standard() -> Self {
        Self {
            depth: MAX_ACCEPTED_RECURSIVE_DEPTH_U16,
            value_nodes: MAX_ACCEPTED_VALUE_BYTES,
            operations: MAX_ACCEPTED_VALUE_BYTES,
            path_components: MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn for_tests(
        depth: u16,
        value_nodes: u32,
        operations: u32,
        path_components: usize,
    ) -> Self {
        Self {
            depth,
            value_nodes,
            operations,
            path_components,
        }
    }
}

struct TargetedEvaluationBudget {
    limits: TargetedEvaluationLimits,
    remaining_value_nodes: u32,
    remaining_operations: u32,
}

impl TargetedEvaluationBudget {
    const fn new(limits: TargetedEvaluationLimits) -> Self {
        Self {
            remaining_value_nodes: limits.value_nodes,
            remaining_operations: limits.operations,
            limits,
        }
    }

    fn visit_node(&mut self, depth: u16) -> Result<(), AcceptedTargetedRuleEvaluationError> {
        if depth >= self.limits.depth {
            return Err(AcceptedTargetedRuleEvaluationError::ValueDepthExceeded);
        }
        self.remaining_value_nodes = self
            .remaining_value_nodes
            .checked_sub(1)
            .ok_or(AcceptedTargetedRuleEvaluationError::ValueNodeBudgetExceeded)?;
        Ok(())
    }

    fn evaluate_operation(&mut self) -> Result<(), AcceptedTargetedRuleEvaluationError> {
        self.remaining_operations = self
            .remaining_operations
            .checked_sub(1)
            .ok_or(AcceptedTargetedRuleEvaluationError::OperationBudgetExceeded)?;
        Ok(())
    }

    const fn push_path(
        &self,
        current_len: usize,
    ) -> Result<(), AcceptedTargetedRuleEvaluationError> {
        if current_len >= self.limits.path_components {
            return Err(AcceptedTargetedRuleEvaluationError::PathBudgetExceeded);
        }
        Ok(())
    }
}

enum TargetTraversalFrame<'a> {
    Visit {
        kind: &'a AcceptedFieldKind,
        value: &'a Value,
        component: Option<AcceptedTargetPathComponent>,
        depth: u16,
    },
    PopPath,
    Record {
        composite_type_id: CompositeTypeId,
        fields: &'a [AcceptedCompositeField],
        entries: &'a [(Value, Value)],
        order: &'a [usize],
        next: usize,
        depth: u16,
    },
    Tuple {
        composite_type_id: CompositeTypeId,
        elements: &'a [crate::db::schema::composite_catalog::AcceptedCompositeElement],
        values: &'a [Value],
        next: usize,
        depth: u16,
    },
    Collection {
        kind: &'a AcceptedFieldKind,
        values: &'a [Value],
        next: usize,
        depth: u16,
        is_set: bool,
    },
    Map {
        key_kind: &'a AcceptedFieldKind,
        value_kind: &'a AcceptedFieldKind,
        entries: &'a [(Value, Value)],
        next: usize,
        side: MapEntrySide,
        depth: u16,
    },
}

#[derive(Clone, Copy)]
enum MapEntrySide {
    Key,
    Value,
}

fn compile_operation(
    target_type: AcceptedNamedTypeIdentity,
    operation: &AcceptedRuleOperation,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<CompiledAcceptedRuleOperation, AcceptedTargetedRuleEvaluationError> {
    if !operation.has_valid_local_shape() {
        return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
    }
    let target_kind = match target_type {
        AcceptedNamedTypeIdentity::Enum(type_id) => AcceptedFieldKind::Enum { type_id },
        AcceptedNamedTypeIdentity::Composite(type_id) => AcceptedFieldKind::Composite { type_id },
    };
    let resolved_kind = value_catalog
        .composite_catalog()
        .resolve_newtype_value_kind(&target_kind)
        .ok_or(AcceptedTargetedRuleEvaluationError::InvalidTarget)?;
    match operation {
        AcceptedRuleOperation::LengthRangeInclusive { min, max } => {
            let length_kind = match resolved_kind {
                AcceptedFieldKind::Text { .. } => AcceptedValueLengthKind::Characters,
                AcceptedFieldKind::Blob { .. } => AcceptedValueLengthKind::Octets,
                AcceptedFieldKind::List(_)
                | AcceptedFieldKind::Set(_)
                | AcceptedFieldKind::Map { .. } => AcceptedValueLengthKind::Cardinality,
                _ => return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget),
            };
            Ok(CompiledAcceptedRuleOperation::LengthRange {
                length_kind,
                min: *min,
                max: *max,
            })
        }
        AcceptedRuleOperation::NumericMinimumInclusive { value } => {
            if value.kind() != &resolved_kind {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            Ok(CompiledAcceptedRuleOperation::NumericMinimum {
                value: decode_literal(value, value_catalog)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?,
            })
        }
        AcceptedRuleOperation::NumericMaximumInclusive { value } => {
            if value.kind() != &resolved_kind {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            Ok(CompiledAcceptedRuleOperation::NumericMaximum {
                value: decode_literal(value, value_catalog)
                    .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?,
            })
        }
        AcceptedRuleOperation::NumericRangeInclusive { min, max } => {
            if min.kind() != &resolved_kind || max.kind() != &resolved_kind {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            let min = decode_literal(min, value_catalog)
                .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?;
            let max = decode_literal(max, value_catalog)
                .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?;
            if compare_values(&min, AcceptedCheckCompareOpV1::Lte, &max)
                .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?
                != AcceptedCheckTruth::True
            {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            Ok(CompiledAcceptedRuleOperation::NumericRange { min, max })
        }
        AcceptedRuleOperation::MultipleOf { divisor } => {
            if divisor.kind() != &resolved_kind {
                return Err(AcceptedTargetedRuleEvaluationError::InvalidTarget);
            }
            let divisor = decode_literal(divisor, value_catalog)
                .map_err(|_| AcceptedTargetedRuleEvaluationError::LiteralCorrupt)?;
            if !matches!(super::compile::exact_numeric_is_zero(&divisor), Some(false)) {
                return Err(AcceptedTargetedRuleEvaluationError::LiteralCorrupt);
            }
            Ok(CompiledAcceptedRuleOperation::MultipleOf { divisor })
        }
    }
}

fn compile_record_orders(
    composite_catalog: &AcceptedCompositeCatalog,
) -> BTreeMap<CompositeTypeId, Vec<usize>> {
    composite_catalog
        .id_by_path()
        .values()
        .filter_map(|type_id| {
            let definition = composite_catalog.composite_type(*type_id)?;
            let AcceptedCompositeShape::Record(fields) = definition.shape() else {
                return None;
            };
            let mut order = (0..fields.len()).collect::<Vec<_>>();
            order.sort_unstable_by_key(|index| fields[*index].id());
            Some((*type_id, order))
        })
        .collect()
}

const fn named_type_identity(kind: &AcceptedFieldKind) -> Option<AcceptedNamedTypeIdentity> {
    match kind {
        AcceptedFieldKind::Enum { type_id } => Some(AcceptedNamedTypeIdentity::Enum(*type_id)),
        AcceptedFieldKind::Composite { type_id } => {
            Some(AcceptedNamedTypeIdentity::Composite(*type_id))
        }
        _ => None,
    }
}

fn validate_record_shape(
    fields: &[AcceptedCompositeField],
    entries: &[(Value, Value)],
) -> Result<(), AcceptedTargetedRuleEvaluationError> {
    if fields.len() != entries.len() {
        return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
    }
    for (field, (key, _)) in fields.iter().zip(entries) {
        let Value::Text(name) = key else {
            return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
        };
        if name != field.name() {
            return Err(AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch);
        }
    }
    Ok(())
}

impl From<AcceptedTargetedRuleEvaluationError> for super::AcceptedRowConstraintEvaluationError {
    fn from(error: AcceptedTargetedRuleEvaluationError) -> Self {
        match error {
            AcceptedTargetedRuleEvaluationError::InvalidTarget => {
                Self::InvalidExpression(AcceptedCheckExprV1Error::UnsupportedFieldKind)
            }
            AcceptedTargetedRuleEvaluationError::LiteralCorrupt => Self::LiteralCorrupt,
            AcceptedTargetedRuleEvaluationError::MissingSlot => Self::MissingSlot,
            AcceptedTargetedRuleEvaluationError::RuntimeValueMismatch => Self::RuntimeValueMismatch,
            AcceptedTargetedRuleEvaluationError::ValueDepthExceeded => Self::ValueDepthExceeded,
            AcceptedTargetedRuleEvaluationError::ValueNodeBudgetExceeded => {
                Self::ValueNodeBudgetExceeded
            }
            AcceptedTargetedRuleEvaluationError::OperationBudgetExceeded => {
                Self::OperationBudgetExceeded
            }
            AcceptedTargetedRuleEvaluationError::PathBudgetExceeded => Self::PathBudgetExceeded,
        }
    }
}
