use crate::{
    db::query::{
        QueryMode,
        plan::{
            ContinuationSignature, CursorBoundary, ExplainPlan, LogicalPlan, PlanError,
            PlanFingerprint, continuation::decode_validated_cursor_boundary,
        },
        predicate::SchemaInfo,
    },
    traits::{EntityKind, FieldValue},
};
use std::marker::PhantomData;

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub struct ExecutablePlan<E: EntityKind> {
    plan: LogicalPlan<E::Key>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(crate) const fn new(plan: LogicalPlan<E::Key>) -> Self {
        Self {
            plan,
            _marker: PhantomData,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain()
    }

    /// Compute a stable fingerprint for this plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        self.plan.fingerprint()
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    pub fn continuation_signature(&self) -> ContinuationSignature {
        self.plan.continuation_signature(E::PATH)
    }

    /// Validate and decode a continuation cursor against this canonical plan.
    ///
    /// This is a planning-boundary validation step. Executors receive only a
    /// typed boundary and must not parse or validate cursor bytes.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn plan_cursor_boundary(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<Option<CursorBoundary>, PlanError>
    where
        E::Key: FieldValue,
    {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let Some(order) = self.plan.order.as_ref() else {
            return Err(PlanError::CursorRequiresOrder);
        };
        if order.fields.is_empty() {
            return Err(PlanError::CursorRequiresOrder);
        }

        let boundary = decode_validated_cursor_boundary(
            cursor,
            E::PATH,
            E::MODEL,
            order,
            self.continuation_signature(),
        )?;

        // Typed key decode is the final authority for PK cursor slots.
        let pk_field = E::MODEL.primary_key.name;
        let pk_index = order
            .fields
            .iter()
            .position(|(field, _)| field == pk_field)
            .ok_or_else(|| PlanError::MissingPrimaryKeyTieBreak {
                field: pk_field.to_string(),
            })?;
        let expected = SchemaInfo::from_entity_model(E::MODEL)
            .map_err(PlanError::PredicateInvalid)?
            .field(pk_field)
            .expect("primary key exists by model contract")
            .to_string();
        let pk_slot = &boundary.slots[pk_index];
        let invalid_pk = match pk_slot {
            super::CursorBoundarySlot::Missing => Some(None),
            super::CursorBoundarySlot::Present(value) => {
                if E::Key::from_value(value).is_none() {
                    Some(Some(value.clone()))
                } else {
                    None
                }
            }
        };
        if let Some(value) = invalid_pk {
            return Err(PlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                field: pk_field.to_string(),
                expected,
                value,
            });
        }

        Ok(Some(boundary))
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.plan.mode
    }

    pub(crate) const fn access(&self) -> &crate::db::query::plan::AccessPlan<E::Key> {
        &self.plan.access
    }

    pub(crate) fn into_inner(self) -> LogicalPlan<E::Key> {
        self.plan
    }
}
