//! Module: db::executor::projection::materialize::plan
//! Responsibility: prepared projection materialization contracts.
//! Does not own: row loops, structural page dispatch, or DISTINCT execution.
//! Boundary: stores planner-derived projection contract for executor-owned consumers.

use crate::db::executor::projection::materialize::contracts::ProjectionSpec;
use crate::{
    db::{
        executor::projection::materialize::contracts::{AccessPlannedQuery, CompiledExpr},
        executor::terminal::RowLayout,
    },
    error::InternalError,
};

#[derive(Debug)]
pub(in crate::db) struct PreparedDirectProjectionSlots {
    projections: Vec<PreparedDirectProjectionSlot>,
    has_repeated_source: bool,
}

#[derive(Debug)]
pub(in crate::db) struct PreparedDirectProjectionSlot {
    source_slot: usize,
    previous_projection_index: Option<usize>,
}

impl PreparedDirectProjectionSlots {
    /// Build source-slot metadata, retaining prior output indices for repeats.
    #[must_use]
    pub(super) fn from_slots(slots: &[usize]) -> Self {
        let mut projections: Vec<PreparedDirectProjectionSlot> = Vec::with_capacity(slots.len());
        let mut has_repeated_source = false;

        for &source_slot in slots {
            let previous_projection_index = projections
                .iter()
                .position(|projection| projection.source_slot == source_slot);
            has_repeated_source |= previous_projection_index.is_some();
            projections.push(PreparedDirectProjectionSlot {
                source_slot,
                previous_projection_index,
            });
        }

        Self {
            projections,
            has_repeated_source,
        }
    }

    #[must_use]
    pub(in crate::db) const fn projections(&self) -> &[PreparedDirectProjectionSlot] {
        self.projections.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.projections.len()
    }

    #[must_use]
    pub(in crate::db) const fn has_repeated_source(&self) -> bool {
        self.has_repeated_source
    }
}

impl PreparedDirectProjectionSlot {
    #[must_use]
    pub(in crate::db) const fn source_slot(&self) -> usize {
        self.source_slot
    }

    #[must_use]
    pub(in crate::db) const fn previous_projection_index(&self) -> Option<usize> {
        self.previous_projection_index
    }
}

///
/// PreparedProjectionContract
///
/// PreparedProjectionContract is the executor-owned prepared projection contract
/// consumed by scalar output shaping.
/// It retains compiled expressions and derived direct-slot layouts, while the
/// planner remains the owner of projection syntax.
///
#[derive(Debug)]
pub(in crate::db) struct PreparedProjectionContract {
    compiled_exprs: Vec<CompiledExpr>,
    projection_is_model_identity: bool,
    retained_slot_direct_projection_slots: Option<PreparedDirectProjectionSlots>,
    retained_slot_direct_octet_length_projection_slots: Vec<Option<usize>>,
    data_row_direct_projection_slots: Option<PreparedDirectProjectionSlots>,
}

impl PreparedProjectionContract {
    #[must_use]
    pub(in crate::db) const fn compiled_exprs(&self) -> &[CompiledExpr] {
        self.compiled_exprs.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) fn scalar_projection_contains_field_path(&self) -> bool {
        self.compiled_exprs()
            .iter()
            .any(CompiledExpr::contains_field_path)
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_is_model_identity(&self) -> bool {
        self.projection_is_model_identity
    }

    #[must_use]
    pub(in crate::db) const fn retained_slot_direct_projection_slots(
        &self,
    ) -> Option<&PreparedDirectProjectionSlots> {
        self.retained_slot_direct_projection_slots.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn retained_slot_direct_octet_length_projection_slots(
        &self,
    ) -> &[Option<usize>] {
        self.retained_slot_direct_octet_length_projection_slots
            .as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn data_row_direct_projection_slots(
        &self,
    ) -> Option<&PreparedDirectProjectionSlots> {
        self.data_row_direct_projection_slots.as_ref()
    }
}

/// Build one executor-owned prepared projection contract from planner-frozen metadata.
pub(in crate::db) fn prepare_projection_contract_from_plan(
    row_layout: &RowLayout,
    plan: &AccessPlannedQuery,
) -> Result<PreparedProjectionContract, InternalError> {
    let projection = plan.frozen_projection_spec()?;
    let compiled_projection = plan
        .scalar_projection_plan()
        .ok_or_else(InternalError::query_executor_invariant)?
        .to_vec();
    let retained_slot_direct_projection_slots =
        direct_projection_slots_from_projection(projection, plan.frozen_direct_projection_slots());
    let retained_slot_direct_octet_length_projection_slots =
        retained_slot_direct_octet_length_projection_slots_from_compiled(
            row_layout,
            &compiled_projection,
        );
    let data_row_direct_projection_slots = direct_projection_slots_from_projection(
        projection,
        plan.frozen_data_row_direct_projection_slots(),
    );
    Ok(PreparedProjectionContract {
        compiled_exprs: compiled_projection,
        projection_is_model_identity: plan.projection_is_model_identity()?,
        retained_slot_direct_projection_slots,
        retained_slot_direct_octet_length_projection_slots,
        data_row_direct_projection_slots,
    })
}

// Reuse planner-frozen slots only when every projection is a direct field.
fn direct_projection_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<PreparedDirectProjectionSlots> {
    let direct_projection_slots = direct_projection_slots?;
    let slots = &direct_projection_slots[..projection.len().min(direct_projection_slots.len())];
    for field in projection.fields().take(slots.len()) {
        field.direct_field_name()?;
    }

    Some(PreparedDirectProjectionSlots::from_slots(slots))
}

fn retained_slot_direct_octet_length_projection_slots_from_compiled(
    row_layout: &RowLayout,
    compiled_projection: &[CompiledExpr],
) -> Vec<Option<usize>> {
    let mut slots = Vec::new();

    for (index, expr) in compiled_projection.iter().enumerate() {
        let slot = expr.direct_octet_length_slot().and_then(|(slot, _field)| {
            row_layout
                .slot_uses_scalar_byte_length_codec(slot)
                .then_some(slot)
        });
        if slots.is_empty() {
            if slot.is_none() {
                continue;
            }
            // Keep the no-override case allocation-free; mixed projections
            // still need one entry per expression, including the earlier ones.
            slots.reserve(compiled_projection.len());
            slots.resize(index, None);
        }
        slots.push(slot);
    }

    slots
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(PreparedDirectProjectionSlot {
Self{source_slot,previous_projection_index} => [source_slot,previous_projection_index],
});
crate::retained::retained_fields!(PreparedDirectProjectionSlots {
Self{projections,has_repeated_source} => [projections,has_repeated_source],
});
crate::retained::retained_fields!(PreparedProjectionContract {
Self{compiled_exprs,projection_is_model_identity,retained_slot_direct_projection_slots,retained_slot_direct_octet_length_projection_slots,data_row_direct_projection_slots} => [compiled_exprs,projection_is_model_identity,retained_slot_direct_projection_slots,retained_slot_direct_octet_length_projection_slots,data_row_direct_projection_slots],
});

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{ProjectionSpec, direct_projection_slots_from_projection};
    use crate::{
        db::query::plan::expr::{Expr, FieldId, ProjectionField},
        value::Value,
    };

    #[test]
    fn direct_slot_metadata_preserves_order_repeats_and_expression_rejection() {
        let projection = ProjectionSpec::from_fields_for_test(
            ["payload", "id", "payload"]
                .into_iter()
                .map(|name| ProjectionField::Scalar {
                    expr: Expr::Field(FieldId::new(name)),
                    alias: None,
                })
                .collect(),
        );
        let prepared =
            direct_projection_slots_from_projection(&projection, Some(&[7, 2, 7])).unwrap();
        assert!(prepared.has_repeated_source());
        assert_eq!(
            prepared
                .projections()
                .iter()
                .map(|slot| (slot.source_slot(), slot.previous_projection_index()))
                .collect::<Vec<_>>(),
            vec![(7, None), (2, None), (7, Some(0))]
        );
        assert!(direct_projection_slots_from_projection(&projection, None).is_none());

        let expression = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Literal(Value::Nat64(1)),
            alias: None,
        }]);
        assert!(direct_projection_slots_from_projection(&expression, Some(&[0])).is_none());
        let empty =
            direct_projection_slots_from_projection(&ProjectionSpec::default(), Some(&[])).unwrap();
        assert_eq!(empty.len(), 0);
        assert!(!empty.has_repeated_source());
    }
}
