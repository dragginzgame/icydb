//! Module: executor::aggregate::runtime::grouped_fold::dispatch
//! Responsibility: grouped fold route and count-key path selection.
//! Boundary: resolves route-owned branching before execution loops run.

use crate::db::executor::{
    aggregate::{
        EffectiveRuntimeFilterProgram, FieldSlot,
        capability::{
            accepted_field_kind_has_identity_group_canonical_form,
            accepted_field_kind_supports_group_probe,
        },
    },
    pipeline::contracts::GroupedRouteStage,
};

///
/// GroupedCountKeyPath
///
/// GroupedCountKeyPath freezes how the dedicated grouped `COUNT(*)` fold path
/// should recover grouped keys from source rows.
/// It keeps the direct single-field identity path and the row-view fallback
/// path under one route-owned owner instead of carrying those decisions as
/// separate ad hoc variables in the fold loop.
///

pub(super) enum GroupedCountKeyPath {
    DirectSingleField { group_field_index: usize },
    RowView { probe_kind: GroupedCountProbeKind },
}

impl GroupedCountKeyPath {
    // Resolve the grouped-count key recovery path once from grouped route
    // shape plus the optional compiled residual predicate.
    pub(super) fn for_route(
        route: &GroupedRouteStage,
        effective_runtime_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    ) -> Self {
        if effective_runtime_filter_program.is_none()
            && let [field] = route.group_fields()
            && field
                .accepted_kind()
                .is_some_and(accepted_field_kind_has_identity_group_canonical_form)
        {
            return Self::DirectSingleField {
                group_field_index: field.index(),
            };
        }

        Self::RowView {
            probe_kind: GroupedCountProbeKind::for_group_fields(route.group_fields()),
        }
    }
}

///
/// GroupedCountProbeKind
///
/// GroupedCountProbeKind records whether grouped-count row-view ingestion can
/// use borrowed row-slot probes or must materialize owned canonical keys.
/// The count executor matches this once before the source-row loop so the
/// per-row path keeps the previous fast-path shape without dynamic dispatch.
///

#[derive(Clone, Copy)]
pub(super) enum GroupedCountProbeKind {
    Borrowed,
    Owned,
}

impl GroupedCountProbeKind {
    // Resolve grouped-count row-view probe mode from the planner-frozen
    // grouped field metadata.
    fn for_group_fields(group_fields: &[FieldSlot]) -> Self {
        if group_fields_support_borrowed_group_probe(group_fields) {
            Self::Borrowed
        } else {
            Self::Owned
        }
    }
}

// Return true when every planner-frozen accepted slot kind supports the
// borrowed grouped-key probe path for this grouped route.
pub(super) fn group_fields_support_borrowed_group_probe(group_fields: &[FieldSlot]) -> bool {
    group_fields.iter().all(|field| {
        field
            .accepted_kind()
            .is_some_and(accepted_field_kind_supports_group_probe)
    })
}

#[cfg(test)]
mod tests {
    use super::group_fields_support_borrowed_group_probe;
    use crate::{
        db::{query::plan::FieldSlot, schema::AcceptedFieldKind},
        model::field::FieldKind,
    };

    #[test]
    fn borrowed_group_probe_requires_accepted_scalar_authority() {
        let accepted = FieldSlot::from_test_accepted_kind(
            0,
            "status",
            AcceptedFieldKind::Text { max_len: None },
        );
        let model_only = FieldSlot::from_model_kind(0, "status", FieldKind::Text { max_len: None });
        let accepted_enum = FieldSlot::from_test_accepted_kind(
            0,
            "status",
            AcceptedFieldKind::Enum {
                type_id: crate::value::EnumTypeId::new(1).expect("non-zero type ID"),
            },
        );

        assert!(group_fields_support_borrowed_group_probe(&[accepted]));
        assert!(!group_fields_support_borrowed_group_probe(&[model_only]));
        assert!(!group_fields_support_borrowed_group_probe(&[accepted_enum]));
    }
}
