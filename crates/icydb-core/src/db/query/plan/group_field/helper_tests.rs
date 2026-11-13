//! Test-only constructors and adapters for the parent module.

use super::*;

impl GroupField {
    pub(in crate::db) fn scalar_path_for_test(
        label: impl Into<String>,
        root: impl Into<FieldId>,
        segments: Vec<String>,
        root_slot: usize,
        accepted_kind: &AcceptedFieldKind,
    ) -> Self {
        let identity_group_canonical_form =
            classify_accepted_field_kind(accepted_kind).has_identity_group_canonical_form();
        Self::ScalarPath(ScalarGroupPath {
            label: label.into(),
            path: PathSpec::new(root, segments),
            root_slot,
            identity_group_canonical_form,
        })
    }
}
