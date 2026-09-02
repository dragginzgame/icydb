//! Module: query::plan::group_field
//! Responsibility: canonical direct and scalar record-path grouping-key identity.
//! Does not own: grouped execution strategy or runtime row traversal.
//! Boundary: resolves authored group fields once through accepted schema authority.

use std::{mem, slice};

use crate::db::{
    query::plan::{
        FieldSlot,
        expr::{Expr, FieldId, FieldPath, PathSpec},
    },
    schema::{AcceptedFieldKind, SchemaInfo, classify_accepted_field_kind},
};

/// One group key source used only after a query contains an accepted scalar path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupField {
    Direct(FieldSlot),
    ScalarPath(ScalarGroupPath),
}

/// One accepted scalar record path compiled to its root row slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarGroupPath {
    label: String,
    path: PathSpec,
    root_slot: usize,
    identity_group_canonical_form: bool,
}

impl ScalarGroupPath {
    /// Borrow the normalized diagnostic label retained for public surfaces.
    #[must_use]
    pub(in crate::db) const fn label(&self) -> &str {
        self.label.as_str()
    }

    /// Borrow the structural path identity.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &PathSpec {
        &self.path
    }

    /// Return the accepted root row slot used by grouped runtime traversal.
    #[must_use]
    pub(in crate::db) const fn root_slot(&self) -> usize {
        self.root_slot
    }

    /// Return whether the persisted scalar leaf is already group-canonical.
    #[must_use]
    pub(in crate::db) const fn has_identity_group_canonical_form(&self) -> bool {
        self.identity_group_canonical_form
    }
}

impl GroupField {
    #[cfg(test)]
    pub(in crate::db) fn scalar_path_for_test(
        label: impl Into<String>,
        root: impl Into<FieldId>,
        segments: Vec<String>,
        root_slot: usize,
        accepted_kind: AcceptedFieldKind,
    ) -> Self {
        let identity_group_canonical_form =
            classify_accepted_field_kind(&accepted_kind).has_identity_group_canonical_form();
        Self::ScalarPath(ScalarGroupPath {
            label: label.into(),
            path: PathSpec::new(root, segments),
            root_slot,
            identity_group_canonical_form,
        })
    }

    /// Resolve one normalized direct field or scalar record path.
    #[must_use]
    pub(in crate::db) fn resolve_with_schema(schema: &SchemaInfo, field: &str) -> Option<Self> {
        let Some((root, nested)) = field.split_once('.') else {
            return FieldSlot::resolve_with_schema(schema, field).map(Self::Direct);
        };
        if root.is_empty() || nested.is_empty() {
            return None;
        }
        let segments = nested.split('.').map(str::to_string).collect::<Vec<_>>();
        if segments.iter().any(String::is_empty) {
            return None;
        }

        let root_slot = schema.field_slot_index(root)?;
        let accepted_kind = schema.accepted_nested_query_field_kind(root, segments.as_slice())?;
        let semantics = classify_accepted_field_kind(accepted_kind);
        if !semantics.is_scalar()
            || !semantics.is_sql_comparable()
            || !semantics.supports_stable_group_key()
        {
            return None;
        }
        Some(Self::ScalarPath(ScalarGroupPath {
            label: field.to_string(),
            path: PathSpec::new(root, segments),
            root_slot,
            identity_group_canonical_form: semantics.has_identity_group_canonical_form(),
        }))
    }

    /// Borrow the normalized field/path label.
    #[must_use]
    pub(in crate::db) fn field(&self) -> &str {
        match self {
            Self::Direct(field) => field.field(),
            Self::ScalarPath(path) => path.label(),
        }
    }

    /// Return the root row slot required by grouped execution.
    #[must_use]
    pub(in crate::db) const fn root_slot(&self) -> usize {
        match self {
            Self::Direct(field) => field.index(),
            Self::ScalarPath(path) => path.root_slot(),
        }
    }

    /// Borrow the accepted terminal kind from the accepted schema authority.
    #[must_use]
    pub(in crate::db) fn accepted_kind_from_schema<'a>(
        &'a self,
        schema: &'a SchemaInfo,
    ) -> Option<&'a AcceptedFieldKind> {
        match self {
            Self::Direct(field) => field.accepted_kind(),
            Self::ScalarPath(path) => schema.accepted_nested_query_field_kind(
                path.path().root().as_str(),
                path.path().segments(),
            ),
        }
    }

    /// Return whether this source carries accepted grouping authority.
    #[must_use]
    pub(in crate::db) const fn is_resolved(&self) -> bool {
        match self {
            Self::Direct(field) => !field.is_unresolved(),
            Self::ScalarPath(_) => true,
        }
    }

    /// Return whether the persisted value is already group-canonical.
    #[must_use]
    pub(in crate::db) fn has_identity_group_canonical_form(&self) -> bool {
        match self {
            Self::Direct(field) => field.accepted_kind().is_some_and(|kind| {
                classify_accepted_field_kind(kind).has_identity_group_canonical_form()
            }),
            Self::ScalarPath(path) => path.has_identity_group_canonical_form(),
        }
    }

    /// Borrow the direct slot when this source stays on the direct fast path.
    #[must_use]
    pub(in crate::db) const fn as_direct(&self) -> Option<&FieldSlot> {
        match self {
            Self::Direct(field) => Some(field),
            Self::ScalarPath(_) => None,
        }
    }

    /// Borrow the scalar path when this source requires path-aware execution.
    #[must_use]
    pub(in crate::db) const fn as_scalar_path(&self) -> Option<&ScalarGroupPath> {
        match self {
            Self::Direct(_) => None,
            Self::ScalarPath(path) => Some(path),
        }
    }

    /// Build the canonical planner expression for grouped projection identity.
    #[must_use]
    pub(in crate::db) fn projection_expr(&self) -> Expr {
        match self {
            Self::Direct(field) => Expr::Field(FieldId::new(field.field())),
            Self::ScalarPath(path) => Expr::FieldPath(FieldPath::new(
                path.path().root().as_str(),
                path.path().segments().to_vec(),
            )),
        }
    }

    /// Return whether one expression leaf has this structural group identity.
    #[must_use]
    pub(in crate::db) fn matches_expr(&self, expr: &Expr) -> bool {
        match (self, expr) {
            (Self::Direct(field), Expr::Field(candidate)) => field.field() == candidate.as_str(),
            (Self::ScalarPath(path), Expr::FieldPath(candidate)) => {
                path.path() == candidate.path_spec()
            }
            _ => false,
        }
    }

    fn same_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Direct(left), Self::Direct(right)) => left.index() == right.index(),
            (Self::ScalarPath(left), Self::ScalarPath(right)) => left.path() == right.path(),
            _ => false,
        }
    }
}

/// Direct-preserving closed set of declared grouping keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupFieldSet {
    Direct(Vec<FieldSlot>),
    PathAware(Vec<GroupField>),
}

impl GroupFieldSet {
    /// Build the empty direct representation used before any grouping key is added.
    #[must_use]
    pub(in crate::db) const fn empty() -> Self {
        Self::Direct(Vec::new())
    }

    /// Return the declared key count.
    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        match self {
            Self::Direct(fields) => fields.len(),
            Self::PathAware(fields) => fields.len(),
        }
    }

    /// Return whether no grouping key is declared.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the exact direct-only representation when no path was admitted.
    #[must_use]
    pub(in crate::db) const fn as_direct(&self) -> Option<&[FieldSlot]> {
        match self {
            Self::Direct(fields) => Some(fields.as_slice()),
            Self::PathAware(_) => None,
        }
    }

    /// Borrow the path-aware representation selected once per grouped query.
    #[must_use]
    pub(in crate::db) const fn as_path_aware(&self) -> Option<&[GroupField]> {
        match self {
            Self::Direct(_) => None,
            Self::PathAware(fields) => Some(fields.as_slice()),
        }
    }

    /// Iterate all grouping keys without constructing a parallel field list.
    pub(in crate::db) fn iter(&self) -> GroupFieldIter<'_> {
        match self {
            Self::Direct(fields) => GroupFieldIter::Direct(fields.iter()),
            Self::PathAware(fields) => GroupFieldIter::PathAware(fields.iter()),
        }
    }

    /// Borrow one declared grouping key by declaration offset.
    #[must_use]
    pub(in crate::db) fn get(&self, index: usize) -> Option<GroupFieldRef<'_>> {
        match self {
            Self::Direct(fields) => fields.get(index).map(GroupFieldRef::Direct),
            Self::PathAware(fields) => fields.get(index).map(GroupFieldRef::PathAware),
        }
    }

    /// Append one key, promoting to the path-aware representation only when needed.
    pub(in crate::db) fn push(&mut self, field: GroupField) {
        match self {
            Self::Direct(fields) => match field {
                GroupField::Direct(direct) => {
                    if !fields
                        .iter()
                        .any(|existing| existing.index() == direct.index())
                    {
                        fields.push(direct);
                    }
                }
                path @ GroupField::ScalarPath(_) => {
                    let mut promoted = mem::take(fields)
                        .into_iter()
                        .map(GroupField::Direct)
                        .collect::<Vec<_>>();
                    promoted.push(path);
                    *self = Self::PathAware(promoted);
                }
            },
            Self::PathAware(fields) => {
                if !fields.iter().any(|existing| existing.same_identity(&field)) {
                    fields.push(field);
                }
            }
        }
    }

    /// Return whether one expression leaf is a declared group key.
    #[must_use]
    pub(in crate::db) fn contains_expr(&self, expr: &Expr) -> bool {
        self.iter().any(|field| field.matches_expr(expr))
    }

    /// Return whether every field/path leaf in one expression is a declared key.
    #[must_use]
    pub(in crate::db) fn contains_all_expr_references(&self, expr: &Expr) -> bool {
        expr.all_tree_expr(&mut |node| match node {
            Expr::Field(_) | Expr::FieldPath(_) => self.contains_expr(node),
            Expr::Aggregate(_)
            | Expr::Literal(_)
            | Expr::FunctionCall { .. }
            | Expr::Unary { .. }
            | Expr::Binary { .. }
            | Expr::Case { .. } => true,
            #[cfg(test)]
            Expr::Alias { .. } => true,
        })
    }

    /// Rebind every authored label through the selected accepted schema snapshot.
    #[must_use]
    pub(in crate::db) fn resolve_with_schema(&self, schema: &SchemaInfo) -> Option<Self> {
        let mut resolved = Self::default();
        for field in self.iter() {
            resolved.push(GroupField::resolve_with_schema(schema, field.field())?);
        }
        Some(resolved)
    }
}

impl Default for GroupFieldSet {
    fn default() -> Self {
        Self::Direct(Vec::new())
    }
}

/// Borrowed grouping-key view shared by semantic-only consumers.
#[derive(Clone, Copy)]
pub(in crate::db) enum GroupFieldRef<'a> {
    Direct(&'a FieldSlot),
    PathAware(&'a GroupField),
}

impl<'a> GroupFieldRef<'a> {
    /// Borrow the normalized field/path label.
    #[must_use]
    pub(in crate::db) fn field(&self) -> &'a str {
        match self {
            Self::Direct(field) => field.field(),
            Self::PathAware(field) => field.field(),
        }
    }

    /// Return the root row slot required by execution.
    #[must_use]
    pub(in crate::db) const fn root_slot(&self) -> usize {
        match self {
            Self::Direct(field) => field.index(),
            Self::PathAware(field) => field.root_slot(),
        }
    }

    /// Borrow the direct slot when this source stays on the direct fast path.
    #[must_use]
    pub(in crate::db) const fn as_direct(&self) -> Option<&'a FieldSlot> {
        match self {
            Self::Direct(field) => Some(field),
            Self::PathAware(field) => field.as_direct(),
        }
    }

    /// Borrow the scalar path when this source requires path-aware execution.
    #[must_use]
    pub(in crate::db) const fn as_scalar_path(&self) -> Option<&'a ScalarGroupPath> {
        match self {
            Self::Direct(_) => None,
            Self::PathAware(field) => field.as_scalar_path(),
        }
    }

    /// Return whether this source carries accepted grouping authority.
    #[must_use]
    pub(in crate::db) const fn is_resolved(&self) -> bool {
        match self {
            Self::Direct(field) => !field.is_unresolved(),
            Self::PathAware(field) => field.is_resolved(),
        }
    }

    /// Return whether this source matches one planner expression leaf.
    #[must_use]
    pub(in crate::db) fn matches_expr(&self, expr: &Expr) -> bool {
        match self {
            Self::Direct(field) => {
                matches!(expr, Expr::Field(candidate) if candidate.as_str() == field.field())
            }
            Self::PathAware(field) => field.matches_expr(expr),
        }
    }

    /// Return whether this borrowed source and an owned source share identity.
    #[must_use]
    pub(in crate::db) fn same_identity(&self, other: &GroupField) -> bool {
        match self {
            Self::Direct(field) => other
                .as_direct()
                .is_some_and(|other| field.index() == other.index()),
            Self::PathAware(field) => field.same_identity(other),
        }
    }

    /// Build the canonical projection expression for this key.
    #[must_use]
    pub(in crate::db) fn projection_expr(&self) -> Expr {
        match self {
            Self::Direct(field) => Expr::Field(FieldId::new(field.field())),
            Self::PathAware(field) => field.projection_expr(),
        }
    }
}

/// Concrete iterator over direct or path-aware key storage.
pub(in crate::db) enum GroupFieldIter<'a> {
    Direct(slice::Iter<'a, FieldSlot>),
    PathAware(slice::Iter<'a, GroupField>),
}

impl<'a> Iterator for GroupFieldIter<'a> {
    type Item = GroupFieldRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Direct(fields) => fields.next().map(GroupFieldRef::Direct),
            Self::PathAware(fields) => fields.next().map(GroupFieldRef::PathAware),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Direct(fields) => fields.size_hint(),
            Self::PathAware(fields) => fields.size_hint(),
        }
    }
}

impl ExactSizeIterator for GroupFieldIter<'_> {}

#[cfg(test)]
mod tests {
    use crate::db::{
        query::plan::{FieldSlot, GroupField, GroupFieldSet},
        schema::AcceptedFieldKind,
    };

    fn rank_path() -> GroupField {
        GroupField::scalar_path_for_test(
            "profile.rank",
            "profile",
            vec!["rank".to_string()],
            1,
            AcceptedFieldKind::Int32,
        )
    }

    #[test]
    fn direct_keys_keep_the_direct_representation() {
        let mut fields = GroupFieldSet::empty();
        fields.push(GroupField::Direct(FieldSlot::from_test_accepted_kind(
            0,
            "direct_rank",
            AcceptedFieldKind::Int32,
        )));

        assert_eq!(fields.as_direct().map(<[FieldSlot]>::len), Some(1));
        assert!(fields.as_path_aware().is_none());
    }

    #[test]
    fn first_path_promotes_the_whole_tuple_once_and_preserves_order() {
        let mut fields = GroupFieldSet::empty();
        fields.push(GroupField::Direct(FieldSlot::from_test_accepted_kind(
            0,
            "direct_rank",
            AcceptedFieldKind::Int32,
        )));
        fields.push(rank_path());
        fields.push(rank_path());

        assert!(fields.as_direct().is_none());
        assert_eq!(
            fields.iter().map(|field| field.field()).collect::<Vec<_>>(),
            vec!["direct_rank", "profile.rank"],
        );
    }
}
