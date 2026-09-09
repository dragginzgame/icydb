//! Module: query::plan::group_field
//! Responsibility: canonical direct and scalar record-path grouping-key identity.
//! Does not own: grouped execution strategy or runtime row traversal.
//! Boundary: resolves authored group fields once through accepted schema authority.

use std::{mem, slice};

use crate::db::{
    QueryError,
    query::plan::{
        FieldSlot,
        expr::{Expr, FieldId, FieldPath, PathSpec},
    },
    query::preparation::PreparationWork,
    schema::{AcceptedFieldKind, SchemaInfo, classify_accepted_field_kind},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

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
    pub(in crate::db) fn resolve_with_schema(
        schema: &SchemaInfo,
        field: &str,
        work: &PreparationWork<'_>,
    ) -> Result<Option<Self>, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1 + field.len() as u64)?;
        let Some((root, nested)) = field.split_once('.') else {
            return Ok(FieldSlot::resolve_with_schema(schema, field).map(Self::Direct));
        };
        let Some(accepted_kind) = Self::accepted_kind_for_label(schema, field) else {
            return Ok(None);
        };
        let Some(root_slot) = schema.field_slot_index(root) else {
            return Ok(None);
        };
        let semantics = classify_accepted_field_kind(accepted_kind);
        // Admit destination backing and each owned string before copying.
        let mut segments = work.vec_with_capacity(nested.split('.').count())?;
        for segment in nested.split('.') {
            segments.push(work.copy_text(segment)?);
        }
        Ok(Some(Self::ScalarPath(ScalarGroupPath {
            label: work.copy_text(field)?,
            path: PathSpec::new(FieldId::new(work.copy_text(root)?), segments),
            root_slot,
            identity_group_canonical_form: semantics.has_identity_group_canonical_form(),
        })))
    }

    /// Check grouping eligibility without retaining a key for validation-only consumers.
    #[must_use]
    pub(in crate::db) fn accepted_kind_for_label<'a>(
        schema: &'a SchemaInfo,
        field: &str,
    ) -> Option<&'a AcceptedFieldKind> {
        Self::accepted_kind_for_components(schema, field.split('.'))
    }

    // Borrow terminal authority without constructing an execution key. Keep
    // nested grouping eligibility shared with retained-key resolution; direct
    // fields deliberately retain their existing, broader resolution contract.
    fn accepted_kind_for_components<'schema, 'path>(
        schema: &'schema SchemaInfo,
        mut components: impl Iterator<Item = &'path str> + Clone,
    ) -> Option<&'schema AcceptedFieldKind> {
        let root = components.next()?;
        if components.clone().next().is_none() {
            return schema.accepted_query_field_kind(root);
        }
        if root.is_empty() || components.clone().any(str::is_empty) {
            return None;
        }
        let accepted_kind = schema.accepted_nested_query_field_kind(root, components)?;
        let semantics = classify_accepted_field_kind(accepted_kind);
        if !semantics.is_scalar()
            || !semantics.is_sql_comparable()
            || !semantics.supports_stable_group_key()
        {
            return None;
        }
        Some(accepted_kind)
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

    /// Borrow a field expression's current accepted grouping kind without
    /// retaining a key, label or schema-kind copy.
    #[must_use]
    pub(in crate::db) fn accepted_kind_for_expr<'a>(
        schema: &'a SchemaInfo,
        expr: &Expr,
    ) -> Option<&'a AcceptedFieldKind> {
        match expr {
            Expr::Field(field) => {
                Self::accepted_kind_for_components(schema, field.as_str().split('.'))
            }
            Expr::FieldPath(path) => {
                let path = path.path_spec();
                // Preserve dotted-name interpretation without rendering and
                // reparsing a temporary label (including empty components).
                let components = std::iter::once(path.root().as_str())
                    .chain(path.segments().iter().map(String::as_str))
                    .flat_map(|component| component.split('.'));
                Self::accepted_kind_for_components(schema, components)
            }
            _ => None,
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
    pub(in crate::db) fn push(
        &mut self,
        field: GroupField,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        for existing in self.iter() {
            // Direct identities compare slots only. Path identities may inspect
            // all component bytes; reserve that work before the shared comparator.
            let bytes = if existing.as_scalar_path().is_some() && field.as_scalar_path().is_some() {
                existing.field().len().saturating_add(field.field().len()) as u64
            } else {
                0
            };
            work.charge(Resource::PredicateExpressionSteps, 1 + bytes)?;
            if existing.same_identity(GroupFieldRef::PathAware(&field)) {
                return Ok(());
            }
        }
        match self {
            Self::Direct(fields) => match field {
                GroupField::Direct(direct) => {
                    work.reserve_vec(fields, 1)?;
                    fields.push(direct);
                }
                path @ GroupField::ScalarPath(_) => {
                    work.charge(Resource::PredicateExpressionSteps, fields.len() as u64)?;
                    let mut promoted = work.vec_with_capacity(fields.len() + 1)?;
                    promoted.extend(mem::take(fields).into_iter().map(GroupField::Direct));
                    promoted.push(path);
                    *self = Self::PathAware(promoted);
                }
            },
            Self::PathAware(fields) => {
                work.reserve_vec(fields, 1)?;
                fields.push(field);
            }
        }
        Ok(())
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
    pub(in crate::db) fn resolve_with_schema(
        &self,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<Option<Self>, QueryError> {
        let mut resolved = Self::default();
        for field in self.iter() {
            let Some(key) = GroupField::resolve_with_schema(schema, field.field(), work)? else {
                return Ok(None);
            };
            resolved.push(key, work)?;
        }
        Ok(Some(resolved))
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

    /// Check current accepted key eligibility and identity without constructing
    /// a replacement key. This does not rebind retained type metadata or grant
    /// execution authority; the planner's existing rebinding still owns that.
    #[must_use]
    pub(in crate::db) fn matches_schema_identity(&self, schema: &SchemaInfo) -> bool {
        if let Some(field) = self.as_direct() {
            // A dotted label resolves as a path, never as a direct key.
            return !field.field().contains('.')
                && schema.field_slot_index(field.field()) == Some(field.index())
                && schema.accepted_query_field_kind(field.field()).is_some();
        }
        let Some(path) = self.as_scalar_path() else {
            return false;
        };
        let Some((root, nested)) = path.label().split_once('.') else {
            return false;
        };
        GroupField::accepted_kind_for_components(schema, path.label().split('.')).is_some()
            && schema.field_slot_index(root) == Some(path.root_slot())
            && path.path().root().as_str() == root
            && nested
                .split('.')
                .eq(path.path().segments().iter().map(String::as_str))
    }

    /// Compare borrowed key identities independently of their container representation.
    #[must_use]
    pub(in crate::db) fn same_identity(&self, other: GroupFieldRef<'_>) -> bool {
        if let Some(field) = self.as_direct() {
            return other
                .as_direct()
                .is_some_and(|other| field.index() == other.index());
        }
        match (self.as_scalar_path(), other.as_scalar_path()) {
            (Some(left), Some(right)) => left.path() == right.path(),
            _ => false,
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
        crate::db::query::preparation::with_preparation_work(|work| {
            let mut fields = GroupFieldSet::empty();
            fields
                .push(
                    GroupField::Direct(FieldSlot::from_test_accepted_kind(
                        0,
                        "direct_rank",
                        AcceptedFieldKind::Int32,
                    )),
                    work,
                )
                .unwrap();

            assert_eq!(fields.as_direct().map(<[FieldSlot]>::len), Some(1));
            assert!(fields.as_path_aware().is_none());
        });
    }

    #[test]
    fn first_path_promotes_the_whole_tuple_once_and_preserves_order() {
        crate::db::query::preparation::with_preparation_work(|work| {
            let mut fields = GroupFieldSet::empty();
            fields
                .push(
                    GroupField::Direct(FieldSlot::from_test_accepted_kind(
                        0,
                        "direct_rank",
                        AcceptedFieldKind::Int32,
                    )),
                    work,
                )
                .unwrap();
            fields.push(rank_path(), work).unwrap();
            fields.push(rank_path(), work).unwrap();

            assert!(fields.as_direct().is_none());
            assert_eq!(
                fields.iter().map(|field| field.field()).collect::<Vec<_>>(),
                vec!["direct_rank", "profile.rank"],
            );
        });
    }

    #[test]
    fn preallocated_path_tuple_matches_incremental_identity_without_promotion() {
        crate::db::query::preparation::with_preparation_work(|work| {
            let direct = GroupField::Direct(FieldSlot::from_test_accepted_kind(
                0,
                "direct_rank",
                AcceptedFieldKind::Int32,
            ));
            let mut expected = GroupFieldSet::empty();
            let mut reserved = GroupFieldSet::PathAware(Vec::with_capacity(4));
            for key in [direct.clone(), rank_path(), direct, rank_path()] {
                expected.push(key.clone(), work).unwrap();
                reserved.push(key, work).unwrap();
            }
            assert_eq!(reserved, expected);
            let GroupFieldSet::PathAware(fields) = reserved else {
                panic!("path-aware group tuple");
            };
            assert_eq!(fields.capacity(), 4);
            assert_eq!(fields.len(), 2);
        });
    }

    #[test]
    fn rejected_group_key_insertion_preserves_the_existing_tuple() {
        use crate::db::{
            RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
            DiagnosticFactTag,
        };

        let direct = FieldSlot::from_test_accepted_kind(0, "rank", AcceptedFieldKind::Int32);
        for (resource, incoming) in [
            (Resource::TemporaryBytes, rank_path()),
            (
                Resource::PredicateExpressionSteps,
                GroupField::Direct(direct.clone()),
            ),
        ] {
            let mut fields = GroupFieldSet::Direct(vec![direct.clone()]);
            let before = fields.clone();
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, 0),
            );
            for _ in 0..2 {
                let error = PreparationWork::run(
                    &root.scope(),
                    DiagnosticExecutionLane::PublicRead,
                    |work| fields.push(incoming.clone(), work),
                )
                .unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                assert_eq!(fields, before);
            }
            crate::db::query::preparation::with_preparation_work(|work| {
                fields.push(incoming, work)
            })
            .unwrap();
        }
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(GroupField {
Self::Direct(field_0) => [field_0],
Self::ScalarPath(field_0) => [field_0],
});
crate::retained::retained_fields!(GroupFieldSet {
Self::Direct(field_0) => [field_0],
Self::PathAware(field_0) => [field_0],
});
crate::retained::retained_fields!(ScalarGroupPath {
Self{label,path,root_slot,identity_group_canonical_form} => [label,path,root_slot,identity_group_canonical_form],
});
