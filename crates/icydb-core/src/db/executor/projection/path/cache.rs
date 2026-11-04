//! Row-local sibling selection, shared across authored expression positions.
//! Only executed paths load roots; selected future leaves remain undecoded.

use crate::{
    db::{
        data::{ValueStorageView, decode_structural_value_storage_bytes},
        executor::projection::path::{resolve_value_field_path, select_value_map_members},
        query::plan::expr::{CompiledExpr, ProjectionEvalError},
        resource::budget::{ExecutionBudgetChargeError, charge_current_execution_budget_typed},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Cow;

// Keep budget exhaustion typed until the expression error boundary. Converting
// it to a reader classification would discard its resource and limit facts.
pub(in crate::db::executor::projection) enum PathError {
    Budget(ExecutionBudgetChargeError),
    Reader(InternalError),
}

impl PathError {
    pub(in crate::db::executor::projection) fn into_projection_error(self) -> ProjectionEvalError {
        match self {
            Self::Budget(error) => ProjectionEvalError::Budget(error),
            Self::Reader(error) => ProjectionEvalError::FieldPathEvaluationFailed {
                class: error.class(),
                origin: error.origin(),
            },
        }
    }
}

impl From<ExecutionBudgetChargeError> for PathError {
    fn from(error: ExecutionBudgetChargeError) -> Self {
        Self::Budget(error)
    }
}

impl From<InternalError> for PathError {
    fn from(error: InternalError) -> Self {
        Self::Reader(error)
    }
}

pub(in crate::db::executor::projection) enum PathSource<'row> {
    Missing,
    Raw(&'row [u8]),
    Value(Cow<'row, Value>),
}

pub(in crate::db::executor::projection) enum PathValue<'row> {
    MissingRoot,
    Leaf(Option<Cow<'row, Value>>),
}

enum Selection<'row> {
    Raw(Vec<Option<&'row [u8]>>),
    Value {
        parent: Cow<'row, Value>,
        positions: Vec<Option<usize>>,
    },
    Missing,
}

struct Group<'plan, 'row> {
    root: usize,
    prefix: &'plan [Box<[u8]>],
    members: Vec<&'plan [u8]>,
    selected: Selection<'row>,
}

/// Share one sibling traversal without evaluating or materializing future leaves.
pub(in crate::db::executor::projection) struct PathCache<'plan, 'row> {
    expressions: &'plan [CompiledExpr],
    groups: Vec<Group<'plan, 'row>>,
}

impl<'plan, 'row> PathCache<'plan, 'row> {
    /// Creating a reader must not allocate for unexecuted lazy branches.
    pub(in crate::db::executor::projection) const fn new(
        expressions: &'plan [CompiledExpr],
    ) -> Self {
        Self {
            expressions,
            groups: Vec::new(),
        }
    }

    /// Resolve at the authored read position, retaining first-match semantics.
    pub(in crate::db::executor::projection) fn resolve(
        &mut self,
        root: usize,
        segments: &[Box<[u8]>],
        load: impl FnOnce() -> Result<PathSource<'row>, InternalError>,
    ) -> Result<PathValue<'row>, PathError> {
        let Some((member, prefix)) = segments.split_last() else {
            return single_path(load()?, segments);
        };
        for group in &self.groups {
            if matches_parent(group.root, group.prefix, root, prefix)? {
                return group.resolve(member);
            }
        }
        let source = load()?;
        if matches!(source, PathSource::Missing) {
            return Ok(PathValue::MissingRoot);
        }
        let mut parent = None;
        let mut count = 0usize;
        visit_siblings(self.expressions, root, prefix, |selected_prefix, _| {
            parent = Some(selected_prefix);
            count = count.saturating_add(1);
            Ok(())
        })?;
        if count < 2 {
            return single_path(source, segments);
        }
        let prefix = parent.ok_or_else(InternalError::query_executor_invariant)?;
        charge_backing::<&[u8]>(count)?;
        let mut members = Vec::with_capacity(count);
        visit_siblings(self.expressions, root, prefix, |_, member| {
            members.push(member);
            Ok(())
        })?;
        let selected = Selection::new(source, prefix, &members)?;
        if self.groups.len() == self.groups.capacity() {
            charge_backing::<Group<'_, '_>>(self.groups.len().saturating_add(1))?;
            self.groups.reserve_exact(1);
        }
        let group = Group {
            root,
            prefix,
            members,
            selected,
        };
        let value = group.resolve(member)?;
        self.groups.push(group);
        Ok(value)
    }
}

impl<'row> Selection<'row> {
    fn new(
        source: PathSource<'row>,
        prefix: &[Box<[u8]>],
        members: &[&[u8]],
    ) -> Result<Self, PathError> {
        match source {
            PathSource::Missing => Err(InternalError::query_executor_invariant().into()),
            PathSource::Raw(raw) => {
                charge_backing::<Option<&[u8]>>(members.len())?;
                let mut selected = vec![None; members.len()];
                ValueStorageView::select_raw_map_members(raw, prefix, members, &mut selected)
                    .map_err(|_| InternalError::persisted_row_decode_corruption())?;
                Ok(Self::Raw(selected))
            }
            PathSource::Value(root) => {
                let parent = match root {
                    Cow::Borrowed(root) => resolve_value_field_path(root, prefix)
                        .map_err(ProjectionEvalError::into_internal_error)?
                        .map(Cow::Borrowed),
                    Cow::Owned(root) => owned_parent(root, prefix)?.map(Cow::Owned),
                };
                let Some(parent) = parent.filter(|value| !matches!(value.as_ref(), Value::Null))
                else {
                    return Ok(Self::Missing);
                };
                charge_backing::<Option<usize>>(members.len())?;
                let mut positions = vec![None; members.len()];
                select_value_map_members(parent.as_ref(), members, &mut positions, |index, _| {
                    index
                })
                .map_err(ProjectionEvalError::into_internal_error)?;
                Ok(Self::Value { parent, positions })
            }
        }
    }

    fn resolve(&self, index: usize) -> Result<PathValue<'row>, PathError> {
        let value = match self {
            Self::Missing => None,
            Self::Raw(selected) => selected
                .get(index)
                .ok_or_else(InternalError::query_executor_invariant)?
                .map(decode_structural_value_storage_bytes)
                .transpose()
                .map_err(|_| InternalError::persisted_row_decode_corruption())?
                .map(Cow::Owned),
            Self::Value { parent, positions } => {
                let Some(index) = *positions
                    .get(index)
                    .ok_or_else(InternalError::query_executor_invariant)?
                else {
                    return Ok(PathValue::Leaf(None));
                };
                match parent {
                    Cow::Borrowed(parent) => Some(Cow::Borrowed(value_at(parent, index)?)),
                    Cow::Owned(parent) => Some(Cow::Owned(value_at(parent, index)?.clone())),
                }
            }
        };
        Ok(PathValue::Leaf(value))
    }
}

impl<'row> Group<'_, 'row> {
    fn resolve(&self, member: &[u8]) -> Result<PathValue<'row>, PathError> {
        for (index, candidate) in self.members.iter().enumerate() {
            charge_current_execution_budget_typed(
                Resource::PredicateExpressionSteps,
                1 + candidate.len() as u64,
            )?;
            if *candidate == member {
                return self.selected.resolve(index);
            }
        }
        Err(InternalError::query_executor_invariant().into())
    }
}

fn visit_siblings<'plan>(
    expressions: &'plan [CompiledExpr],
    root: usize,
    prefix: &[Box<[u8]>],
    mut visit: impl FnMut(&'plan [Box<[u8]>], &'plan [u8]) -> Result<(), PathError>,
) -> Result<(), PathError> {
    for expression in expressions {
        expression.try_for_each_node(&mut |node| -> Result<(), PathError> {
            charge_current_execution_budget_typed(Resource::PredicateExpressionSteps, 1)?;
            if let CompiledExpr::FieldPath {
                root_slot,
                segment_bytes,
                ..
            } = node
                && let Some((member, parent)) = segment_bytes.split_last()
                && matches_parent(*root_slot, parent, root, prefix)?
            {
                visit(parent, member)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn matches_parent(
    left_root: usize,
    left: &[Box<[u8]>],
    right_root: usize,
    right: &[Box<[u8]>],
) -> Result<bool, PathError> {
    charge_current_execution_budget_typed(
        Resource::PredicateExpressionSteps,
        1 + left.iter().map(|part| part.len() as u64).sum::<u64>(),
    )?;
    Ok(left_root == right_root && left == right)
}

fn charge_backing<T>(count: usize) -> Result<(), PathError> {
    charge_current_execution_budget_typed(
        Resource::TemporaryBytes,
        (count as u64).saturating_mul(size_of::<T>() as u64),
    )
    .map_err(PathError::from)
}

fn value_at(parent: &Value, index: usize) -> Result<&Value, PathError> {
    parent
        .as_map()
        .and_then(|entries| entries.get(index))
        .map(|(_, value)| value)
        .ok_or_else(|| InternalError::query_executor_invariant().into())
}

// A synthesized historical root is already owned. Move its selected parent,
// retaining future leaves without eagerly cloning or decoding them.
fn owned_parent(mut value: Value, prefix: &[Box<[u8]>]) -> Result<Option<Value>, PathError> {
    for segment in prefix {
        let entries = match value {
            Value::Null => return Ok(None),
            Value::Map(entries) => entries,
            _ => return Err(InternalError::persisted_row_decode_corruption().into()),
        };
        let Some((_, child)) = entries.into_iter().find(
            |(key, _)| matches!(key, Value::Text(text) if text.as_bytes() == segment.as_ref()),
        ) else {
            return Ok(None);
        };
        value = child;
    }
    Ok(Some(value))
}

fn single_path<'row>(
    source: PathSource<'row>,
    segments: &[Box<[u8]>],
) -> Result<PathValue<'row>, PathError> {
    let value = match source {
        PathSource::Missing => return Ok(PathValue::MissingRoot),
        PathSource::Raw(raw) => ValueStorageView::from_raw_map_text_path(raw, segments)
            .map_err(|_| InternalError::persisted_row_decode_corruption())?
            .map(|view| decode_structural_value_storage_bytes(view.as_bytes()))
            .transpose()
            .map_err(|_| InternalError::persisted_row_decode_corruption())?
            .map(Cow::Owned),
        PathSource::Value(Cow::Borrowed(root)) => resolve_value_field_path(root, segments)
            .map_err(ProjectionEvalError::into_internal_error)?
            .map(Cow::Borrowed),
        PathSource::Value(Cow::Owned(root)) => resolve_value_field_path(&root, segments)
            .map_err(ProjectionEvalError::into_internal_error)?
            .cloned()
            .map(Cow::Owned),
    };
    Ok(PathValue::Leaf(value))
}
