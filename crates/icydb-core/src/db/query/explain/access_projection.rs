//! Module: query::explain::access_projection
//! Responsibility: access-path projection adapters for EXPLAIN.
//! Does not own: logical plan policy or execution descriptor rendering.
//! Boundary: planner access path -> explain access DTOs/json adapters.

#[cfg(test)]
mod construction_tests;

use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{
    fmt::{self, Write},
    ops::Bound,
};

use crate::{
    db::QueryError,
    db::{
        access::AccessPlan,
        query::{
            explain::{ExplainAccessPath, writer::JsonWriter},
            plan::{AccessPlanProjection, project_access_plan},
            preparation::PreparationWork,
        },
    },
    error::InternalError,
    value::Value,
};

const INDEX_BRANCH_SET_ORDERED_SUFFIX_LABEL: &str = "primary_key_asc";
// Diagnostic policy for derived access shapes, independent of authored input
// admission. Root and leaf each count as one level.
pub(in crate::db::query::explain) const MAX_EXPLAIN_ACCESS_DEPTH: usize = 128;

///
/// ExplainAccessProjection
///
/// Local EXPLAIN adapter that consumes the planner-owned access traversal
/// contract and projects it into the transport-facing `ExplainAccessPath` DTO.
///

struct ExplainAccessProjection<'work, 'scope> {
    work: &'work PreparationWork<'scope>,
    depth: usize,
}

impl ExplainAccessProjection<'_, '_> {
    fn node(&self) -> Result<(), QueryError> {
        self.work.charge(Resource::PredicateExpressionSteps, 1)
    }

    fn fields<'a>(
        &self,
        fields: impl ExactSizeIterator<Item = &'a str>,
    ) -> Result<Vec<String>, QueryError> {
        let mut copied = self.work.vec_with_capacity(fields.len())?;
        for field in fields {
            copied.push(self.work.copy_text(field)?);
        }
        Ok(copied)
    }

    fn bound(&self, bound: &Bound<Value>) -> Result<Bound<Value>, QueryError> {
        Ok(match bound {
            Bound::Included(value) => Bound::Included(self.work.copy_value(value)?),
            Bound::Excluded(value) => Bound::Excluded(self.work.copy_value(value)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    }

    fn children<T>(
        &self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Result<ExplainAccessPath, QueryError>,
    ) -> Result<Vec<ExplainAccessPath>, QueryError> {
        self.node()?;
        // Reject before allocating child backing or descending. Local child
        // visitors keep sibling depth stable, including after a failed child.
        if self.depth == MAX_EXPLAIN_ACCESS_DEPTH && !children.is_empty() {
            return Err(QueryError::execute(
                InternalError::query_explain_depth_exceeded(
                    MAX_EXPLAIN_ACCESS_DEPTH as u64,
                    (self.depth + 1) as u64,
                ),
            ));
        }
        // Reserve final DTO backing before descent; a failed child stops sibling
        // construction and cannot publish a partial composite.
        let mut copied = self.work.vec_with_capacity(children.len())?;
        let mut child_projection = Self {
            work: self.work,
            depth: self.depth + 1,
        };
        for child in children {
            copied.push(project(child, &mut child_projection)?);
        }
        Ok(copied)
    }
}

impl AccessPlanProjection<Value> for ExplainAccessProjection<'_, '_> {
    type Output = Result<ExplainAccessPath, QueryError>;

    fn by_key(&mut self, key: &Value) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::ByKey {
            key: self.work.copy_value(key)?,
        })
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::ByKeys {
            keys: self
                .work
                .copy_slice(keys, |key| self.work.copy_value(key))?,
        })
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::KeyRange {
            start: self.work.copy_value(start)?,
            end: self.work.copy_value(end)?,
        })
    }

    fn index_prefix<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::IndexPrefix {
            name: self.work.copy_text(index_name)?,
            fields: self.fields(index_fields)?,
            prefix_len,
            values: self
                .work
                .copy_slice(values, |value| self.work.copy_value(value))?,
        })
    }

    fn index_multi_lookup<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        values: &[Value],
    ) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::IndexMultiLookup {
            name: self.work.copy_text(index_name)?,
            fields: self.fields(index_fields)?,
            values: self
                .work
                .copy_slice(values, |value| self.work.copy_value(value))?,
        })
    }

    fn index_branch_set<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        fixed_values: &[Value],
        branch_values: &[Value],
    ) -> Self::Output {
        self.node()?;
        let name = self.work.copy_text(index_name)?;
        let fields = self.fields(index_fields)?;
        let branch_field = fields
            .get(fixed_values.len())
            .map(|field| self.work.copy_text(field))
            .transpose()?;
        Ok(ExplainAccessPath::IndexBranchSet {
            name,
            fields,
            fixed_values: self
                .work
                .copy_slice(fixed_values, |value| self.work.copy_value(value))?,
            branch_values: self
                .work
                .copy_slice(branch_values, |value| self.work.copy_value(value))?,
            branch_field,
        })
    }

    fn index_range<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::IndexRange {
            name: self.work.copy_text(index_name)?,
            fields: self.fields(index_fields)?,
            prefix_len,
            prefix: self
                .work
                .copy_slice(prefix, |value| self.work.copy_value(value))?,
            lower: self.bound(lower)?,
            upper: self.bound(upper)?,
        })
    }

    fn full_scan(&mut self) -> Self::Output {
        self.node()?;
        Ok(ExplainAccessPath::FullScan)
    }

    fn union<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        self.children(children, project)
            .map(ExplainAccessPath::Union)
    }

    fn intersection<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        self.children(children, project)
            .map(ExplainAccessPath::Intersection)
    }
}

// Serialize the borrowed DTO directly: a fold producing child Strings cannot
// stream into the parent's bounded destination or stop at its first failure.
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive DTO match keeps canonical JSON field order and fallible streaming together"
)]
pub(in crate::db::query::explain) fn write_access_json(
    access: &ExplainAccessPath,
    out: &mut dyn Write,
) -> fmt::Result {
    let mut object = JsonWriter::begin_object(out)?;
    match access {
        ExplainAccessPath::ByKey { .. } => {
            object.field_str("type", "ByKey")?;
            object.field_u64("key_count", 1)?;
        }
        ExplainAccessPath::ByKeys { keys } => {
            object.field_str("type", "ByKeys")?;
            object.field_u64("key_count", keys.len() as u64)?;
        }
        ExplainAccessPath::KeyRange { .. } => {
            object.field_str("type", "KeyRange")?;
        }
        ExplainAccessPath::IndexPrefix {
            name: index_name,
            fields: index_fields,
            prefix_len,
            values,
        } => {
            let prefix_len = *prefix_len;

            object.field_str("type", "IndexPrefix")?;
            object.field_str("name", index_name)?;
            object.field_str_slice("fields", index_fields)?;
            object.field_u64("prefix_len", prefix_len as u64)?;
            object.field_u64("value_count", values.len() as u64)?;
            object.field_str_slice(
                "bound_fields",
                bounded_prefix_fields(index_fields, prefix_len),
            )?;
            object.field_str_slice(
                "unbound_fields",
                unbound_prefix_fields(index_fields, prefix_len),
            )?;
        }
        ExplainAccessPath::IndexMultiLookup {
            name: index_name,
            fields: index_fields,
            values,
        } => {
            object.field_str("type", "IndexMultiLookup")?;
            object.field_str("name", index_name)?;
            object.field_str_slice("fields", index_fields)?;
            object.field_u64("value_count", values.len() as u64)?;
        }
        ExplainAccessPath::IndexBranchSet {
            name: index_name,
            fields: index_fields,
            fixed_values,
            branch_values,
            branch_field,
        } => {
            debug_assert_eq!(
                branch_field.as_deref(),
                index_fields.get(fixed_values.len()).map(String::as_str),
            );
            object.field_str("type", "IndexBranchSet")?;
            object.field_str("name", index_name)?;
            object.field_str_slice("fields", index_fields)?;
            object.field_u64("fixed_prefix_len", fixed_values.len() as u64)?;
            object.field_u64("branch_count", branch_values.len() as u64)?;
            object.field_str("ordered_suffix", INDEX_BRANCH_SET_ORDERED_SUFFIX_LABEL)?;
            match index_fields.get(fixed_values.len()) {
                Some(branch_field) => object.field_str("branch_field", branch_field)?,
                None => object.field_null("branch_field")?,
            }
            object.field_str_slice(
                "bound_fields",
                bounded_prefix_fields(index_fields, fixed_values.len().saturating_add(1)),
            )?;
        }
        ExplainAccessPath::IndexRange {
            name: index_name,
            fields: index_fields,
            prefix_len,
            prefix: _,
            lower,
            upper,
        } => {
            let prefix_len = *prefix_len;

            object.field_str("type", "IndexRange")?;
            object.field_str("name", index_name)?;
            object.field_str_slice("fields", index_fields)?;
            object.field_u64("prefix_len", prefix_len as u64)?;
            object.field_str_slice(
                "equality_prefix_fields",
                bounded_prefix_fields(index_fields, prefix_len),
            )?;
            match index_fields.get(prefix_len) {
                Some(range_field) => object.field_str("range_field", range_field)?,
                None => object.field_null("range_field")?,
            }
            object.field_str("lower_inclusivity", bound_inclusivity(lower))?;
            object.field_str("upper_inclusivity", bound_inclusivity(upper))?;
            object.field_str_slice(
                "trailing_fields",
                trailing_range_fields(index_fields, prefix_len),
            )?;
        }
        ExplainAccessPath::FullScan => {
            object.field_str("type", "FullScan")?;
        }

        ExplainAccessPath::Union(children) | ExplainAccessPath::Intersection(children) => {
            object.field_str(
                "type",
                if matches!(access, ExplainAccessPath::Union(_)) {
                    "Union"
                } else {
                    "Intersection"
                },
            )?;
            object.field_with("children", |out| {
                out.write_char('[')?;
                for (index, child) in children.iter().enumerate() {
                    if index > 0 {
                        out.write_char(',')?;
                    }
                    write_access_json(child, out)?;
                }
                out.write_char(']')
            })?;
        }
    }
    object.finish()
}

pub(in crate::db) fn explain_access_plan(
    access: &AccessPlan<Value>,
    work: &PreparationWork<'_>,
) -> Result<ExplainAccessPath, QueryError> {
    project_access_plan(access, &mut ExplainAccessProjection { work, depth: 1 })
}

fn bounded_prefix_fields(index_fields: &[String], prefix_len: usize) -> &[String] {
    &index_fields[..prefix_len.min(index_fields.len())]
}

fn unbound_prefix_fields(index_fields: &[String], prefix_len: usize) -> &[String] {
    &index_fields[prefix_len.min(index_fields.len())..]
}

fn trailing_range_fields(index_fields: &[String], prefix_len: usize) -> &[String] {
    let trailing_start = prefix_len.saturating_add(1).min(index_fields.len());

    &index_fields[trailing_start..]
}

const fn bound_inclusivity(bound: &std::ops::Bound<Value>) -> &'static str {
    match bound {
        std::ops::Bound::Included(_) => "inclusive",
        std::ops::Bound::Excluded(_) => "exclusive",
        std::ops::Bound::Unbounded => "unbounded",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::{explain::writer::render_logical, plan::write_explain_access_strategy_label},
        value::Value,
    };

    #[test]
    fn nested_access_json_reports_one_ordered_metadata_contract() {
        let access = ExplainAccessPath::Union(vec![
            ExplainAccessPath::FullScan,
            ExplainAccessPath::Intersection(vec![
                ExplainAccessPath::IndexPrefix {
                    name: "by_owner".into(),
                    fields: vec!["owner".into(), "amount".into()],
                    prefix_len: 1,
                    values: vec![],
                },
                ExplainAccessPath::IndexBranchSet {
                    name: "by_owner".into(),
                    fields: vec!["owner".into(), "amount".into()],
                    fixed_values: vec![],
                    branch_values: vec![],
                    branch_field: Some("owner".into()),
                },
            ]),
        ]);
        let rendered = render_logical(|out| write_access_json(&access, out)).unwrap();
        assert_eq!(
            rendered,
            concat!(
                r#"{"type":"Union","children":[{"type":"FullScan"},{"type":"Intersection","children":["#,
                r#"{"type":"IndexPrefix","name":"by_owner","fields":["owner","amount"],"prefix_len":1,"value_count":0,"bound_fields":["owner"],"unbound_fields":["amount"]},"#,
                r#"{"type":"IndexBranchSet","name":"by_owner","fields":["owner","amount"],"fixed_prefix_len":0,"branch_count":0,"ordered_suffix":"primary_key_asc","branch_field":"owner","bound_fields":["owner"]}]}]}"#,
            )
        );
    }

    #[test]
    fn explain_access_strategy_label_projects_stable_render_labels() {
        for (access, expected) in [
            (
                ExplainAccessPath::ByKey {
                    key: Value::Nat64(1),
                },
                "ByKey",
            ),
            (
                ExplainAccessPath::Union(vec![
                    ExplainAccessPath::FullScan,
                    ExplainAccessPath::ByKeys {
                        keys: vec![Value::Nat64(2)],
                    },
                ]),
                "Union(2)",
            ),
        ] {
            let mut output = String::new();
            write_explain_access_strategy_label(&access, &mut output).unwrap();
            assert_eq!(output, expected);
        }
    }
}
