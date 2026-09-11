#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::AccessPlan,
        query::{
            fingerprint::hash_sections::{
                ACCESS_TAG_BY_KEY, ACCESS_TAG_BY_KEYS, ACCESS_TAG_FULL_SCAN,
                ACCESS_TAG_INDEX_BRANCH_SET, ACCESS_TAG_INDEX_MULTI_LOOKUP,
                ACCESS_TAG_INDEX_PREFIX, ACCESS_TAG_INDEX_RANGE, ACCESS_TAG_INTERSECTION,
                ACCESS_TAG_KEY_RANGE, ACCESS_TAG_UNION, write_str, write_tag, write_u32,
                write_value, write_value_bound,
            },
            plan::{AccessPlanProjection, project_access_plan},
        },
    },
    value::Value,
};
use sha2::Sha256;
use std::ops::Bound;

///
/// AccessFingerprintVisitor
///
/// Hash planner-owned lowered values without converting or copying keys.
///
struct AccessFingerprintVisitor<'a> {
    hasher: &'a mut Sha256,
}

/// Hash planner-owned access contracts into the plan hash stream.
pub(in crate::db::query::fingerprint::hash_sections) fn hash_access_plan(
    hasher: &mut Sha256,
    access: &AccessPlan<Value>,
) {
    let mut visitor = AccessFingerprintVisitor { hasher };
    project_access_plan(access, &mut visitor);
}

fn write_access_fields<'a>(
    hasher: &mut Sha256,
    tag: u8,
    name: &str,
    fields: impl ExactSizeIterator<Item = &'a str> + Clone,
) {
    write_tag(hasher, tag);
    write_str(hasher, name);
    write_u32(hasher, fields.len() as u32);
    for field in fields {
        write_str(hasher, field);
    }
}

fn write_values(hasher: &mut Sha256, values: &[Value]) {
    write_u32(hasher, values.len() as u32);
    for value in values {
        write_value(hasher, value);
    }
}

impl AccessPlanProjection<Value> for AccessFingerprintVisitor<'_> {
    type Output = ();

    fn by_key(&mut self, key: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEY);
        write_value(self.hasher, key);
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEYS);
        write_values(self.hasher, keys);
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_KEY_RANGE);
        write_value(self.hasher, start);
        write_value(self.hasher, end);
    }

    fn index_prefix<'a>(
        &mut self,
        name: &str,
        fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_PREFIX, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, values);
    }

    fn index_multi_lookup<'a>(
        &mut self,
        name: &str,
        fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_MULTI_LOOKUP, name, fields);
        write_values(self.hasher, values);
    }

    fn index_branch_set<'a>(
        &mut self,
        name: &str,
        fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        fixed_values: &[Value],
        branch_values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_BRANCH_SET, name, fields);
        write_values(self.hasher, fixed_values);
        write_values(self.hasher, branch_values);
    }

    fn index_range<'a>(
        &mut self,
        name: &str,
        fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_RANGE, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, prefix);
        write_value_bound(self.hasher, lower);
        write_value_bound(self.hasher, upper);
    }

    fn full_scan(&mut self) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_FULL_SCAN);
    }

    fn union<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        // Identity uses the maintained postorder stream, without a child Vec.
        for child in children {
            project(child, self);
        }
        write_tag(self.hasher, ACCESS_TAG_UNION);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        for child in children {
            project(child, self);
        }
        write_tag(self.hasher, ACCESS_TAG_INTERSECTION);
        write_u32(self.hasher, children.len() as u32);
    }
}
