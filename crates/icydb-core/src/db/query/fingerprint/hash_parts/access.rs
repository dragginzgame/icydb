use crate::{
    db::{
        access::AccessPlan,
        query::{
            explain::ExplainAccessPath,
            fingerprint::hash_parts::{
                ACCESS_TAG_BY_KEY, ACCESS_TAG_BY_KEYS, ACCESS_TAG_FULL_SCAN,
                ACCESS_TAG_INDEX_MULTI_LOOKUP, ACCESS_TAG_INDEX_PREFIX, ACCESS_TAG_INDEX_RANGE,
                ACCESS_TAG_INTERSECTION, ACCESS_TAG_KEY_RANGE, ACCESS_TAG_UNION, write_str,
                write_tag, write_u32, write_value, write_value_bound,
            },
            plan::{AccessPlanProjection, project_access_plan, project_explain_access_path},
        },
    },
    traits::FieldValue,
    value::Value,
};
use sha2::Sha256;
use std::ops::Bound;

///
/// FingerprintVisitor
///
/// Explain-access hash visitor that preserves canonical child-before-parent
/// token ordering used by structural fingerprinting.
///

struct FingerprintVisitor<'a> {
    hasher: &'a mut Sha256,
}

///
/// PlanFingerprintVisitor
///
/// Access-plan hash visitor over planner-owned canonical access contracts.
/// This keeps identity hashing independent from explain DTO projection.
///

struct PlanFingerprintVisitor<'a> {
    hasher: &'a mut Sha256,
}

/// Hash explain access paths into the plan hash stream.
pub(super) fn hash_access(hasher: &mut Sha256, access: &ExplainAccessPath) {
    let mut visitor = FingerprintVisitor { hasher };
    project_explain_access_path(access, &mut visitor);
}

/// Hash planner-owned access contracts into the plan hash stream.
pub(in crate::db) fn hash_access_plan<K>(hasher: &mut Sha256, access: &AccessPlan<K>)
where
    K: FieldValue,
{
    let mut visitor = PlanFingerprintVisitor { hasher };
    project_access_plan(access, &mut visitor);
}

fn write_access_fields(hasher: &mut Sha256, tag: u8, name: &'static str, fields: &[&'static str]) {
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

fn write_field_values<K>(hasher: &mut Sha256, values: &[K])
where
    K: FieldValue,
{
    write_u32(hasher, values.len() as u32);
    for value in values {
        write_value(hasher, &value.to_value());
    }
}

impl AccessPlanProjection<Value> for FingerprintVisitor<'_> {
    type Output = ();

    fn by_key(&mut self, key: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEY);
        write_value(self.hasher, key);
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEYS);
        write_u32(self.hasher, keys.len() as u32);
        for key in keys {
            write_value(self.hasher, key);
        }
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_KEY_RANGE);
        write_value(self.hasher, start);
        write_value(self.hasher, end);
    }

    fn index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_PREFIX, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, values);
    }

    fn index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_MULTI_LOOKUP, name, fields);
        write_values(self.hasher, values);
    }

    fn index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
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

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_UNION);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_INTERSECTION);
        write_u32(self.hasher, children.len() as u32);
    }
}

impl<K> AccessPlanProjection<K> for PlanFingerprintVisitor<'_>
where
    K: FieldValue,
{
    type Output = ();

    fn by_key(&mut self, key: &K) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEY);
        write_value(self.hasher, &key.to_value());
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEYS);
        write_field_values(self.hasher, keys);
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_KEY_RANGE);
        write_value(self.hasher, &start.to_value());
        write_value(self.hasher, &end.to_value());
    }

    fn index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_PREFIX, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, values);
    }

    fn index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_MULTI_LOOKUP, name, fields);
        write_values(self.hasher, values);
    }

    fn index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
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

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_UNION);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_INTERSECTION);
        write_u32(self.hasher, children.len() as u32);
    }
}
