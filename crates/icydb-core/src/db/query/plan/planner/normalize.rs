use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        query::plan::canonical,
    },
    value::Value,
};

// Normalize composite access plans into canonical, flattened forms.
pub(in crate::db::query::plan::planner) fn normalize_access_plan(
    plan: AccessPlan<Value>,
) -> AccessPlan<Value> {
    plan.normalize_for_planner()
}

impl AccessPlan<Value> {
    // Normalize this access plan into a canonical deterministic form.
    fn normalize_for_planner(self) -> Self {
        match self {
            Self::Path(path) => Self::path(path.normalize_for_planner()),
            Self::Union(children) => Self::normalize_union(children),
            Self::Intersection(children) => Self::normalize_intersection(children),
        }
    }

    fn normalize_union(children: Vec<Self>) -> Self {
        let mut out = Vec::new();

        for child in children {
            let child = child.normalize_for_planner();
            if child.is_single_full_scan() {
                return Self::full_scan();
            }

            Self::append_union_child(&mut out, child);
        }

        Self::collapse_composite(out, true)
    }

    fn normalize_intersection(children: Vec<Self>) -> Self {
        let mut out = Vec::new();

        for child in children {
            let child = child.normalize_for_planner();
            if child.is_single_full_scan() {
                continue;
            }

            Self::append_intersection_child(&mut out, child);
        }

        Self::collapse_composite(out, false)
    }

    fn collapse_composite(mut out: Vec<Self>, is_union: bool) -> Self {
        if out.is_empty() {
            return Self::full_scan();
        }
        if out.len() == 1 {
            return out.pop().expect("single composite child");
        }

        canonical::canonicalize_access_plans_value(&mut out);
        out.dedup();
        if out.len() == 1 {
            return out.pop().expect("single composite child");
        }

        if is_union {
            Self::Union(out)
        } else {
            Self::Intersection(out)
        }
    }

    fn append_union_child(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Union(children) => out.extend(children),
            other => out.push(other),
        }
    }

    fn append_intersection_child(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Intersection(children) => out.extend(children),
            other => out.push(other),
        }
    }
}

impl AccessPath<Value> {
    // Normalize one concrete access path for deterministic planning.
    fn normalize_for_planner(self) -> Self {
        match self {
            Self::ByKeys(mut keys) => {
                canonical::canonicalize_key_values(&mut keys);
                Self::ByKeys(keys)
            }
            other => other,
        }
    }
}
