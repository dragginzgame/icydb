use crate::db::query::fingerprint::hash_parts::{
    CONTINUATION_SECTION_ACCESS_TAG, CONTINUATION_SECTION_DISTINCT_TAG,
    CONTINUATION_SECTION_ENTITY_PATH_TAG, CONTINUATION_SECTION_GROUPING_SHAPE_TAG,
    CONTINUATION_SECTION_MODE_TAG, CONTINUATION_SECTION_ORDER_TAG,
    CONTINUATION_SECTION_PREDICATE_TAG, CONTINUATION_SECTION_PROJECTION_SPEC_TAG,
    FINGERPRINT_SECTION_ACCESS_TAG, FINGERPRINT_SECTION_CONSISTENCY_TAG,
    FINGERPRINT_SECTION_DELETE_LIMIT_TAG, FINGERPRINT_SECTION_DISTINCT_TAG,
    FINGERPRINT_SECTION_MODE_TAG, FINGERPRINT_SECTION_ORDER_TAG, FINGERPRINT_SECTION_PAGE_TAG,
    FINGERPRINT_SECTION_PREDICATE_TAG, FINGERPRINT_SECTION_PROJECTION_SPEC_TAG,
    access::{hash_access, hash_access_plan},
    grouping::{GroupingFingerprintSource, hash_grouping_shape_v1, hash_projection_spec_v1},
    hash_consistency, hash_delete_limit, hash_delete_limit_spec, hash_distinct, hash_mode,
    hash_order, hash_order_spec, hash_page, hash_page_spec, hash_scalar_semantic_filter, write_str,
    write_tag,
};
use crate::db::query::{
    explain::ExplainPlan,
    plan::{AccessPlannedQuery, expr::ProjectionSpec},
};
use sha2::Sha256;

///
/// ExplainHashProfile
///
/// Hashing profiles that select canonical explain-surface fields.
///

pub(in crate::db::query) enum ExplainHashProfile<'a> {
    Fingerprint,
    Continuation { entity_path: &'a str },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::fingerprint::hash_parts) enum ExplainHashField {
    EntityPath,
    Mode,
    Access,
    Predicate,
    Order,
    Distinct,
    Page,
    DeleteLimit,
    Consistency,
    GroupingShape,
    ProjectionSpec,
}

///
/// ExplainHashSource
///
/// Canonical hash-profile source shared by explain and planner-owned query
/// hashing. This keeps the per-field profile walk on one owner-local seam
/// instead of maintaining parallel match trees for the two input surfaces.
///

#[allow(dead_code)]
#[derive(Clone, Copy)]
enum ExplainHashSource<'a> {
    Explain(&'a ExplainPlan),
    Planned(&'a AccessPlannedQuery),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::fingerprint::hash_parts) struct ExplainHashStep {
    pub(super) section_tag: u8,
    pub(super) field: ExplainHashField,
}

pub(in crate::db::query::fingerprint::hash_parts) struct ExplainHashProfileSpec<'a> {
    pub(super) entity_path: Option<&'a str>,
    pub(super) steps: &'static [ExplainHashStep],
}

pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_STEPS: [ExplainHashStep; 9] = [
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PAGE_TAG,
        field: ExplainHashField::Page,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_DELETE_LIMIT_TAG,
        field: ExplainHashField::DeleteLimit,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_CONSISTENCY_TAG,
        field: ExplainHashField::Consistency,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpec,
    },
];

pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_STEPS: [ExplainHashStep; 8] = [
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ENTITY_PATH_TAG,
        field: ExplainHashField::EntityPath,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_GROUPING_SHAPE_TAG,
        field: ExplainHashField::GroupingShape,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpec,
    },
];

impl<'a> ExplainHashProfile<'a> {
    pub(in crate::db::query::fingerprint::hash_parts) const fn spec(
        self,
    ) -> ExplainHashProfileSpec<'a> {
        match self {
            Self::Fingerprint => ExplainHashProfileSpec {
                entity_path: None,
                steps: &FINGERPRINT_STEPS,
            },
            Self::Continuation { entity_path } => ExplainHashProfileSpec {
                entity_path: Some(entity_path),
                steps: &CONTINUATION_STEPS,
            },
        }
    }
}

impl<'a> ExplainHashSource<'a> {
    const fn grouping_source(self) -> GroupingFingerprintSource<'a> {
        match self {
            Self::Explain(plan) => GroupingFingerprintSource::Explain(plan.grouping()),
            Self::Planned(plan) => GroupingFingerprintSource::Plan(plan),
        }
    }

    fn hash_field(
        self,
        hasher: &mut Sha256,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        match self {
            Self::Explain(plan) => self.hash_explain_field(
                hasher,
                plan,
                field,
                entity_path,
                projection,
                include_group_strategy,
            ),
            Self::Planned(plan) => self.hash_planned_field(
                hasher,
                plan,
                field,
                entity_path,
                projection,
                include_group_strategy,
            ),
        }
    }

    fn hash_explain_field(
        self,
        hasher: &mut Sha256,
        plan: &'a ExplainPlan,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        match field {
            ExplainHashField::EntityPath => {
                let entity_path = entity_path.expect("entity path required by hash profile");
                write_str(hasher, entity_path);
            }
            ExplainHashField::Mode => hash_mode(hasher, plan.mode()),
            ExplainHashField::Access => hash_access(hasher, plan.access()),
            ExplainHashField::Predicate => hash_scalar_semantic_filter(
                hasher,
                plan.filter_expr_model_for_hash(),
                plan.predicate_model_for_hash(),
            ),
            ExplainHashField::Order => hash_order(hasher, plan.order_by()),
            ExplainHashField::Distinct => hash_distinct(hasher, plan.distinct()),
            ExplainHashField::Page => hash_page(hasher, plan.page()),
            ExplainHashField::DeleteLimit => hash_delete_limit(hasher, plan.delete_limit()),
            ExplainHashField::Consistency => hash_consistency(hasher, plan.consistency()),
            ExplainHashField::GroupingShape => {
                hash_grouping_shape_v1(hasher, self.grouping_source(), include_group_strategy);
            }
            ExplainHashField::ProjectionSpec => {
                hash_projection_spec_v1(
                    hasher,
                    projection,
                    self.grouping_source(),
                    include_group_strategy,
                );
            }
        }
    }

    fn hash_planned_field(
        self,
        hasher: &mut Sha256,
        plan: &'a AccessPlannedQuery,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        let scalar = plan.scalar_plan();

        match field {
            ExplainHashField::EntityPath => {
                let entity_path = entity_path.expect("entity path required by hash profile");
                write_str(hasher, entity_path);
            }
            ExplainHashField::Mode => hash_mode(hasher, scalar.mode),
            ExplainHashField::Access => hash_access_plan(hasher, &plan.access),
            ExplainHashField::Predicate => hash_scalar_semantic_filter(
                hasher,
                scalar.filter_expr.as_ref(),
                scalar.predicate.as_ref(),
            ),
            ExplainHashField::Order => hash_order_spec(hasher, scalar.order.as_ref()),
            ExplainHashField::Distinct => hash_distinct(hasher, scalar.distinct),
            ExplainHashField::Page => hash_page_spec(hasher, scalar.page.as_ref()),
            ExplainHashField::DeleteLimit => {
                hash_delete_limit_spec(hasher, scalar.delete_limit.as_ref());
            }
            ExplainHashField::Consistency => hash_consistency(hasher, scalar.consistency),
            ExplainHashField::GroupingShape => {
                hash_grouping_shape_v1(hasher, self.grouping_source(), include_group_strategy);
            }
            ExplainHashField::ProjectionSpec => {
                hash_projection_spec_v1(
                    hasher,
                    projection,
                    self.grouping_source(),
                    include_group_strategy,
                );
            }
        }
    }
}

/// Hash a planner-owned query with an explicit semantic projection section.
pub(in crate::db::query) fn hash_planned_query_profile_with_projection(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    profile: ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_planned_query_profile_internal(hasher, plan, profile, Some(projection));
}

fn hash_planned_query_profile_internal(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    profile: ExplainHashProfile<'_>,
    projection: Option<&ProjectionSpec>,
) {
    let spec = profile.spec();
    let include_group_strategy = spec.entity_path.is_some();
    let source = ExplainHashSource::Planned(plan);

    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        source.hash_field(
            hasher,
            step.field,
            spec.entity_path,
            projection,
            include_group_strategy,
        );
    }
}

/// Hash an `ExplainPlan` using a profile-specific canonical field set.
pub(in crate::db::query) fn hash_explain_plan_profile(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
) {
    hash_explain_plan_profile_internal(hasher, plan, profile, None);
}

pub(in crate::db::query::fingerprint) fn hash_explain_plan_profile_internal(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
    projection: Option<&ProjectionSpec>,
) {
    // Apply selected hash profile in declared order to preserve determinism.
    let spec = profile.spec();
    let include_group_strategy = spec.entity_path.is_some();
    let source = ExplainHashSource::Explain(plan);

    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        source.hash_field(
            hasher,
            step.field,
            spec.entity_path,
            projection,
            include_group_strategy,
        );
    }
}
