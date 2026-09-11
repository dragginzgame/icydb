//! Continuation identity over the planner's canonical inputs.
//! Diagnostic DTOs are not identity sources.

use crate::db::query::{
    fingerprint::hash_sections::{
        CONTINUATION_SECTION_ACCESS_TAG, CONTINUATION_SECTION_DISTINCT_TAG,
        CONTINUATION_SECTION_ENTITY_PATH_TAG, CONTINUATION_SECTION_GROUPING_SHAPE_TAG,
        CONTINUATION_SECTION_MODE_TAG, CONTINUATION_SECTION_ORDER_TAG,
        CONTINUATION_SECTION_PREDICATE_TAG, CONTINUATION_SECTION_PROJECTION_SPEC_TAG,
        access::hash_access_plan,
        grouping::{hash_grouping_shape, hash_projection_spec},
        hash_distinct, hash_mode, hash_order_spec, hash_scalar_semantic_filter, write_str,
        write_tag,
    },
    plan::{AccessPlannedQuery, expr::ProjectionSpec},
};
use sha2::Sha256;

/// Preserve the canonical section order and framing used by cursor identity.
pub(in crate::db::query) fn hash_continuation_with_projection(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    entity_path: &str,
    projection: &ProjectionSpec,
) {
    let scalar = plan.scalar_plan();
    write_tag(hasher, CONTINUATION_SECTION_ENTITY_PATH_TAG);
    write_str(hasher, entity_path);
    write_tag(hasher, CONTINUATION_SECTION_MODE_TAG);
    hash_mode(hasher, scalar.mode);
    write_tag(hasher, CONTINUATION_SECTION_ACCESS_TAG);
    hash_access_plan(hasher, &plan.access);
    write_tag(hasher, CONTINUATION_SECTION_PREDICATE_TAG);
    hash_scalar_semantic_filter(
        hasher,
        scalar.filter_expr.as_ref(),
        scalar.predicate.as_ref(),
    );
    write_tag(hasher, CONTINUATION_SECTION_ORDER_TAG);
    hash_order_spec(hasher, scalar.order.as_ref());
    write_tag(hasher, CONTINUATION_SECTION_DISTINCT_TAG);
    hash_distinct(hasher, scalar.distinct);
    write_tag(hasher, CONTINUATION_SECTION_GROUPING_SHAPE_TAG);
    hash_grouping_shape(hasher, plan);
    write_tag(hasher, CONTINUATION_SECTION_PROJECTION_SPEC_TAG);
    hash_projection_spec(hasher, projection, plan);
}
