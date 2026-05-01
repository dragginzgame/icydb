use crate::{
    db::{
        access::AccessPlan,
        predicate::MissingRowPolicy,
        query::plan::{
            OrderSpec, constant_covering_projection_value_from_access,
            covering_index_projection_context,
        },
    },
    value::Value,
};

///
/// BytesByProjectionMode
///
/// Canonical `bytes_by(field)` projection mode classification used by execution
/// and explain surfaces. Keeping the policy in this module avoids scattering
/// covering-vs-materialized byte projection decisions across route adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum BytesByProjectionMode {
    Materialized,
    CoveringIndex,
    CoveringConstant,
}

/// Classify canonical `bytes_by(field)` execution mode from one neutral access context.
#[must_use]
pub(in crate::db::executor) fn classify_bytes_by_projection_mode(
    access: &AccessPlan<Value>,
    order_spec: Option<&OrderSpec>,
    consistency: MissingRowPolicy,
    has_predicate: bool,
    target_field: &str,
    primary_key_name: &'static str,
) -> BytesByProjectionMode {
    if !matches!(consistency, MissingRowPolicy::Ignore) {
        return BytesByProjectionMode::Materialized;
    }

    if constant_covering_projection_value_from_access(access, target_field).is_some() {
        return BytesByProjectionMode::CoveringConstant;
    }

    if has_predicate {
        return BytesByProjectionMode::Materialized;
    }

    if covering_index_projection_context(access, order_spec, target_field, primary_key_name)
        .is_some()
    {
        return BytesByProjectionMode::CoveringIndex;
    }

    BytesByProjectionMode::Materialized
}
