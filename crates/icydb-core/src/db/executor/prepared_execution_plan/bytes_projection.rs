use crate::{
    db::{
        access::AccessPlan,
        executor::prepared_execution_plan::contracts::{
            OrderSpec, constant_covering_projection_value_from_access,
            covering_index_projection_facts,
        },
        predicate::MissingRowPolicy,
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

impl BytesByProjectionMode {
    /// Return a stable explain/diagnostic label for this bytes-by mode.
    #[must_use]
    pub(in crate::db::executor) const fn label(self) -> &'static str {
        match self {
            Self::Materialized => "field_materialized",
            Self::CoveringIndex => "field_covering_index",
            Self::CoveringConstant => "field_covering_constant",
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_index_only(self) -> bool {
        matches!(self, Self::CoveringIndex | Self::CoveringConstant)
    }
}

/// Classify canonical `bytes_by(field)` execution mode from one neutral access context.
#[must_use]
pub(in crate::db::executor) fn classify_bytes_by_projection_mode(
    access: &AccessPlan<Value>,
    order_spec: Option<&OrderSpec>,
    consistency: MissingRowPolicy,
    has_predicate: bool,
    target_field: &str,
    primary_key_names: &[&str],
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

    if covering_index_projection_facts(access, order_spec, target_field, primary_key_names)
        .is_some()
    {
        return BytesByProjectionMode::CoveringIndex;
    }

    BytesByProjectionMode::Materialized
}
