use crate::{
    db::predicate::{CompareOp, SchemaInfo, literal_matches_type},
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

pub(in crate::db::query::plan::planner) fn sorted_indexes(
    model: &EntityModel,
) -> Vec<&'static IndexModel> {
    let mut indexes = model.indexes.to_vec();
    indexes.sort_by(|left, right| left.name.cmp(right.name));

    indexes
}

pub(in crate::db::query::plan::planner) fn better_index(
    candidate: (usize, bool, &IndexModel),
    current: (usize, bool, &IndexModel),
) -> bool {
    let (cand_len, cand_exact, cand_index) = candidate;
    let (best_len, best_exact, best_index) = current;

    cand_len > best_len
        || (cand_len == best_len && cand_exact && !best_exact)
        || (cand_len == best_len && cand_exact == best_exact && cand_index.name < best_index.name)
}

pub(in crate::db::query::plan::planner) fn index_literal_matches_schema(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    let Some(field_type) = schema.field(field) else {
        return false;
    };
    if !literal_matches_type(value, field_type) {
        return false;
    }

    true
}

impl IndexModel {
    /// Return true when this index can structurally support the field/operator pair.
    #[must_use]
    pub(in crate::db::query::plan::planner) fn is_field_indexable(
        &self,
        field: &str,
        op: CompareOp,
    ) -> bool {
        if !self.fields.contains(&field) {
            return false;
        }

        matches!(
            op,
            CompareOp::Eq
                | CompareOp::In
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::StartsWith
        )
    }
}
