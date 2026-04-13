use crate::db::{
    query::intent::StructuralQuery,
    sql::parser::{SqlOrderDirection, SqlOrderTerm},
};

pub(super) fn apply_order_terms_structural(
    mut query: StructuralQuery,
    order_by: Vec<SqlOrderTerm>,
) -> StructuralQuery {
    for term in order_by {
        query = match term.direction {
            SqlOrderDirection::Asc => query.order_by(term.field),
            SqlOrderDirection::Desc => query.order_by_desc(term.field),
        };
    }

    query
}
