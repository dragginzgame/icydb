use icydb_core::{db::FluentLoadQuery, traits::EntityKind};

fn assert_removed_group_methods<E: EntityKind>(query: FluentLoadQuery<'_, E>) {
    let _ = query.group_count_distinct_by("rank");
}

fn main() {}
