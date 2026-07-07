use icydb::{db::FluentLoadQuery, traits::Entity};

fn legacy_load_helpers_are_removed<E>(query: FluentLoadQuery<'_, E>)
where
    E: Entity,
{
    let _ = query.count();
    let _ = query.sum_by("amount");
    let _ = query.values_by("amount");
    let _ = query.top_k_by("amount", 10);
    let _ = query.all();
}

fn main() {}
