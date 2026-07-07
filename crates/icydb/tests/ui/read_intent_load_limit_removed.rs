use icydb::{db::FluentLoadQuery, traits::Entity};

fn public_load_limit_is_removed<E>(query: FluentLoadQuery<'_, E>)
where
    E: Entity,
{
    let _ = query.limit(1);
}

fn main() {}
