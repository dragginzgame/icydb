use icydb::{db::FluentLoadQuery, traits::Entity};

fn bounded_window_compiles<E>(query: FluentLoadQuery<'_, E>)
where
    E: Entity,
{
    let _ = query.bounded_window(1);
}

fn main() {}
