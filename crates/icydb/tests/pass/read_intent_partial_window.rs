use icydb::{db::FluentLoadQuery, traits::Entity};

fn partial_window_compiles<E>(query: FluentLoadQuery<'_, E>)
where
    E: Entity,
{
    let _ = query.partial_window(1);
}

fn main() {}
