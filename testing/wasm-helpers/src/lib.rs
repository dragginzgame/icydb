//! Shared helpers for wasm fixture schema and canister builds.

use icydb::{
    db::{DbSession, DynamicQuery, TypedEntityAdapter, TypedWrite, TypedWriteAdapter},
    traits::{CanisterKind, EntityKey, EntitySource},
    types::{Id, Ulid},
    value::InputValue,
};

/// Execute the maintained operation mix that makes one generated entity's
/// read and write adapters reachable in audit Wasm.
///
/// The operation selector is deliberately runtime-controlled so the optimizer
/// cannot remove any selected path. The harness measures reachability and code
/// shape; it is not an application mutation API.
#[must_use]
#[inline(never)]
pub fn execute_reachable_entity_operation<C, E, I, P>(
    session: &DbSession<C>,
    operation: u8,
    insert: I,
    patch: P,
    batch_first: I,
    batch_second: I,
) -> u32
where
    C: CanisterKind,
    E: EntityKey<Key = Ulid> + EntitySource + TypedEntityAdapter,
    E::Row: Clone,
    I: TypedWriteAdapter<Entity = E>,
    P: TypedWriteAdapter<Entity = E>,
{
    let succeeded = match operation {
        0 => execute_typed_page::<C, E>(session),
        1 => session.get_many::<E>(&[Id::from_key(Ulid::MIN)]).is_ok(),
        2 => execute_single_typed_write::<C, E, I>(session, insert),
        3 => execute_single_typed_write::<C, E, P>(session, patch),
        4 => execute_typed_write_batch::<C, E, I>(session, batch_first, batch_second),
        5 => execute_typed_delete::<C, E>(session),
        _ => false,
    };
    u32::from(succeeded)
}

#[inline(never)]
fn execute_typed_page<C, E>(session: &DbSession<C>) -> bool
where
    C: CanisterKind,
    E: EntitySource + TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(session) else {
        return false;
    };
    let request = DynamicQuery::new(E::ENTITY).limit(1);
    let mut cursor = session.prepare_live_page_cursor(binding, request);
    let Ok(Some(rows)) = cursor.next_trusted_page() else {
        return false;
    };
    rows.into_iter()
        .all(|row| E::decode_row(cursor.binding(), row).is_ok())
}

#[inline(never)]
fn execute_single_typed_write<C, E, W>(session: &DbSession<C>, input: W) -> bool
where
    C: CanisterKind,
    E: TypedEntityAdapter,
    W: TypedWriteAdapter<Entity = E>,
{
    let Ok(binding) = E::typed_binding(session) else {
        return false;
    };
    let Ok(write) = input.encode_write(&binding) else {
        return false;
    };
    let Ok(row) = session.execute_trusted_typed_write_row(write) else {
        return false;
    };
    E::decode_row(&binding, row).is_ok()
}

#[inline(never)]
fn execute_typed_write_batch<C, E, W>(session: &DbSession<C>, first: W, second: W) -> bool
where
    C: CanisterKind,
    E: TypedEntityAdapter,
    W: TypedWriteAdapter<Entity = E>,
{
    let Ok(binding) = E::typed_binding(session) else {
        return false;
    };
    let Ok(first) = first.encode_write(&binding) else {
        return false;
    };
    let Ok(second) = second.encode_write(&binding) else {
        return false;
    };
    let Ok(mut rows) =
        session.execute_trusted_typed_write_batch_rows(&binding, vec![first, second])
    else {
        return false;
    };
    rows.all(|row| E::decode_row(&binding, row).is_ok())
}

#[inline(never)]
fn execute_typed_delete<C, E>(session: &DbSession<C>) -> bool
where
    C: CanisterKind,
    E: EntityKey<Key = Ulid> + EntitySource + TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(session) else {
        return false;
    };
    let write = TypedWrite::delete(&binding, InputValue::from(Ulid::MIN));
    let Ok(mut rows) = session.execute_trusted_typed_write_batch_rows(&binding, vec![write]) else {
        return false;
    };
    rows.all(|row| E::decode_row(&binding, row).is_ok())
}

/// Invoke [`execute_reachable_entity_operation`] with the shared simple audit
/// entity input shape.
#[macro_export]
macro_rules! execute_simple_reachable_entity_operation {
    ($session:expr, $operation:expr, $entity:ty, $insert:ident, $patch:ident $(,)?) => {
        $crate::execute_reachable_entity_operation::<_, $entity, $insert, $patch>(
            $session,
            $operation,
            $insert {
                name: ::icydb::db::WriteCell::Value("single-insert".to_string()),
            },
            $patch {
                id: ::icydb::types::Id::<$entity>::from_key(::icydb::types::Ulid::MIN),
                name: ::icydb::db::WriteCell::Value("single-update".to_string()),
            },
            $insert {
                name: ::icydb::db::WriteCell::Value("batch-first".to_string()),
            },
            $insert {
                name: ::icydb::db::WriteCell::Value("batch-second".to_string()),
            },
        )
    };
}

///
/// define_fixture_canister
///
/// Generate the repeated canister declaration used by wasm fixture schema
/// crates.
///
/// `memory_min`, `memory_max`, and `commit_memory_id` are canister-level
/// stable-memory manager configuration. The maximum ID is reserved for
/// integrity progress; per-store memory IDs live in
/// `define_fixture_store!(Store, canister = "...", storage(...))`.
///
#[macro_export]
macro_rules! define_fixture_canister {
    (
        $canister:ident = $canister_name:literal,
        namespace = $namespace:literal,
        memory_min = $memory_min:literal,
        memory_max = $memory_max:literal,
        commit_memory_id = $commit_memory_id:literal,
        startup_memory_id = $startup_memory_id:literal,
        integrity_progress_memory_id = $integrity_progress_memory_id:literal
        $(, migrations($($migrations:tt)*))?
        $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($canister)]
        #[doc = ""]
        #[doc = "Canister model used by wasm SQL fixtures."]
        #[doc = ""]
        #[canister(
            memory_namespace = $namespace,
            memory_min = $memory_min,
            memory_max = $memory_max,
            commit_memory_id = $commit_memory_id,
            startup_memory_id = $startup_memory_id,
            integrity_progress_memory_id = $integrity_progress_memory_id
            $(, migrations($($migrations)*))?
        )]
        pub struct $canister {}
    };
}

///
/// define_fixture_store
///
/// Generate the repeated store declaration used by wasm fixture schema crates.
///
#[macro_export]
macro_rules! define_fixture_store {
    (
        $store:ident,
        canister = $canister_name:literal,
        storage(journaled(
            data_memory_id = $data_memory_id:literal,
            index_memory_id = $index_memory_id:literal,
            schema_memory_id = $schema_memory_id:literal,
            journal_memory_id = $journal_memory_id:literal,
        )) $(,)?
    ) => {
        #[doc = ""]
        #[doc = stringify!($store)]
        #[doc = ""]
        #[doc = "Main store model used by wasm SQL fixtures."]
        #[doc = ""]
        #[store(canister = $canister_name, storage(journaled(data_memory_id = $data_memory_id, index_memory_id = $index_memory_id, schema_memory_id = $schema_memory_id, journal_memory_id = $journal_memory_id)))]
        pub struct $store {}
    };
}

///
/// define_simple_audit_entities
///
/// Generate one or more repeated simple audit entities for wasm-size fixtures.
///
#[macro_export]
macro_rules! define_simple_audit_entities {
    ($store:literal; $($entity:ident),+ $(,)?) => {
        $(
            #[doc = ""]
            #[doc = stringify!($entity)]
            #[doc = ""]
            #[doc = "Repeated simple audit entity used to measure base per-entity wasm cost."]
            #[doc = ""]
            #[entity(
                store = $store,
                version = 1,
                pk(fields = ["id"]),
                fields(
                    field(name = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
                    field(name = "name", value(item(prim = "Text", unbounded)))
                ),
                timestamps
            )]
            pub struct $entity {}
        )+
    };
}
