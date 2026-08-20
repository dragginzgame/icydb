use icydb::{
    db::{DbSession, TypedWriteError, WriteCell},
    traits::CanisterKind,
    types::{Id, Principal},
};
use icydb_model::prelude::*;

#[canister(
    memory_namespace = "typed_enrollment_example",
    memory_min = 230,
    memory_max = 232,
    commit_memory_id = 232,
    startup_memory_id = 231
)]
pub struct EnrollmentCanister {}

#[store(canister = "EnrollmentCanister", storage(heap()))]
pub struct EnrollmentStore {}

#[entity(
    store = "EnrollmentStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "display_name", value(item(prim = "Text", max_len = 64)))
    )
)]
pub struct User {}

#[entity(
    store = "EnrollmentStore",
    version = 1,
    pk(field = "authentication_principal"),
    index(field = "user_id"),
    index(fields = ["user_id", "authentication_principal"], unique),
    fields(
        field(name = "authentication_principal", value(item(prim = "Principal"))),
        field(name = "user_id", value(item(rel = "User", prim = "Ulid")))
    )
)]
pub struct UserPrincipal {}

#[entity(
    store = "EnrollmentStore",
    version = 1,
    pk(field = "id"),
    index(field = "user_id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "user_id", value(item(rel = "User", prim = "Ulid"))),
        field(name = "label", value(item(prim = "Text", max_len = 64)))
    )
)]
pub struct Robot {}

#[allow(dead_code)]
fn enroll<C: CanisterKind>(
    session: &DbSession<C>,
    principal: Principal,
) -> Result<Id<User>, TypedWriteError> {
    let user_id = Id::<User>::generate()
        .map_err(icydb::Error::from)
        .map_err(TypedWriteError::Database)?;
    let mut batch = session.trusted_typed_write_batch();
    let user = batch.push(UserInsert {
        id: WriteCell::Value(user_id),
        display_name: WriteCell::Value("Ada".to_string()),
    })?;
    let membership = batch.push(UserPrincipalInsert {
        authentication_principal: WriteCell::Value(Id::from_key(principal)),
        user_id: WriteCell::Value(user_id),
    })?;
    let robot = batch.push(RobotInsert {
        user_id: WriteCell::Value(user_id),
        label: WriteCell::Value("Ada's robot".to_string()),
    })?;

    let results = batch.execute()?;
    let _user_result = results.result(&user).map_err(TypedWriteError::from)?;
    let _membership_result = results.result(&membership).map_err(TypedWriteError::from)?;
    let _robot_result = results.result(&robot).map_err(TypedWriteError::from)?;
    Ok(user_id)
}

#[test]
fn typed_enrollment_example_compiles_without_sql() {
    let _ = core::mem::size_of::<UserInsert>();
}
