use icydb::{
    db::{DbSession, StructuralMutation, StructuralPatch, TypedOperationError, WriteCell},
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

#[enum_(
    variant(name = "Ready"),
    variant(name = "Weighted", value(item(prim = "Nat64")))
)]
pub struct EnrollmentState {}

#[record(fields(
    field(name = "label", value(item(prim = "Text", max_len = 64))),
    field(name = "state", value(item(is = "EnrollmentState"))),
    field(name = "note", value(opt, item(prim = "Text", max_len = 64)))
))]
pub struct EnrollmentProfile {}

#[list(item(is = "EnrollmentProfile"))]
pub struct EnrollmentProfiles {}

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

#[entity(
    store = "EnrollmentStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "profiles", value(item(is = "EnrollmentProfiles")))
    )
)]
pub struct ProfileOwner {}

#[allow(dead_code)]
fn enroll<C: CanisterKind>(
    session: &DbSession<C>,
    principal: Principal,
) -> Result<Id<User>, TypedOperationError> {
    let user_id = Id::<User>::generate()
        .map_err(icydb::Error::from)
        .map_err(TypedOperationError::Database)?;
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

    let mut results = batch.execute()?;
    let _user_row = results.row(&user)?;
    let _membership_row = results.row(&membership)?;
    let _robot_row = results.row(&robot)?;
    Ok(user_id)
}

#[allow(dead_code)]
fn generated_structural_input<C: CanisterKind>(
    session: &DbSession<C>,
) -> Result<(), TypedOperationError> {
    let binding = ProfileOwner::typed_binding(session)?;
    let profiles = EnrollmentProfiles(vec![
        EnrollmentProfile {
            label: "Ada".to_string(),
            state: EnrollmentState::Ready,
            note: None,
        },
        EnrollmentProfile {
            label: "Grace".to_string(),
            state: EnrollmentState::Weighted(7),
            note: Some("nested enum payload".to_string()),
        },
    ]);
    let profiles = session.bind_typed_input(&binding, profiles)?;
    let mutation = StructuralMutation::Insert {
        entity: ProfileOwner::ENTITY.to_string(),
        patch: StructuralPatch::new()
            .field(ProfileOwner::PROFILES.as_str(), WriteCell::Value(profiles)),
    };

    let _single = session.execute_trusted_structural_mutation(mutation.clone());
    let _batch = session.execute_trusted_structural_mutation_batch(vec![mutation.clone()]);
    let _bound_batch =
        session.execute_trusted_structural_mutation_batch_rows(&binding, vec![mutation]);
    Ok(())
}

#[test]
fn typed_enrollment_example_compiles_without_sql() {
    let _ = core::mem::size_of::<UserInsert>();
}
