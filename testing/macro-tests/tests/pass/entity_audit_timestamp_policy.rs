use icydb::design::prelude::*;

#[canister(
    memory_namespace = "no_audit",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110
)]
pub struct NoAuditCanister {}

#[store(
    ident = "NO_AUDIT_STORE",
    store_name = "no_audit",
    canister = "NoAuditCanister",
    storage(heap())
)]
pub struct NoAuditStore {}

#[entity(
    source_key = "entity/no_audit",
    store = "NoAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(
        source_key = "id",
        ident = "id",
        value(item(prim = "Nat64"))
    ))
)]
pub struct NoAuditEntity {}

#[entity(
    source_key = "entity/explicit_audit",
    store = "NoAuditStore",
    version = 1,
    pk(fields = ["id"]),
    audit_timestamps(
        created_at(source_key = "audit/created", ident = "created_on"),
        updated_at(source_key = "audit/updated", ident = "updated_on")
    ),
    fields(field(
        source_key = "id",
        ident = "id",
        value(item(prim = "Nat64"))
    ))
)]
pub struct ExplicitAuditEntity {}

fn proves_exact_field_surface(NoAuditEntity { id }: NoAuditEntity) {
    let _ = id;
}

fn proves_explicit_field_surface(
    ExplicitAuditEntity {
        id,
        created_on,
        updated_on,
    }: ExplicitAuditEntity,
) {
    let _ = (id, created_on, updated_on);
}

fn main() {}
