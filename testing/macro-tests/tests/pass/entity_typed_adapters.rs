use icydb::{
    db::{
        OutputRow, TypedEntityBinding, TypedRowAdapter, TypedWriteAdapter, WriteCell,
    },
    design::prelude::*,
};

#[canister(
    memory_namespace = "typed_adapter",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110
)]
pub struct TypedAdapterCanister {}

#[store(
    ident = "TYPED_ADAPTER_STORE",
    store_name = "typed_adapter",
    canister = "TypedAdapterCanister",
    storage(heap())
)]
pub struct TypedAdapterStore {}

#[record(
    source_key = "type/typed_adapter/profile",
    fields(field(
        source_key = "display_name",
        ident = "display_name",
        value(item(prim = "Text", max_len = 64))
    ))
)]
pub struct AdapterProfile {}

#[enum_(
    source_key = "type/typed_adapter/status",
    variant(source_key = "active", ident = "Active"),
    variant(source_key = "disabled", ident = "Disabled")
)]
pub struct AdapterStatus {}

#[entity(
    source_key = "entity/typed_adapter",
    store = "TypedAdapterStore",
    version = 1,
    pk(fields = ["id"]),
    typed_adapters,
    audit_timestamps(
        created_at(source_key = "audit/created", ident = "created_at"),
        updated_at(source_key = "audit/updated", ident = "updated_at")
    ),
    fields(
        field(
            source_key = "id",
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(
            source_key = "name",
            ident = "name",
            value(item(prim = "Text", max_len = 64))
        ),
        field(
            source_key = "nickname",
            ident = "nickname",
            value(opt, item(prim = "Text", max_len = 64))
        ),
        field(
            source_key = "tags",
            ident = "tags",
            value(many, item(prim = "Text", max_len = 32))
        ),
        field(
            source_key = "status",
            ident = "status",
            value(item(is = "AdapterStatus"))
        ),
        field(
            source_key = "profile",
            ident = "profile",
            value(item(is = "AdapterProfile"))
        )
    )
)]
pub struct AdapterEntity {}

fn proves_generated_adapter_surface(
    id: Ulid,
    binding: &TypedEntityBinding,
    row: OutputRow,
) {
    let insert = AdapterEntityInsert {
        name: WriteCell::Value("Ada".to_string()),
        nickname: WriteCell::Null,
        tags: WriteCell::Value(vec!["admin".to_string()]),
        status: WriteCell::Value(AdapterStatus::Active),
        profile: WriteCell::Value(AdapterProfile {
            display_name: "Ada".to_string(),
        }),
    };
    let patch = AdapterEntityPatch {
        id,
        name: WriteCell::Omitted,
        nickname: WriteCell::Default,
        tags: WriteCell::Value(Vec::new()),
        status: WriteCell::Value(AdapterStatus::Disabled),
        profile: WriteCell::Omitted,
    };
    let replace = AdapterEntityReplace {
        id,
        name: WriteCell::Value("Bea".to_string()),
        nickname: WriteCell::Null,
        tags: WriteCell::Omitted,
        status: WriteCell::Value(AdapterStatus::Active),
        profile: WriteCell::Value(AdapterProfile {
            display_name: "Bea".to_string(),
        }),
    };

    let _ = insert.encode_write(binding);
    let _ = patch.encode_write(binding);
    let _ = replace.encode_write(binding);
    let _ = AdapterEntity::decode_row(binding, row);
}

fn main() {}
