use icydb::{
    db::{
        DbSession, StructuralMutation, StructuralPatch, TypedEntityBinding, TypedWrite, WriteCell,
    },
    traits::CanisterKind,
    value::InputValue,
};

#[allow(dead_code)]
fn structural_mutation_batch_compiles_without_sql<C>(
    db: &DbSession<C>,
    binding: &TypedEntityBinding,
) where
    C: CanisterKind,
{
    let patch = StructuralPatch::new()
        .field(
            "name",
            WriteCell::Value(InputValue::Text("Ada".to_string())),
        )
        .field("score", WriteCell::Default)
        .field("nickname", WriteCell::Null);

    let mutation = StructuralMutation::Insert {
        entity: "User".to_string(),
        patch,
    };

    let batch = vec![
        mutation,
        StructuralMutation::Delete {
            entity: "User".to_string(),
            key: InputValue::Nat64(1),
        },
    ];

    let _ = db.execute_trusted_structural_mutation_batch(batch.clone());
    let _ = db.execute_trusted_structural_mutation_batch_rows(binding, batch);
}

#[allow(dead_code)]
fn typed_write_terminals_compile_without_sql<C>(
    db: &DbSession<C>,
    write: TypedWrite,
    writes: Vec<TypedWrite>,
) where
    C: CanisterKind,
{
    let _ = db.execute_trusted_typed_write_row(write);
    let _ = db.execute_trusted_typed_write_batch(writes);
}

#[test]
fn public_structural_write_facade_compile_contract() {}
