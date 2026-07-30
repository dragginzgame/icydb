use icydb::{
    db::{DbSession, StructuralMutation, StructuralPatch, WriteCell},
    traits::CanisterKind,
    value::InputValue,
};

fn structural_mutation_batch_compiles_without_sql<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let patch = StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::Text("Ada".to_string())))
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

    let _ = db.execute_trusted_structural_mutation_batch(batch);
}

fn main() {}
