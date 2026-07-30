use icydb::{
    db::{StructuralMutation, StructuralPatch, WriteCell},
    value::InputValue,
};

fn main() {
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

    let _ = batch;
}
