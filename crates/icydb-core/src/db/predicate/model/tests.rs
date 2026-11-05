use super::*;

#[test]
fn borrowed_field_visit_stops_at_the_original_error() {
    let predicate = Predicate::And(vec![
        Predicate::eq("first".into(), Value::Text("not_a_field".into())),
        Predicate::Not(Box::new(Predicate::IsNull {
            field: "stop".into(),
        })),
        Predicate::IsEmpty {
            field: "unvisited".into(),
        },
    ]);
    let mut visited = Vec::new();
    let result = predicate.try_for_each_field(&mut |field| {
        visited.push(field.to_string());
        if field == "stop" { Err(7_u8) } else { Ok(()) }
    });
    assert_eq!(result, Err(7));
    assert_eq!(visited, ["first", "stop"]);
}

#[test]
fn compare_predicate_builders_preserve_operator_shape() {
    assert_eq!(
        Predicate::gt("age".to_string(), Value::Nat64(7)),
        Predicate::Compare(ComparePredicate::gt("age".to_string(), Value::Nat64(7))),
    );
}
