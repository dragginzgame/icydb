use crate::db::{
    query::plan::expr::{Alias, Expr, FieldPath, ProjectionField},
    sql::lowering::select::projection::grouped_projection_is_canonical_identity,
};

#[test]
fn grouped_path_identity_preserves_exact_labels_and_alias_policy() {
    let mut fields = [ProjectionField::Scalar {
        expr: Expr::FieldPath(FieldPath::new("profile", vec!["city".into()])),
        alias: None,
    }];
    assert!(grouped_projection_is_canonical_identity(
        &fields,
        &["profile.city".into()],
    ));
    for label in ["profile", "profile.city.code", "profile.city."] {
        assert!(!grouped_projection_is_canonical_identity(
            &fields,
            &[label.into()],
        ));
    }
    let ProjectionField::Scalar { alias, .. } = &mut fields[0];
    *alias = Some(Alias::new("city"));
    assert!(!grouped_projection_is_canonical_identity(
        &fields,
        &["profile.city".into()],
    ));
}
