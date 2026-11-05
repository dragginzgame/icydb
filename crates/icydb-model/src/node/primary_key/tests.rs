use super::{PrimaryKey, PrimaryKeySource};

#[test]
fn primary_key_keeps_ordered_field_list_without_scalarizing_composite_keys() {
    let primary_key = PrimaryKey::new(&["tenant_id", "local_id"], PrimaryKeySource::External);

    assert_eq!(primary_key.fields(), ["tenant_id", "local_id"]);
    assert_eq!(primary_key.scalar_field(), None);
    assert_eq!(primary_key.source(), PrimaryKeySource::External);
}

#[test]
fn scalar_primary_key_exposes_scalar_field() {
    let primary_key = PrimaryKey::new(&["id"], PrimaryKeySource::Internal);

    assert_eq!(primary_key.fields(), ["id"]);
    assert_eq!(primary_key.scalar_field(), Some("id"));
}
