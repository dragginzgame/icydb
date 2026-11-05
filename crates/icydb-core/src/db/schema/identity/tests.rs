use super::*;

#[test]
fn constraint_allocator_preserves_reserved_high_water_identity() {
    let empty = ConstraintIdAllocator::default();
    let reserved = ConstraintIdAllocator::new(7);

    assert_eq!(empty.high_water(), 0);
    assert_eq!(reserved.high_water(), 7);
}

#[test]
fn constraint_allocator_reserves_monotonically_and_fails_closed_at_exhaustion() {
    let (first, first_id) = ConstraintIdAllocator::default()
        .checked_reserve()
        .expect("empty allocator should reserve its first identity");
    let (second, second_id) = first
        .checked_reserve()
        .expect("allocator should reserve its next identity");

    assert_eq!(first_id.get(), 1);
    assert_eq!(second_id.get(), 2);
    assert_eq!(second.high_water(), 2);
    assert_eq!(ConstraintIdAllocator::new(u32::MAX).checked_reserve(), None,);
}

#[test]
fn relation_allocator_reserves_monotonically_and_fails_closed_at_exhaustion() {
    let (first, first_id) = RelationIdAllocator::default()
        .checked_reserve()
        .expect("empty allocator should reserve its first identity");
    let (second, second_id) = first
        .checked_reserve()
        .expect("allocator should reserve its next identity");

    assert_eq!(first_id.get(), 1);
    assert_eq!(second_id.get(), 2);
    assert_eq!(second.high_water(), 2);
    assert_eq!(RelationIdAllocator::new(u32::MAX).checked_reserve(), None);
}

#[test]
fn logical_structural_ids_reject_zero() {
    assert_eq!(SchemaIndexId::new(0), None);
    assert_eq!(RelationId::new(0), None);
}
