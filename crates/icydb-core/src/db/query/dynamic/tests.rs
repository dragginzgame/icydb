use super::{DynamicQuery, FilterExpr};

#[test]
fn repeated_filters_accumulate_as_one_conjunction() {
    let owner = FilterExpr::eq("owner_id", 1_u64);
    let slot = FilterExpr::eq("slot", 2_u64);
    let query = DynamicQuery::new("InventoryStack")
        .filter(owner.clone())
        .filter(slot.clone());

    assert_eq!(
        query.filter_expr(),
        Some(&FilterExpr::and(vec![owner, slot]))
    );
}
