use super::BoundedCache;

#[test]
fn retained_cache_lazy_reservation_rejects_overflow_and_releases_on_eviction() {
    let mut cache = BoundedCache::new_weighted(2, 10);
    let entry = cache
        .insert_weighted("a", 1, 4)
        .cloned()
        .expect("entry charge");
    assert!(entry.reserve(6));
    assert!(!entry.reserve(1));
    assert!(!entry.reserve(usize::MAX));
    assert_eq!(cache.retained_weight(), 10);
    cache.insert_weighted("b", 2, 4);
    assert!(cache.get(&"a").is_none());
    assert_eq!(cache.retained_weight(), 4);
    assert!(!entry.reserve(0));
    drop(entry);
    assert_eq!(cache.retained_weight(), 4);
}

#[test]
fn retained_cache_replacement_releases_lazy_charge_and_revokes_old_handle() {
    let mut cache = BoundedCache::new_weighted(2, 12);
    let old = cache
        .insert_weighted("a", 1, 3)
        .cloned()
        .expect("entry charge");
    assert!(old.reserve(7));
    let current = cache
        .insert_weighted("a", 2, 4)
        .cloned()
        .expect("replacement charge");
    assert_eq!(cache.get(&"a"), Some(&2));
    assert_eq!(cache.retained_weight(), 4);
    assert!(!old.reserve(1));
    assert!(current.reserve(8));
    assert_eq!(cache.retained_weight(), 12);
    drop(cache);
    assert!(!current.reserve(0));
}

#[test]
fn refreshed_cache_entry_keeps_its_charge_and_outlives_an_unused_entry() {
    for (entries, bytes) in [(2, 20), (10, 8)] {
        let mut cache = BoundedCache::new_weighted(entries, bytes);
        let charge = cache.insert_weighted("a", 1, 3).unwrap().clone();
        cache.insert_weighted("b", 2, 4);
        assert!(charge.reserve(1));
        cache.refresh(&"missing");
        cache.refresh(&"a");
        cache.refresh(&"a");
        assert_eq!(cache.retained_weight(), 8);
        assert_eq!(cache.get(&"a"), Some(&1));
        cache.insert_weighted("c", 3, 4);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.retained_weight(), 8);
        assert!(charge.reserve(0));
        cache.insert_weighted("d", 4, 4);
        assert_eq!(cache.get(&"a"), None);
        assert!(!charge.reserve(0));
    }
}

#[test]
fn bounded_cache_evicts_oldest_inserted_key() {
    let mut cache = BoundedCache::new_weighted(2, 10);

    cache.insert_weighted("a", 1, 1);
    cache.insert_weighted("b", 2, 1);
    cache.insert_weighted("c", 3, 1);

    assert!(cache.get(&"a").is_none());
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.len(), 2);
}

#[test]
fn weighted_cache_evicts_until_new_entry_fits() {
    let mut cache = BoundedCache::new_weighted(4, 10);

    cache.insert_weighted("a", 1, 4);
    cache.insert_weighted("b", 2, 4);
    cache.insert_weighted("c", 3, 4);

    assert!(cache.get(&"a").is_none());
    assert_eq!(cache.retained_weight(), 8);
}

#[test]
fn weighted_cache_rejects_single_oversize_entry() {
    let mut cache = BoundedCache::new_weighted(4, 10);

    cache.insert_weighted("a", 1, 11);

    assert!(cache.is_empty());
    assert_eq!(cache.retained_weight(), 0);
}

#[test]
fn weighted_cache_replacement_cannot_evict_itself() {
    let mut cache = BoundedCache::new_weighted(4, 10);
    cache.insert_weighted("a", 1, 4);
    cache.insert_weighted("b", 2, 4);

    let outcome = cache.insert_weighted("a", 3, 8);

    assert!(outcome.is_some());
    assert_eq!(cache.get(&"a"), Some(&3));
    assert!(cache.get(&"b").is_none());
    assert_eq!(cache.retained_weight(), 8);
}

#[test]
fn rejected_replacement_returns_no_handle_and_preserves_existing_charge_and_order() {
    let mut cache = BoundedCache::new_weighted(2, 10);
    let first = cache
        .insert_weighted("a", 1, 3)
        .cloned()
        .expect("first charge");
    cache.insert_weighted("b", 2, 3);

    assert!(cache.insert_weighted("a", 99, 11).is_none());
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.retained_weight(), 6);
    assert!(first.reserve(1));
    assert_eq!(cache.retained_weight(), 7);

    cache.insert_weighted("c", 3, 3);
    assert!(cache.get(&"a").is_none());
    assert!(!first.reserve(0));
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.retained_weight(), 6);
}

#[test]
fn disabled_cache_returns_no_accounting_handle() {
    let mut cache = BoundedCache::new_weighted(0, 10);
    assert!(cache.insert_weighted("a", 1, 1).is_none());
    assert!(cache.is_empty());
    assert_eq!(cache.retained_weight(), 0);
}
