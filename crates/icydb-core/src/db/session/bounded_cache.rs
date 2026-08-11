//! Module: db::session::bounded_cache
//! Responsibility: small bounded in-heap cache container for canister-lifetime session caches.
//! Does not own: cache key semantics, artifact compilation, or cache attribution.
//! Boundary: keeps global session caches from growing without limit.

use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

///
/// BoundedCache
///
/// FIFO-bounded map used by process-global session caches. It preserves O(1)
/// key lookup while evicting the oldest inserted key when a new key would
/// exceed the configured entry or retained-weight budget.
///

pub(in crate::db::session) struct BoundedCache<K, V> {
    entries: HashMap<K, BoundedCacheEntry<V>>,
    insertion_order: VecDeque<K>,
    max_entries: usize,
    max_retained_weight: usize,
    retained_weight: usize,
}

struct BoundedCacheEntry<V> {
    value: V,
    weight: usize,
}

/// Result of one weighted insertion into a bounded cache.
pub(in crate::db::session) struct BoundedCacheInsertOutcome<V> {
    pub(in crate::db::session) replaced: Option<V>,
    pub(in crate::db::session) evicted: usize,
    pub(in crate::db::session) rejected_oversize: bool,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(in crate::db::session) fn new(max_entries: usize) -> Self {
        Self::new_weighted(max_entries, usize::MAX)
    }

    pub(in crate::db::session) fn new_weighted(
        max_entries: usize,
        max_retained_weight: usize,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries,
            max_retained_weight,
            retained_weight: 0,
        }
    }

    pub(in crate::db::session) fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    pub(in crate::db::session) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries.get_mut(key).map(|entry| &mut entry.value)
    }

    pub(in crate::db::session) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert_weighted(key, value, 0).replaced
    }

    pub(in crate::db::session) fn insert_weighted(
        &mut self,
        key: K,
        value: V,
        weight: usize,
    ) -> BoundedCacheInsertOutcome<V> {
        if self.max_entries == 0 || weight > self.max_retained_weight {
            return BoundedCacheInsertOutcome {
                replaced: None,
                evicted: 0,
                rejected_oversize: true,
            };
        }

        if let Some(replaced) = self.entries.remove(&key) {
            self.retained_weight = self.retained_weight.saturating_sub(replaced.weight);
            self.insertion_order.retain(|existing| existing != &key);
            let evicted = self.evict_until_new_key_fits(weight);
            self.insertion_order.push_back(key.clone());
            self.retained_weight = self.retained_weight.saturating_add(weight);
            self.entries
                .insert(key, BoundedCacheEntry { value, weight });

            return BoundedCacheInsertOutcome {
                replaced: Some(replaced.value),
                evicted,
                rejected_oversize: false,
            };
        }

        let evicted = self.evict_until_new_key_fits(weight);
        self.insertion_order.push_back(key.clone());
        self.retained_weight = self.retained_weight.saturating_add(weight);
        self.entries
            .insert(key, BoundedCacheEntry { value, weight });

        BoundedCacheInsertOutcome {
            replaced: None,
            evicted,
            rejected_oversize: false,
        }
    }

    pub(in crate::db::session) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(in crate::db::session) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::db::session) fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    #[cfg(all(test, feature = "sql", feature = "diagnostics"))]
    pub(in crate::db::session) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values().map(|entry| &entry.value)
    }

    #[cfg(test)]
    pub(in crate::db::session) const fn retained_weight(&self) -> usize {
        self.retained_weight
    }

    fn evict_until_new_key_fits(&mut self, new_weight: usize) -> usize {
        let mut evicted = 0usize;
        while self.entries.len() >= self.max_entries
            || self.retained_weight.saturating_add(new_weight) > self.max_retained_weight
        {
            if !self.evict_oldest() {
                break;
            }
            evicted = evicted.saturating_add(1);
        }

        evicted
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self.insertion_order.pop_front() else {
            self.entries.clear();
            self.retained_weight = 0;
            return false;
        };
        if let Some(entry) = self.entries.remove(&oldest) {
            self.retained_weight = self.retained_weight.saturating_sub(entry.weight);
        }

        true
    }
}

impl<K, V> Default for BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedCache;

    #[test]
    fn bounded_cache_evicts_oldest_inserted_key() {
        let mut cache = BoundedCache::new(2);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

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
        let outcome = cache.insert_weighted("c", 3, 4);

        assert_eq!(outcome.evicted, 1);
        assert!(cache.get(&"a").is_none());
        assert_eq!(cache.retained_weight(), 8);
    }

    #[test]
    fn weighted_cache_rejects_single_oversize_entry() {
        let mut cache = BoundedCache::new_weighted(4, 10);

        let outcome = cache.insert_weighted("a", 1, 11);

        assert!(outcome.rejected_oversize);
        assert!(cache.is_empty());
        assert_eq!(cache.retained_weight(), 0);
    }

    #[test]
    fn weighted_cache_replacement_cannot_evict_itself() {
        let mut cache = BoundedCache::new_weighted(4, 10);
        cache.insert_weighted("a", 1, 4);
        cache.insert_weighted("b", 2, 4);

        let outcome = cache.insert_weighted("a", 3, 8);

        assert_eq!(outcome.replaced, Some(1));
        assert_eq!(outcome.evicted, 1);
        assert_eq!(cache.get(&"a"), Some(&3));
        assert!(cache.get(&"b").is_none());
        assert_eq!(cache.retained_weight(), 8);
    }
}
