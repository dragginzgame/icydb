//! Module: db::session::bounded_cache
//! Responsibility: small bounded in-heap cache container for canister-lifetime session caches.
//! Does not own: cache key semantics or artifact compilation.
//! Boundary: keeps global session caches from growing without limit.

use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
    hash::Hash,
    rc::{Rc, Weak},
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
    retained_weight: Rc<Cell<usize>>,
}

struct BoundedCacheEntry<V> {
    value: V,
    weight: Rc<CacheEntryWeight>,
}

/// Entry-lifetime charge. Plans hold only a weak accounting handle, so eviction
/// releases the budget even when an execution still owns the prepared plan.
#[derive(Debug)]
pub(in crate::db) struct CacheEntryWeight {
    bytes: Cell<Option<usize>>,
    total: Weak<Cell<usize>>,
    limit: usize,
}

impl CacheEntryWeight {
    #[cfg(test)]
    pub(in crate::db) fn for_tests(initial: usize, limit: usize) -> (Rc<Self>, Rc<Cell<usize>>) {
        let total = Rc::new(Cell::new(initial));
        let entry = Rc::new(Self {
            bytes: Cell::new(Some(initial)),
            total: Rc::downgrade(&total),
            limit,
        });
        (entry, total)
    }
    /// Reserve before attaching another lazy resident; failure declines caching,
    /// never execution. No cache-map borrow is needed during plan preparation.
    pub(in crate::db) fn reserve(&self, bytes: usize) -> bool {
        let Some(total) = self.total.upgrade() else {
            return false;
        };
        let Some(next_total) = total.get().checked_add(bytes) else {
            return false;
        };
        let Some(current) = self.bytes.get() else {
            return false;
        };
        let Some(next_entry) = current.checked_add(bytes) else {
            return false;
        };
        if next_total > self.limit {
            return false;
        }
        total.set(next_total);
        self.bytes.set(Some(next_entry));
        true
    }

    pub(in crate::db) fn remaining(&self) -> usize {
        if self.bytes.get().is_none() {
            return 0;
        }
        self.total
            .upgrade()
            .map_or(0, |total| self.limit.saturating_sub(total.get()))
    }

    fn release(&self) {
        if let Some(bytes) = self.bytes.take()
            && let Some(total) = self.total.upgrade()
        {
            total.set(total.get().saturating_sub(bytes));
        }
    }
}

impl Drop for CacheEntryWeight {
    fn drop(&mut self) {
        self.release();
    }
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
            retained_weight: Rc::new(Cell::new(0)),
        }
    }

    pub(in crate::db::session) fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    pub(in crate::db::session) fn entry_weight(&self, key: &K) -> Option<Rc<CacheEntryWeight>> {
        self.entries.get(key).map(|entry| Rc::clone(&entry.weight))
    }

    pub(in crate::db::session) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert_weighted(key, value, 0)
    }

    pub(in crate::db::session) fn insert_weighted(
        &mut self,
        key: K,
        value: V,
        weight: usize,
    ) -> Option<V> {
        if self.max_entries == 0 || weight > self.max_retained_weight {
            return None;
        }

        let replaced = self.entries.remove(&key).map(|entry| {
            // Drop the old charge before reserving replacement space.
            entry.weight.release();
            entry.value
        });
        if replaced.is_some() {
            self.insertion_order.retain(|existing| existing != &key);
        }
        self.evict_until_new_key_fits(weight);
        self.insertion_order.push_back(key.clone());
        self.retained_weight
            .set(self.retained_weight.get().saturating_add(weight));
        let weight = Rc::new(CacheEntryWeight {
            bytes: Cell::new(Some(weight)),
            total: Rc::downgrade(&self.retained_weight),
            limit: self.max_retained_weight,
        });
        self.entries
            .insert(key, BoundedCacheEntry { value, weight });

        replaced
    }

    #[cfg(test)]
    pub(in crate::db::session) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(in crate::db::session) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(in crate::db::session) fn retained_weight(&self) -> usize {
        self.retained_weight.get()
    }

    #[cfg(test)]
    pub(in crate::db::session) fn retained_entries(&self) -> impl Iterator<Item = (&K, &V, usize)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key, &entry.value, entry.weight.bytes.get().unwrap_or(0)))
    }

    fn evict_until_new_key_fits(&mut self, new_weight: usize) {
        while self.entries.len() >= self.max_entries
            || self.retained_weight.get().saturating_add(new_weight) > self.max_retained_weight
        {
            if !self.evict_oldest() {
                break;
            }
        }
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self.insertion_order.pop_front() else {
            for entry in self.entries.values() {
                entry.weight.release();
            }
            self.entries.clear();
            return false;
        };
        if let Some(entry) = self.entries.remove(&oldest) {
            entry.weight.release();
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
    fn retained_cache_lazy_reservation_rejects_overflow_and_releases_on_eviction() {
        let mut cache = BoundedCache::new_weighted(2, 10);
        cache.insert_weighted("a", 1, 4);
        let entry = cache.entry_weight(&"a").expect("entry charge");
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
        cache.insert_weighted("a", 1, 3);
        let old = cache.entry_weight(&"a").expect("entry charge");
        assert!(old.reserve(7));
        assert_eq!(cache.insert_weighted("a", 2, 4), Some(1));
        assert_eq!(cache.retained_weight(), 4);
        assert!(!old.reserve(1));
        let current = cache.entry_weight(&"a").expect("replacement charge");
        assert!(current.reserve(8));
        assert_eq!(cache.retained_weight(), 12);
        drop(cache);
        assert!(!current.reserve(0));
    }

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

        assert_eq!(outcome, Some(1));
        assert_eq!(cache.get(&"a"), Some(&3));
        assert!(cache.get(&"b").is_none());
        assert_eq!(cache.retained_weight(), 8);
    }
}
