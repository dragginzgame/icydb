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
/// exceed the configured entry budget.
///

pub(in crate::db::session) struct BoundedCache<K, V> {
    entries: HashMap<K, V>,
    insertion_order: VecDeque<K>,
    max_entries: usize,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(in crate::db::session) fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries,
        }
    }

    pub(in crate::db::session) fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key)
    }

    pub(in crate::db::session) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.entries.contains_key(&key) {
            return self.entries.insert(key, value);
        }

        self.evict_until_new_key_fits();
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, value)
    }

    #[cfg(test)]
    pub(in crate::db::session) fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
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

    fn evict_until_new_key_fits(&mut self) {
        if self.max_entries == 0 {
            self.entries.clear();
            self.insertion_order.clear();
            return;
        }

        while self.entries.len() >= self.max_entries {
            let Some(oldest) = self.insertion_order.pop_front() else {
                self.entries.clear();
                return;
            };
            self.entries.remove(&oldest);
        }
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
}
