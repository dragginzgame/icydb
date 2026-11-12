//! Module: db::resource::bounded_cache
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
/// exceed the configured entry or retained-weight budget. Consumers may refresh
/// an admitted hit explicitly without replacing its value or weight.
///

pub(in crate::db) struct BoundedCache<K, V> {
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
    pub(in crate::db) fn new_weighted(max_entries: usize, max_retained_weight: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_entries,
            max_retained_weight,
            retained_weight: Rc::new(Cell::new(0)),
        }
    }

    pub(in crate::db) fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    /// Move an existing key to the back of the bounded eviction queue.
    /// Call only after consumer-specific reuse admission; misses leave order intact.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn refresh(&mut self, key: &K) {
        if let Some(position) = self.insertion_order.iter().position(|entry| entry == key)
            && let Some(key) = self.insertion_order.remove(position)
        {
            self.insertion_order.push_back(key);
        }
    }

    /// Borrow the admitted entry's accounting handle, never a previous entry's.
    /// Rejected insertions leave existing values, charges and FIFO order intact.
    pub(in crate::db) fn insert_weighted(
        &mut self,
        key: K,
        value: V,
        weight: usize,
    ) -> Option<&Rc<CacheEntryWeight>> {
        if self.max_entries == 0 || weight > self.max_retained_weight {
            return None;
        }

        if let Some(entry) = self.entries.remove(&key) {
            // Drop the old charge before reserving replacement space.
            entry.weight.release();
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
        let entry = self
            .entries
            .entry(key)
            .insert_entry(BoundedCacheEntry { value, weight })
            .into_mut();

        Some(&entry.weight)
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

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
