//! Test-only constructors and adapters for the parent module.

use super::*;

impl CacheEntryWeight {
    pub(in crate::db) fn for_tests(initial: usize, limit: usize) -> (Rc<Self>, Rc<Cell<usize>>) {
        let total = Rc::new(Cell::new(initial));
        let entry = Rc::new(Self {
            bytes: Cell::new(Some(initial)),
            total: Rc::downgrade(&total),
            limit,
        });
        (entry, total)
    }
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(in crate::db) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::db) fn retained_weight(&self) -> usize {
        self.retained_weight.get()
    }

    pub(in crate::db) fn retained_entries(&self) -> impl Iterator<Item = (&K, &V, usize)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key, &entry.value, entry.weight.bytes.get().unwrap_or(0)))
    }
}
