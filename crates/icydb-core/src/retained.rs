//! Module: retained
//! Responsibility: fallible accounting of cache-retained owned allocations.
//! Does not own: cache admission policy, execution budgets, or allocator telemetry.
//! Boundary: immutable resident owners report capacity; caches decide retention.

use std::{
    alloc::Layout,
    collections::BTreeMap,
    mem::{align_of, size_of, size_of_val},
    ops::Bound,
    rc::Rc,
    sync::{Arc, OnceLock},
};

/// Private owned-allocation contract. Inline storage is charged by the parent;
/// implementations enumerate every heap-owning field, including initialized cells.
pub(crate) trait Retained {
    const HAS_HEAP: bool = true;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()>;
}

/// Bounded traversal of retained payload capacity, excluding allocator bookkeeping.
/// Shared graphs are deliberately charged once per strong reference, conservatively.
pub(crate) struct RetainedBytes {
    bytes: usize,
    limit: usize,
    depth: usize,
}

impl RetainedBytes {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            bytes: 0,
            limit,
            depth: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn measure<T: Retained + ?Sized>(value: &T, limit: usize) -> Option<usize> {
        let mut bytes = Self::new(limit);
        bytes.add(size_of_val(value))?;
        bytes.visit(value)?;
        Some(bytes.bytes)
    }

    pub(crate) fn add(&mut self, amount: usize) -> Option<()> {
        self.bytes = self.bytes.checked_add(amount)?;
        (self.bytes <= self.limit).then_some(())
    }

    pub(crate) fn visit<T: Retained + ?Sized>(&mut self, value: &T) -> Option<()> {
        if !T::HAS_HEAP {
            return Some(());
        }
        // Cache ineligibility must not require an unbounded recursive walk.
        if self.depth >= 128 {
            return None;
        }
        self.depth += 1;
        let result = value.visit_retained(self);
        self.depth -= 1;
        result
    }

    pub(crate) const fn total(&self) -> usize {
        self.bytes
    }
}

macro_rules! retained_copy {
    ($($ty:ty),+ $(,)?) => { $(
        impl $crate::retained::Retained for $ty {
            const HAS_HEAP: bool = false;
            fn visit_retained(&self, _: &mut $crate::retained::RetainedBytes) -> Option<()> {
                // A representation change losing Copy cannot silently skip ownership.
                const fn require_copy<T: Copy>(_: &T) {}
                require_copy(self);
                Some(())
            }
        }
    )+ };
}

macro_rules! retained_fields {
    ($name:ident $(<$generic:ident>)? { $($(#[$arm_cfg:meta])* $pattern:pat => [$($(#[$field_cfg:meta])* $field:ident),* $(,)?]),* $(,)? }) => {
        impl $(<$generic: $crate::retained::Retained>)? $crate::retained::Retained for $name $(<$generic>)? {
            fn visit_retained(&self, bytes: &mut $crate::retained::RetainedBytes) -> Option<()> {
                let _ = &bytes;
                match self {
                    $($(#[$arm_cfg])* $pattern => {
                        $($(#[$field_cfg])* bytes.visit($field)?;)*
                        Some(())
                    }),*
                }
            }
        }
    };
}

pub(crate) use {retained_copy, retained_fields};

retained_copy!(
    (),
    bool,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    crate::types::Account,
    crate::types::Date,
    crate::types::Decimal,
    crate::types::Duration,
    crate::types::Float32,
    crate::types::Float64,
    crate::types::Principal,
    crate::types::Subaccount,
    crate::types::Timestamp,
    crate::types::U256,
    crate::types::Ulid,
    icydb_schema::ScalarKind
);

impl Retained for String {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.add(self.capacity())
    }
}

impl Retained for str {
    const HAS_HEAP: bool = false;
    fn visit_retained(&self, _: &mut RetainedBytes) -> Option<()> {
        Some(())
    }
}

impl<T: Retained> Retained for [T] {
    const HAS_HEAP: bool = T::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        for value in self {
            bytes.visit(value)?;
        }
        Some(())
    }
}

impl<T: Retained, const N: usize> Retained for [T; N] {
    const HAS_HEAP: bool = T::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.visit(self.as_slice())
    }
}

impl<T: Retained> Retained for Vec<T> {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.add(self.capacity().checked_mul(size_of::<T>())?)?;
        bytes.visit(self.as_slice())
    }
}

impl<T: Retained + ?Sized> Retained for Box<T> {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.add(size_of_val(self.as_ref()))?;
        bytes.visit(self.as_ref())
    }
}

// Rc and Arc both carry two pointer-width reference counts before their payload.
// Include alignment padding; allocator size classes/metadata remain outside this contract.
fn shared_allocation_bytes<T: ?Sized>(value: &T) -> Option<usize> {
    let (layout, _) = Layout::new::<[usize; 2]>()
        .extend(Layout::for_value(value))
        .ok()?;
    Some(layout.pad_to_align().size())
}

impl<T: Retained + ?Sized> Retained for Rc<T> {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.add(shared_allocation_bytes(self.as_ref())?)?;
        bytes.visit(self.as_ref())
    }
}

impl<T: Retained + ?Sized> Retained for Arc<T> {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.add(shared_allocation_bytes(self.as_ref())?)?;
        bytes.visit(self.as_ref())
    }
}

impl<T: ?Sized + 'static> Retained for &'static T {
    const HAS_HEAP: bool = false;
    fn visit_retained(&self, _: &mut RetainedBytes) -> Option<()> {
        Some(())
    }
}

impl<T: Retained> Retained for Option<T> {
    const HAS_HEAP: bool = T::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        if let Some(value) = self {
            bytes.visit(value)?;
        }
        Some(())
    }
}

impl<T: Retained> Retained for Bound<T> {
    const HAS_HEAP: bool = T::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        match self {
            Self::Included(value) | Self::Excluded(value) => bytes.visit(value),
            Self::Unbounded => Some(()),
        }
    }
}

impl<T: Retained> Retained for OnceLock<T> {
    const HAS_HEAP: bool = T::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        if let Some(value) = self.get() {
            bytes.visit(value)?;
        }
        Some(())
    }
}

impl<A: Retained, B: Retained> Retained for (A, B) {
    const HAS_HEAP: bool = A::HAS_HEAP || B::HAS_HEAP;
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        bytes.visit(&self.0)?;
        bytes.visit(&self.1)
    }
}

impl<K: Retained, V: Retained> Retained for BTreeMap<K, V> {
    fn visit_retained(&self, bytes: &mut RetainedBytes) -> Option<()> {
        // Rust 1.97 alloc/collections/btree/node.rs: 11 keys/values and at most
        // 12 child pointers per node. Charge an internal node per entry plus
        // one possibly empty retained root, including conservative padding.
        // Recheck this bound when changing the pinned Rust toolchain.
        let payload = size_of::<K>()
            .checked_add(size_of::<V>())?
            .checked_mul(11)?;
        let padding = align_of::<K>()
            .checked_add(align_of::<V>())?
            .checked_mul(4)?;
        let node = payload
            .checked_add(16 * size_of::<usize>())?
            .checked_add(padding)?;
        bytes.add(self.len().checked_add(1)?.checked_mul(node)?)?;
        for (key, value) in self {
            bytes.visit(key)?;
            bytes.visit(value)?;
        }
        Some(())
    }
}

impl Retained for crate::types::IntBig {
    fn visit_retained(&self, _: &mut RetainedBytes) -> Option<()> {
        // The external limb allocation does not expose its retained capacity.
        // Execution remains supported; this artifact cannot claim a byte bound.
        None
    }
}

impl Retained for crate::types::NatBig {
    fn visit_retained(&self, _: &mut RetainedBytes) -> Option<()> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::RetainedBytes;
    use crate::{types::IntBig, value::Value};
    use std::{rc::Rc, sync::OnceLock};

    #[test]
    fn retained_capacity_counts_spare_buffers_and_nested_copies() {
        let mut text = String::with_capacity(4096);
        text.push('x');
        let mut values = Vec::with_capacity(8);
        values.push(text);
        let expected =
            size_of_val(&values) + values.capacity() * size_of::<String>() + values[0].capacity();
        assert_eq!(RetainedBytes::measure(&values, expected), Some(expected));
        assert_eq!(RetainedBytes::measure(&values, expected - 1), None);
        let shared = Rc::new(values);
        let first = RetainedBytes::measure(&shared, usize::MAX).expect("known allocations");
        let copies = (shared.clone(), shared);
        assert_eq!(RetainedBytes::measure(&copies, usize::MAX), Some(first * 2));
    }

    #[test]
    fn retained_cells_charge_only_initialized_payloads() {
        let cell = OnceLock::new();
        let before = RetainedBytes::measure(&cell, usize::MAX).expect("empty cell");
        cell.set(vec![0_u8; 1024]).expect("initialize once");
        assert_eq!(
            RetainedBytes::measure(&cell, usize::MAX),
            Some(before + 1024)
        );
    }

    #[test]
    fn retained_unknown_or_excessively_deep_values_decline_accounting() {
        let value = Value::IntBig(IntBig::from(1_i64));
        assert_eq!(RetainedBytes::measure(&value, usize::MAX), None);
        let mut nested = Value::Null;
        for _ in 0..130 {
            nested = Value::List(vec![nested]);
        }
        assert_eq!(RetainedBytes::measure(&nested, usize::MAX), None);
        let mut bytes = RetainedBytes::new(usize::MAX);
        assert_eq!(bytes.add(usize::MAX), Some(()));
        assert_eq!(bytes.add(1), None);
    }
}
