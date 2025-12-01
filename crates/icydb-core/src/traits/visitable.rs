use crate::{
    traits::{Sanitize, Validate},
    visitor::{PathSegment, Visitor, VisitorMut, perform_visit, perform_visit_mut},
};
use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
};

///
/// Visitable
///

pub trait Visitable: Sanitize + Validate {
    fn drive(&self, _: &mut dyn Visitor) {}
    fn drive_mut(&mut self, _: &mut dyn VisitorMut) {}
}

impl<T: Visitable> Visitable for Option<T> {
    fn drive(&self, visitor: &mut dyn Visitor) {
        if let Some(value) = self {
            perform_visit(visitor, value, PathSegment::Empty);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMut) {
        if let Some(value) = self {
            perform_visit_mut(visitor, value, PathSegment::Empty);
        }
    }
}

impl<T: Visitable> Visitable for Vec<T> {
    fn drive(&self, visitor: &mut dyn Visitor) {
        for (i, value) in self.iter().enumerate() {
            perform_visit(visitor, value, i);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMut) {
        for (i, value) in self.iter_mut().enumerate() {
            perform_visit_mut(visitor, value, i);
        }
    }
}

impl<T: Visitable> Visitable for Box<T> {
    fn drive(&self, visitor: &mut dyn Visitor) {
        // A Box is just a heap-allocated wrapper.
        // Delegate directly to the inner value.
        (**self).drive(visitor);
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMut) {
        // Same logic as `drive`, but with mutable access.
        (**self).drive_mut(visitor);
    }
}

impl<T, S> Visitable for HashSet<T, S>
where
    T: Visitable + Eq + std::hash::Hash,
    S: BuildHasher + Default,
{
    fn drive(&self, visitor: &mut dyn Visitor) {
        // Sets don’t have stable ordering or indices.
        // We still traverse all items, but don’t add a path segment.
        for item in self {
            perform_visit(visitor, item, PathSegment::Empty);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMut) {
        // Drain moves items out so we can mutate them safely
        // (keys in a set are values, so they can change).
        let mut new_set = Self::with_hasher(S::default());

        for mut item in self.drain() {
            perform_visit_mut(visitor, &mut item, PathSegment::Empty);
            new_set.insert(item);
        }

        *self = new_set;
    }
}

impl<K, V, S> Visitable for HashMap<K, V, S>
where
    K: Visitable + Eq + std::hash::Hash,
    V: Visitable,
    S: BuildHasher + Default,
{
    fn drive(&self, visitor: &mut dyn Visitor) {
        // Traverse both key and value with explicit path segments
        for (k, v) in self {
            perform_visit(visitor, k, "key");
            perform_visit(visitor, v, "value");
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMut) {
        // Drain ensures we can mutate keys safely without UB.
        // We rebuild the map after visiting keys + values.
        let mut new_map = Self::with_hasher(S::default());

        for (mut k, mut v) in self.drain() {
            perform_visit_mut(visitor, &mut k, "key");
            perform_visit_mut(visitor, &mut v, "value");
            new_map.insert(k, v);
        }

        *self = new_map;
    }
}

impl_primitive!(Visitable);
