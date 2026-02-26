mod composite;
mod contracts;
mod order;

pub(crate) use composite::{IntersectOrderedKeyStream, MergeOrderedKeyStream};
pub(crate) use contracts::{
    BudgetedOrderedKeyStream, OrderedKeyStream, OrderedKeyStreamBox, VecOrderedKeyStream,
};
pub(crate) use order::KeyOrderComparator;

///
/// TESTS
///

#[cfg(test)]
mod tests;
