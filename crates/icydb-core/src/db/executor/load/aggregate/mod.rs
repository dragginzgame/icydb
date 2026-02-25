mod contracts;
mod distinct;
mod fast_path;
mod helpers;
mod numeric;
mod orchestration;
mod projection;
mod terminals;

use crate::types::Id;
pub(in crate::db::executor) use contracts::{
    AggregateExecutionDescriptor, AggregateFastPathInputs,
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;
