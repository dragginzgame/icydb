mod distinct;
mod helpers;
mod numeric;
mod projection;
mod terminals;

use crate::types::Id;

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;
