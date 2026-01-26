mod unit;

pub use unit::WriteUnit;

#[cfg(test)]
pub use unit::{fail_checkpoint_label, fail_next_checkpoint};
