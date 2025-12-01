pub mod family;
pub mod text;

#[cfg(test)]
pub mod tests;

// re-export the main entrypoint so callers can do:
//   coerce::coerce_basic(...)
pub use family::coerce_basic;
