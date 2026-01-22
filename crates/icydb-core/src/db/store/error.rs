///
/// StoreError
///

#[derive(Debug)]
pub enum StoreError {
    NotFound { key: String },
    Corrupt { message: String },
    InvariantViolation { message: String },
}
