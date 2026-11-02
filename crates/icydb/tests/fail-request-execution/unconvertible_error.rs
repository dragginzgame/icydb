struct ApplicationError;

#[icydb::request_execution]
async fn entry() -> Result<(), ApplicationError> {
    Ok(())
}

fn main() {}
