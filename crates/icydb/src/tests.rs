use crate::build;

struct ModelWrapper(u64);

impl icydb_model::Inner<u64> for ModelWrapper {
    fn inner(&self) -> &u64 {
        &self.0
    }

    fn into_inner(self) -> u64 {
        self.0
    }
}

impl icydb_model::Path for ModelWrapper {
    const PATH: &'static str = "icydb::tests::ModelWrapper";
}

#[test]
fn build_facade_exports_typed_entrypoint() {
    fn assert_model_inner<T: crate::traits::Inner<u64>>() {}

    assert_model_inner::<ModelWrapper>();
    let wrapper = ModelWrapper(7);
    assert_eq!(*crate::traits::Inner::inner(&wrapper), 7);
    assert_eq!(crate::traits::Inner::into_inner(wrapper), 7);

    std::hint::black_box(
        build_facade_macros_resolve as fn() -> Result<(), Box<dyn std::error::Error>>,
    );
}

fn build_facade_macros_resolve() -> Result<(), Box<dyn std::error::Error>> {
    build::build_canister!(ModelWrapper)?;

    Ok(())
}
