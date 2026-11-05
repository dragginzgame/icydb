use super::{Error, build::BuildError, error::ErrorTree, node::NodeError};

#[test]
fn build_errors_remain_in_build_boundary() {
    let schema_error = Error::from(BuildError::Validation(ErrorTree::from(
        "missing schema relation target",
    )));

    match schema_error {
        Error::BuildError(BuildError::Validation(tree)) => {
            assert!(
                tree.messages()
                    .iter()
                    .any(|message| message == "missing schema relation target"),
                "build validation errors must remain wrapped as build-boundary failures",
            );
        }
        Error::BuildError(BuildError::Graph(error)) => {
            panic!("unexpected graph error: {error}");
        }
        Error::NodeError(_) => {
            panic!("build validation failures must not be remapped into node-boundary errors");
        }
    }
}

#[test]
fn node_errors_remain_in_node_boundary() {
    let schema_error = Error::from(NodeError::PathNotFound("entity.user_id".to_string()));

    match schema_error {
        Error::NodeError(NodeError::PathNotFound(path)) => {
            assert_eq!(path, "entity.user_id");
        }
        Error::NodeError(NodeError::IncorrectNodeType(path)) => {
            panic!("unexpected node error kind after conversion for path {path}");
        }
        Error::BuildError(_) => {
            panic!("node errors must not be remapped into build-boundary failures");
        }
    }
}
