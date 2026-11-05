use super::index_origin;
use crate::db::schema::{PersistedIndexKeySnapshot, PersistedIndexSnapshot, SchemaIndexId};

#[test]
fn index_origin_reads_accepted_snapshot_instead_of_query_projection() {
    let generated = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index ID should be non-zero"),
        2,
        "generated_index".to_string(),
        "GeneratedIndex".to_string(),
        false,
        PersistedIndexKeySnapshot::from_field_paths(Vec::new()),
        None,
    );
    let ddl = PersistedIndexSnapshot::new_sql_ddl(
        SchemaIndexId::new(2).expect("test index ID should be non-zero"),
        3,
        "ddl_index".to_string(),
        "DdlIndex".to_string(),
        false,
        PersistedIndexKeySnapshot::from_field_paths(Vec::new()),
        None,
    );
    let indexes = [generated, ddl];

    assert_eq!(index_origin(&indexes, 2), Some("generated"));
    assert_eq!(index_origin(&indexes, 3), Some("ddl"));
    assert_eq!(index_origin(&indexes, 4), None);
}
