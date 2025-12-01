use crate::prelude::*;
use std::{any::Any, collections::HashSet};

///
/// Entity
///

const MAX_INDEX_FIELDS: usize = 4;

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    pub def: Def,
    pub store: &'static str,
    pub primary_key: &'static str,

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub indexes: &'static [Index],

    pub fields: FieldList,
    pub ty: Type,
}

impl Entity {
    #[must_use]
    pub fn get_pk_field(&self) -> &Field {
        self.fields
            .get(self.primary_key)
            .unwrap_or_else(|| panic!("missing primary key field '{}'", self.primary_key))
    }
}

impl MacroNode for Entity {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TypeNode for Entity {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // primary key must exist and be single-valued
        match self.fields.get(self.primary_key) {
            Some(pk) => {
                if !matches!(pk.value.cardinality, Cardinality::One) {
                    err!(
                        errs,
                        "primary key '{0}' must have cardinality One",
                        self.primary_key
                    );
                }
            }
            None => {
                err!(errs, "missing primary key field '{0}'", self.primary_key);
            }
        }

        // store
        match schema.cast_node::<Store>(self.store) {
            Ok(store) if !matches!(store.ty, StoreType::Data) => {
                err!(errs, "store is not type Data");
            }
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

        // Load and validate index references
        let mut resolved_indexes = Vec::new();

        // check indexes have proper fields
        for index in self.indexes {
            // index store
            match schema.cast_node::<Store>(index.store) {
                Ok(store) if !matches!(store.ty, StoreType::Index) => {
                    err!(errs, "store is not type Index");
                }
                Ok(_) => {}
                Err(e) => errs.add(e),
            }

            // basic length checks
            if index.fields.is_empty() {
                err!(errs, "index must reference at least one field");
            }
            if index.fields.len() > MAX_INDEX_FIELDS {
                err!(
                    errs,
                    "index has {} fields; maximum is {}",
                    index.fields.len(),
                    MAX_INDEX_FIELDS
                );
            }

            // no duplicate fields in a single index definition
            let mut seen = HashSet::new();
            // Check all fields in the index exist on the entity
            for field in index.fields {
                if !seen.insert(*field) {
                    err!(errs, "index contains duplicate field '{field}'");
                }
                if let Some(field) = self.fields.get(field) {
                    if field.value.cardinality == Cardinality::Many {
                        err!(errs, "cannot add an index field with many cardinality");
                    }
                } else {
                    err!(errs, "index field '{field}' not found");
                }
            }
            resolved_indexes.push(index);
        }

        // Check for redundant indexes (prefix relationships)
        for (i, a) in resolved_indexes.iter().enumerate() {
            for b in resolved_indexes.iter().skip(i + 1) {
                if a.unique == b.unique {
                    if a.is_prefix_of(b) {
                        err!(
                            errs,
                            "index {:?} is redundant (prefix of {:?})",
                            a.fields,
                            b.fields
                        );
                    } else if b.is_prefix_of(a) {
                        err!(
                            errs,
                            "index {:?} is redundant (prefix of {:?})",
                            b.fields,
                            a.fields
                        );
                    }
                }
            }
        }

        errs.result()
    }
}

impl VisitableNode for Entity {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        self.fields.accept(v);
        self.ty.accept(v);
    }
}
