# Relation Field Metadata Naming

## Status

Accepted.

## Family

Relation field metadata.

## Problem

The relation module used `RelationDescriptor` for metadata extracted from
generated entity fields. Under the 0.165 naming policy, `Descriptor` is reserved
for renderable or observable description values. The relation value is semantic
metadata consumed by schema describe, relation validation, and reverse-index
maintenance, so `Descriptor` overstated the render/diagnostic role.

## Accepted Renames

```text
RelationDescriptor -> RelationFieldMetadata
RelationDescriptorCardinality -> RelationFieldCardinality
relation_descriptors_for_model_iter(...) -> relation_field_metadata_for_model_iter(...)
```

The accepted target metadata helper was also aligned:

```text
AcceptedRelationTargetDescriptor -> AcceptedRelationTargetMetadata
accepted_relation_target_descriptor_from_kind(...) -> accepted_relation_target_metadata_from_kind(...)
```

## Kept Names

- `AcceptedRowLayoutRuntimeContract` remains because it is the accepted-schema
  runtime row-layout descriptor used as a decode/write trust boundary.
- EXPLAIN execution descriptors remain because they are renderable diagnostics.

## Old-Vocabulary Scan Terms

```text
RelationDescriptor|RelationDescriptorCardinality|relation_descriptors_for_model_iter|accepted_relation_target_descriptor_from_kind|AcceptedRelationTargetDescriptor
```
