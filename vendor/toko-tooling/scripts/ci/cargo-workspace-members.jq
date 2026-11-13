# Pure coordinated-member projection; callers own successful Cargo admission.
def coordinated_members($primary; $version):
    . as $metadata |
    [.packages[] | select(.id as $id | $metadata.workspace_members | index($id))] as $owned |
    if ($metadata.workspace_members | length) > 0 and
       (($metadata.workspace_members | unique | length) == ($metadata.workspace_members | length)) and
       (($owned | map(.id) | sort) == ($metadata.workspace_members | sort)) and
       (($owned | map(.name) | unique | length) == ($owned | length)) and
       any($owned[]; .name == $primary) and
       all($owned[]; .source == null and .version == $version and
           (.name | test("^[A-Za-z0-9][A-Za-z0-9_-]*$")))
    then $owned else error("invalid coordinated workspace members") end;
