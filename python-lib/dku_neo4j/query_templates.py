LOAD_FROM_CSV_PREFIX = """
USING PERIODIC COMMIT {periodic_commit}
LOAD CSV FROM 'file:///{import_file_path}' AS line FIELDTERMINATOR ','
WITH {definition}"""

UNWIND_PREFIX = """
WITH ${data} AS dataset
UNWIND dataset AS {rows}"""

EXPORT_NODES_SUFFIX = """
MERGE (n:`{nodes_label}`{node_primary_key_statement})
{properties}
"""

EXPORT_RELATIONSHIPS_SUFFIX = """
MERGE (src:`{source_node_label}`{source_node_primary_key_statement})
{source_node_properties}
MERGE (tgt:`{target_node_label}`{target_node_primary_key_statement})
{target_node_properties}
MERGE (src)-[rel:`{relationships_verb}`{relationship_primary_key_statement}]->(tgt)
{relationship_properties}
"""

# Don't create non-existing nodes and relationships if either the source or the target node doesn't exist
EXPORT_RELATIONSHIPS_EXISTING_NODES_ONLY_SUFFIX = """
MATCH (src:`{source_node_label}`{source_node_primary_key_statement})
MATCH (tgt:`{target_node_label}`{target_node_primary_key_statement})
{source_node_properties}
{target_node_properties}
MERGE (src)-[rel:`{relationships_verb}`{relationship_primary_key_statement}]->(tgt)
{relationship_properties}
"""

LOAD_NODES_FROM_CSV = LOAD_FROM_CSV_PREFIX + EXPORT_NODES_SUFFIX

LOAD_RELATIONSHIPS_FROM_CSV = LOAD_FROM_CSV_PREFIX + EXPORT_RELATIONSHIPS_SUFFIX

BATCH_INSERT_NODES = UNWIND_PREFIX + EXPORT_NODES_SUFFIX

BATCH_INSERT_RELATIONSHIPS = UNWIND_PREFIX + EXPORT_RELATIONSHIPS_SUFFIX

BATCH_DELETE_NODES = """
CALL apoc.periodic.iterate("MATCH (n:`{nodes_label}`) return n", "DETACH DELETE n", {{batchSize:{batch_size}}}) yield batches, total RETURN batches, total
"""
