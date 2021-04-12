LOAD_FROM_CSV_PREFIX = """
USING PERIODIC COMMIT
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

LOAD_NODES_FROM_CSV = LOAD_FROM_CSV_PREFIX + EXPORT_NODES_SUFFIX

LOAD_RELATIONSHIPS_FROM_CSV = LOAD_FROM_CSV_PREFIX + EXPORT_RELATIONSHIPS_SUFFIX

BATCH_INSERT_NODES = UNWIND_PREFIX + EXPORT_NODES_SUFFIX

BATCH_INSERT_RELATIONSHIPS = UNWIND_PREFIX + EXPORT_RELATIONSHIPS_SUFFIX
