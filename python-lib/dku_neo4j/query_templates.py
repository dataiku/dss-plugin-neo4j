LOAD_FROM_CSV_PREFIX = """
USING PERIODIC COMMIT {periodic_commit}
LOAD CSV FROM 'file:///{import_file_path}' AS line FIELDTERMINATOR ','
WITH {definition}"""

UNWIND_PREFIX = """
WITH ${data} AS dataset
UNWIND dataset AS {rows}"""

SOURCE_MERGE_STATEMENT = """
MERGE (src:`{node_label}`{node_primary_key_statement})"""

SOURCE_MATCH_STATEMENT = """
MATCH (src:`{node_label}`{node_primary_key_statement})"""

TARGET_MERGE_STATEMENT = """
MERGE (tgt:`{node_label}`{node_primary_key_statement})"""

TARGET_MATCH_STATEMENT = """
MATCH (tgt:`{node_label}`{node_primary_key_statement})"""

RELATIONSHIP_MERGE_STATEMENT = """
MERGE (src)-[rel:`{relationships_verb}`{relationship_primary_key_statement}]->(tgt)"""

PROPERTIES_STATEMENT = """
{properties}"""

BATCH_DELETE_NODES = """
CALL apoc.periodic.iterate("MATCH (n:`{nodes_label}`) return n", "DETACH DELETE n", {{batchSize:{batch_size}}})
YIELD batches, total RETURN batches, total"""

DELETE_NODES = """
MATCH (n:`{nodes_label}`) DETACH DELETE n"""

CREATE_CONSTRAINT_IF_NOT_EXIST = """
CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{label}`)
REQUIRE n.`{property_key}` IS UNIQUE"""


def create_export_relationship_suffix_query(
    source_node_label,
    source_node_primary_key_statement,
    source_node_properties,
    target_node_label,
    target_node_primary_key_statement,
    target_node_properties,
    relationships_verb,
    relationship_primary_key_statement,
    relationship_properties,
    skip_row_if_not_source,
    skip_row_if_not_target,
):
    query = ""
    if skip_row_if_not_source and skip_row_if_not_target:
        query += SOURCE_MATCH_STATEMENT.format(
            node_label=source_node_label, node_primary_key_statement=source_node_primary_key_statement
        )
        query += TARGET_MATCH_STATEMENT.format(
            node_label=target_node_label, node_primary_key_statement=target_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=source_node_properties)
        query += PROPERTIES_STATEMENT.format(properties=target_node_properties)
    elif skip_row_if_not_source:
        query += SOURCE_MATCH_STATEMENT.format(
            node_label=source_node_label, node_primary_key_statement=source_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=source_node_properties)
        query += TARGET_MERGE_STATEMENT.format(
            node_label=target_node_label, node_primary_key_statement=target_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=target_node_properties)
    elif skip_row_if_not_target:
        query += TARGET_MATCH_STATEMENT.format(
            node_label=target_node_label, node_primary_key_statement=target_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=target_node_properties)
        query += SOURCE_MERGE_STATEMENT.format(
            node_label=source_node_label, node_primary_key_statement=source_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=source_node_properties)
    else:
        query += SOURCE_MERGE_STATEMENT.format(
            node_label=source_node_label, node_primary_key_statement=source_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=source_node_properties)
        query += TARGET_MERGE_STATEMENT.format(
            node_label=target_node_label, node_primary_key_statement=target_node_primary_key_statement
        )
        query += PROPERTIES_STATEMENT.format(properties=target_node_properties)

    query += RELATIONSHIP_MERGE_STATEMENT.format(
        relationships_verb=relationships_verb,
        relationship_primary_key_statement=relationship_primary_key_statement,
    )
    query += PROPERTIES_STATEMENT.format(properties=relationship_properties)
    return query
