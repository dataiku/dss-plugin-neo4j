import os
from dataiku.customrecipe import get_recipe_config
from commons import get_input_output, create_dataframe_iterator, check_load_from_csv, ImportFileHandler
from dku_neo4j import NodesExportParams, Neo4jHandle

# --- Setup recipe
recipe_config = get_recipe_config()

(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()

params = NodesExportParams(
    recipe_config.get("nodes_label"),
    recipe_config.get("node_id_column"),
    recipe_config.get("properties_mode"),
    recipe_config.get("node_properties"),
    recipe_config.get("property_names_mapping"),
    recipe_config.get("property_names_map"),
    recipe_config.get("clear_before_run", False),
    input_dataset_schema,
)

params.check(input_dataset_schema)

load_from_csv = recipe_config.get("load_from_csv", False)
if load_from_csv:
    check_load_from_csv(output_folder)
    file_handler = ImportFileHandler(output_folder)

neo4j_server_configuration = recipe_config.get("neo4j_server_configuration")
uri = neo4j_server_configuration.get("neo4j_uri")
username = neo4j_server_configuration.get("neo4j_username")
password = neo4j_server_configuration.get("neo4j_password")

with Neo4jHandle(uri, username, password) as neo4jhandle:
    neo4jhandle.check()

    neo4jhandle.add_unique_constraint_on_nodes(params)

    if params.clear_before_run:
        neo4jhandle.delete_nodes(params.nodes_label)

    batch_size = 100000 if load_from_csv else 10000
    df_iterator = create_dataframe_iterator(input_dataset, batch_size=batch_size, columns=params.used_columns)

    if load_from_csv:
        neo4jhandle.load_nodes_from_csv(df_iterator, input_dataset_schema, params, file_handler)
    else:
        neo4jhandle.insert_nodes_by_batch(df_iterator, input_dataset_schema, params)
