import os
from dataiku.customrecipe import get_recipe_config
from commons import (
    get_input_output,
    create_dataframe_iterator,
    ImportFileHandler,
    GeneralExportParams,
)
from dku_neo4j.neo4j_handle import NodesExportParams, Neo4jHandle

# --- Setup recipe
recipe_config = get_recipe_config()

(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()

export_params = GeneralExportParams(recipe_config)
export_params.check()

params = NodesExportParams(
    nodes_label=recipe_config.get("nodes_label"),
    node_id_column=recipe_config.get("node_id_column"),
    properties_mode=recipe_config.get("properties_mode"),
    node_properties=recipe_config.get("node_properties"),
    property_names_mapping=recipe_config.get("property_names_mapping"),
    property_names_map=recipe_config.get("property_names_map"),
    expert_mode=recipe_config.get("expert_mode", False),
    clear_before_run=recipe_config.get("clear_before_run", False),
    columns_list=input_dataset_schema,
)

params.check(input_dataset_schema)

if export_params.load_from_csv:
    file_handler = ImportFileHandler(output_folder)
    params.set_periodic_commit(export_params.batch_size)

with Neo4jHandle(export_params.uri, export_params.username, export_params.password) as neo4jhandle:
    neo4jhandle.check()

    neo4jhandle.add_unique_constraint_on_nodes(params)

    if params.clear_before_run:
        neo4jhandle.delete_nodes(params.nodes_label)

    batch_size = export_params.csv_size if export_params.load_from_csv else export_params.batch_size
    df_iterator = create_dataframe_iterator(input_dataset, batch_size=batch_size, columns=params.used_columns)

    if export_params.load_from_csv:
        neo4jhandle.load_nodes_from_csv(df_iterator, input_dataset_schema, params, file_handler)
    else:
        neo4jhandle.insert_nodes_by_batch(df_iterator, input_dataset_schema, params)
