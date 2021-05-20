from json import load
import os
from dataiku.customrecipe import get_recipe_config
from commons import (
    get_input_output,
    create_dataframe_iterator,
    ImportFileHandler,
    GeneralExportParams,
)
from dku_neo4j.neo4j_handle import RelationshipsExportParams, Neo4jHandle

# --- Setup recipe
recipe_config = get_recipe_config()

(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()

export_params = GeneralExportParams(recipe_config)
export_params.check()

params = RelationshipsExportParams(
    source_node_label=recipe_config.get("source_node_label"),
    source_node_id_column=recipe_config.get("source_node_id_column"),
    source_node_properties=recipe_config.get("source_node_properties"),
    target_node_label=recipe_config.get("target_node_label"),
    target_node_id_column=recipe_config.get("target_node_id_column"),
    target_node_properties=recipe_config.get("target_node_properties"),
    relationships_verb=recipe_config.get("relationships_verb"),
    relationship_id_column=recipe_config.get("relationship_id_column"),
    relationship_properties=recipe_config.get("relationship_properties"),
    property_names_mapping=recipe_config.get("property_names_mapping"),
    property_names_map=recipe_config.get("property_names_map"),
    expert_mode=recipe_config.get("expert_mode"),
    clear_before_run=recipe_config.get("clear_before_run"),
    node_count_property=recipe_config.get("node_count_property"),
    edge_weight_property=recipe_config.get("edge_weight_property"),
    skip_row_if_not_source=recipe_config.get("skip_row_if_not_source"),
    skip_row_if_not_target=recipe_config.get("skip_row_if_not_target"),
)

params.check(input_dataset_schema)

if export_params.load_from_csv:
    file_handler = ImportFileHandler(output_folder)
    params.set_periodic_commit(export_params.batch_size)

with Neo4jHandle(export_params.uri, export_params.username, export_params.password) as neo4jhandle:
    neo4jhandle.check()

    neo4jhandle.add_unique_constraint_on_relationship_nodes(params)

    if params.clear_before_run:
        neo4jhandle.delete_nodes(params.source_node_label, batch_size=export_params.batch_size)
        neo4jhandle.delete_nodes(params.target_node_label, batch_size=export_params.batch_size)

    batch_size = export_params.csv_size if export_params.load_from_csv else export_params.batch_size
    df_iterator = create_dataframe_iterator(input_dataset, batch_size=batch_size, columns=params.used_columns)

    if export_params.load_from_csv:
        neo4jhandle.load_relationships_from_csv(df_iterator, input_dataset_schema, params, file_handler)
    else:
        neo4jhandle.insert_relationships_by_batch(df_iterator, input_dataset_schema, params)
