import os
import dataiku
from dataiku.customrecipe import get_input_names_for_role, \
    get_output_names_for_role, get_recipe_config
from commons import export_to_dataset, get_neo4jhandle, get_export_file_path_in_dataset
from dku_neo4j import ExportDatasetParams

neo4jhandle = get_neo4jhandle()
params = ExportDatasetParams(
    get_recipe_config().get('node_label'),
    get_recipe_config().get('source_column'),
    get_recipe_config().get('target_column'),
    get_recipe_config().get('source_properties'),
    get_recipe_config().get('relation_verb'),
    get_recipe_config().get('relation_properties'),
    get_recipe_config().get('clear_before_run', True)
    )

input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)

input_dataset_schema = input_dataset.read_schema()
# params.check(input_dataset_schema)

export_to_dataset(input_dataset, output_dataset)

neo4jhandle.add_unique_constraint_on_source_column_property(params)
if params.clear_before_run:
    neo4jhandle.delete_nodes(params.node_label)

export_file_fullname = get_export_file_path_in_dataset(output_dataset)

neo4jhandle.load_dataset(export_file_fullname, input_dataset_schema, params)

# --- Cleanup
# neo4jhandle.delete_file_from_import_dir(export_file_name)
