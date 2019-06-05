import os
from dataiku.customrecipe import *
from commons import *
from dku_neo4j import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = CombinedExportParams(
    get_recipe_config().get('source_node_label'),
    get_recipe_config().get('source_node_id_column'),
    get_recipe_config().get('source_node_properties'),
    get_recipe_config().get('target_node_label'),
    get_recipe_config().get('target_node_id_column'),
    get_recipe_config().get('target_node_properties'),
    get_recipe_config().get('relationships_verb'),
    get_recipe_config().get('relationship_properties'),
    get_recipe_config().get('relationship_properties_use_all_remaining_columns', True),
    get_recipe_config().get('clear_before_run', True)
    )
(input_dataset, output_folder) = get_input_output()
logger = setup_logging(output_folder)
input_dataset_schema = input_dataset.read_schema()
params.check(input_dataset_schema)

# --- Run

export_file = os.path.join(output_folder, 'export.csv')
export_dataset(input_dataset, export_file)
neo4jhandle.move_to_import_dir(export_file)

neo4jhandle.add_unique_constraint_on_relationship_nodes(params)
if params.clear_before_run:
    neo4jhandle.delete_nodes(params.source_node_label)
    neo4jhandle.delete_nodes(params.target_node_label)
neo4jhandle.load_combined('export.csv', params, input_dataset_schema)

# --- Cleanup
neo4jhandle.delete_file_from_import_dir('export.csv')