import os
from dataiku.customrecipe import *
from commons import *
from dku_neo4j import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = NodesExportParams(
    get_recipe_config().get('nodes_label'),
    get_recipe_config().get('node_id_column'),
    get_recipe_config().get('clear_before_run', True)
    )
params.check()
(input_dataset, output_folder) = get_input_output()
logger = setup_logging(output_folder)
input_dataset_schema = input_dataset.read_schema()

# --- Run

export_file = os.path.join(output_folder, 'export.csv')
export_dataset(input_dataset, export_file)
neo4jhandle.move_to_import_dir(export_file)

# TODO add constraint?
if params.clear_before_run:
    neo4jhandle.delete_nodes(params.nodes_label)
neo4jhandle.load_nodes('export.csv', params.nodes_label, params.node_id_column, input_dataset_schema)

# --- Cleanup
neo4jhandle.delete_file_from_import_dir('export.csv')