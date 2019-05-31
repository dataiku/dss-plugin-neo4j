import os
import dataiku
from dataiku.customrecipe import *
from commons import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = get_nodes_export_params()
(input_dataset, output_folder) = get_input_output()
logger = setup_logging(output_folder)
input_dataset_schema = input_dataset.read_schema()

# --- Run

export_file = os.path.join(output_folder, 'export.csv')
export_dataset(input_dataset, export_file)
move_to_import_dir(export_file, neo4jhandle)

# TODO add constraint?
if params.clear_before_run:
    neo4jhandle.delete_nodes(params)
neo4jhandle.load_nodes('export.csv', params.nodes_label, input_dataset_schema)

# --- Cleanup

outfile = os.path.join(neo4jhandle.import_dir, 'export.csv')
if neo4jhandle.is_remote:
    os.remove(export_file)
    ssh_remove_file(outfile, neo4jhandle)
else:
    os.remove(outfile)
