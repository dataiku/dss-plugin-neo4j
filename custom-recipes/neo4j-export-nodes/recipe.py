import os
import shutil

import dataiku
from dataiku.customrecipe import *
from commons import *

# --- Setup recipe

EXPORT_FILE_NAME = 'export.csv'
neo4jhandle = get_neo4jhandle()
params = get_nodes_export_params()
(input_dataset, output_folder) = get_input_output()
logger = setup_logging(output_folder)
input_dataset_schema = input_dataset.read_schema()

# --- Run

export_file = os.path.join(output_folder, EXPORT_FILE_NAME)
export_dataset(input_dataset, export_file)

if neo4jhandle.is_remote:
    scp_nopassword_to_server(export_file, neo4jhandle)
else:
    logger.info("[+] Move file to Neo4j import dir...")
    outfile = os.path.join(neo4jhandle.import_dir, EXPORT_FILE_NAME)
    shutil.move(export_file, outfile)

# TODO add constraint?
if params.clear_before_run:
    neo4jhandle.delete_nodes(params)
neo4jhandle.load_nodes_csv(EXPORT_FILE_NAME, params.nodes_label, input_dataset_schema)

# --- Cleanup

if neo4jhandle.is_remote:
    os.remove(export_file)
    ssh_remove_file("{}/export.csv".format(neo4jhandle.import_dir), neo4jhandle)
else:
    os.remove(outfile)
