import os
import shutil

import dataiku
from dataiku.customrecipe import *
from commons import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = get_nodes_export_params()
(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()
logger = setup_logging(output_folder)
EXPORT_FILE_NAME = 'export.csv'

# --- Run

logger.info("NEO4J EXPORT START")
export_file = os.path.join(output_folder, EXPORT_FILE_NAME)
export_dataset(input_dataset, export_file)
logger.info("[+] Move file to Neo4j import dir...")
outfile = os.path.join(neo4jhandle.import_dir, EXPORT_FILE_NAME)
shutil.move(export_file, outfile)

if params.clear_before_run:
    neo4jhandle.delete_nodes_with_label(params.nodes_label)
neo4jhandle.load_nodes_csv(EXPORT_FILE_NAME, params.nodes_label, input_dataset_schema)

# --- Cleanup

os.remove(outfile)
logger.info("NEO4J EXPORT DONE")
