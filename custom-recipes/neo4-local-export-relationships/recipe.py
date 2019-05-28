import os
import shutil

import dataiku
from dataiku.customrecipe import *
from dku_neo4j import RelationsExportParams
from commons import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = get_relations_export_params()
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

neo4jhandle.add_relationships_unique_constraint(params)
if params.relationships_delete:
    neo4jhandle.delete_relationships(nodes_a_label=params.nodes_from_label, nodes_b_label=params.nodes_to_label, relationships_verb=params.relationships_verb)
neo4jhandle.load_relationships_csv(EXPORT_FILE_NAME, input_dataset_schema, params)

# --- Cleanup
os.remove(outfile)
os.remove(export_file)

logger.info("NEO4J EXPORT DONE")
