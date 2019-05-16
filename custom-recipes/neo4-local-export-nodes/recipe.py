import os
import shutil
import logging
import dataiku
from py2neo import Graph
from neo4j_utils import *
from dataiku.customrecipe import *
from logging import FileHandler


#==============================================================================
# PLUGIN SETTINGS
#==============================================================================

# I/O settings
INPUT_DATASET_NAME = get_input_names_for_role('inputDataset')[0]
OUTPUT_FOLDER_NAME = get_output_names_for_role('outputFolder')[0]

# Plugin settings
NEO4J_URI          = get_plugin_config().get("neo4jUri", None)
NEO4J_USERNAME     = get_plugin_config().get("neo4jUsername", None)
NEO4J_PASSWORD     = get_plugin_config().get("neo4jPassword", None)
        
# Recipe settings
GRAPH_NODES_LABEL  = get_recipe_config().get('graphNodesLabel', None)
GRAPH_NODES_DELETE = get_recipe_config().get('graphNodesDelete', False)
NEO4J_IMPORT_DIR   = get_recipe_config().get('neo4jImportDir', None)

EXPORT_FILE_NAME      = 'export.csv'



#==============================================================================
# LOGGING SETTINGS
#==============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

export_folder = dataiku.Folder(OUTPUT_FOLDER_NAME).get_path()
export_log = os.path.join(export_folder, 'export.log')
file_handler = FileHandler(export_log, 'w')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("*"*80)
logger.info("* NEO4J EXPORT PROCESS START")
logger.info("*"*80)


#==============================================================================
# EXPORTING TO CSV
#==============================================================================

export_file = os.path.join(export_folder, EXPORT_FILE_NAME)
export_dataset(dataset=INPUT_DATASET_NAME, output_file=export_file)


#==============================================================================
# COPYING TO NEO4J IMPORT FOLDER
#==============================================================================

logger.info("[+] Making file available to Neo4j...")
print export_file
print outfile
outfile = os.path.join(NEO4J_IMPORT_DIR, EXPORT_FILE_NAME)
shutil.copyfile(export_file, outfile)
logger.info("[+] Done.")

#==============================================================================
# LOADING DATA INTO NEO4J
#==============================================================================

# Creating schema
schema = build_node_schema(node_label=GRAPH_NODES_LABEL, dataset=INPUT_DATASET_NAME)

# Connect to Neo4j
uri = NEO4J_URI
graph = Graph(uri, auth=("{}".format(NEO4J_USERNAME), "{}".format(NEO4J_PASSWORD)))

# Clean data if needed
if GRAPH_NODES_DELETE:
    delete_nodes_with_label(graph=graph, node_label=GRAPH_NODES_LABEL)
        
# Actually load the data
create_nodes_from_csv(graph=graph, csv=EXPORT_FILE_NAME, schema=schema)


#==============================================================================
# FINAL CLEANUP
#==============================================================================

# Remote file
os.remove(outfile)

# Local file
os.remove( export_file )

logger.info("*"*80)
logger.info("* NEO4J EXPORT PROCESS END")
logger.info("*"*80)

