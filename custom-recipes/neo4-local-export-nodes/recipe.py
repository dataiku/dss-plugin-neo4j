import os
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
INPUT_DATASET_NAME    = get_input_names_for_role('input-dataset')[0]
OUTPUT_FOLDER_NAME    = get_output_names_for_role('output-folder')[0]

# Recipe settings
GRAPH_NODES_LABEL     = get_recipe_config().get('graph-nodes-label', None)
GRAPH_NODES_DELETE    = get_recipe_config().get('graph-nodes-delete', False)
NEO4J_URI             = get_recipe_config().get('neo4j-uri', None)
NEO4J_USER            = get_recipe_config().get('neo4j-user', None)
NEO4J_PASSWORD        = get_recipe_config().get('neo4j-password', None)

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
