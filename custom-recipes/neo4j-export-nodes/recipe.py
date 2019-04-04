import os
import logging
import dataiku
import pandas as pd
from py2neo import Graph
from neo4j_utils import *
from subprocess import Popen, PIPE
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
SSH_HOST              = get_recipe_config().get('ssh-host', None)
SSH_USER              = get_recipe_config().get('ssh-user', None)
SSH_IMPORT_DIRECTORY  = get_recipe_config().get('ssh-import-directory', None)

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
logger.info("* NEO4J EXPORT PROCESS START ")
logger.info("*"*80)
logger.info("\n")

#==============================================================================
# EXPORTING TO CSV
#==============================================================================

export_file = os.path.join(export_folder, EXPORT_FILE_NAME)
export_dataset(dataset=INPUT_DATASET_NAME, output_file=export_file)


#==============================================================================
# COPYING FILE TO NEO4J SERVER
#==============================================================================

out, err = scp_nopassword_to_server(
    file_to_copy=export_file, 
    sshuser=SSH_USER, 
    sshhost=SSH_HOST, 
    sshpath=SSH_IMPORT_DIRECTORY
)

if err != '':
    msg = "[-] Issue while copying CSV file to Neo4j server\n"
    msg = msg + "[-] {}".format(err)
    raise Exception(msg)    
    

#==============================================================================
# LOADING DATA INTO NEO4J
#==============================================================================

# Creating schema
schema = build_node_schema(node_label=GRAPH_NODES_LABEL, dataset=INPUT_DATASET_NAME)

# Connect to Neo4j
uri = NEO4J_URI
graph = Graph(uri, auth=("{}".format(NEO4J_USER), "{}".format(NEO4J_PASSWORD)))

# Clean data if needed
if GRAPH_NODES_DELETE:
    delete_nodes_with_label(graph=graph, node_label=GRAPH_NODES_LABEL)
        
# Actually load the data
create_nodes_from_csv(graph=graph, csv=EXPORT_FILE_NAME, schema=schema)

    
#==============================================================================
# FINAL CLEANUP
#==============================================================================

# Remote file
p = Popen(
    ["ssh", "{}@{}".format(SSH_USER, SSH_HOST), "rm -rf", "{}/export.csv".format(SSH_IMPORT_DIRECTORY)], 
    stdin=PIPE, stdout=PIPE, stderr=PIPE
)
out, err = p.communicate()

# Local file
os.remove( export_file )
