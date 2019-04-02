import os
import sys
import logging
import dataiku
import tempfile
import pandas as pd
from py2neo import Graph
from subprocess import Popen, PIPE
from dataiku.customrecipe import *


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



#==============================================================================
# LOGGING SETTINGS
#==============================================================================
out_folder = dataiku.Folder(OUTPUT_FOLDER_NAME).get_path()

logging.basicConfig(
    filename=os.path.join(out_folder, 'export.log'),
    format='%(asctime)s %(levelname)s:%(message)s', 
    level=logging.INFO
)


#==============================================================================
# EXPORTING TO CSV
#==============================================================================

logging.info("[+] Reading dataset as dataframe...")
ds = dataiku.Dataset(INPUT_DATASET_NAME)
df = ds.get_dataframe()
logging.info("[+] Read dataset with {} rows and {} columns".format(df.shape[0], df.shape[1]))

logging.info("[+] Exporting input dataframe to CSV...")
df.to_csv(path_or_buf=os.path.join(out_folder, 'export.csv'), sep="|",header=False, index=False)
logging.info("[+] Exported to CSV")


#==============================================================================
# COPYING TO NEO4J SERVER
#==============================================================================

logging.info("[+] Copying file to Neo4j server...")

p = Popen(
    [
        "scp", 
        os.path.join(out_folder, 'export.csv'), 
        "{}@{}:{}".format(SSH_USER, SSH_HOST, SSH_IMPORT_DIRECTORY)
    ], stdin=PIPE, stdout=PIPE, stderr=PIPE
)

out, err = p.communicate()

if err == '':
    logging.info("[+] Copied file to Neo4j server")
else:
    logging.error("[-] Issue while copying CSV file to Neo4j server")
    logging.error("[-] {}".format(err))
    
    

#==============================================================================
# LOADING DATA INTTO NEO4J
#==============================================================================
