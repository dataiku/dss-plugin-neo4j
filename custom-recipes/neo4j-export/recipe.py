import os
import sys
import logging
import dataiku
import tempfile
import pandas as pd
from py2neo import Graph
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



#==============================================================================
# LOGGING SETTINGS
#==============================================================================
out_folder = dataiku.Folder(OUTPUT_FOLDER_NAME).get_path()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
file_handler = FileHandler(os.path.join(out_folder, 'export.log'), 'w')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#==============================================================================
# EXPORTING TO CSV
#==============================================================================

logging.info("[+] Reading dataset as dataframe...")
ds = dataiku.Dataset(INPUT_DATASET_NAME)
df = ds.get_dataframe()
logger.info("[+] Read dataset with {} rows and {} columns\n".format(df.shape[0], df.shape[1]))

logger.info("[+] Exporting input dataframe to CSV...")
df.to_csv(path_or_buf=os.path.join(out_folder, 'export.csv'), sep="|",header=False, index=False)
logger.info("[+] Exported to CSV\n")


#==============================================================================
# COPYING TO NEO4J SERVER
#==============================================================================

logger.info("[+] Copying file to Neo4j server...")

p = Popen(
    [
        "scp", 
        os.path.join(out_folder, 'export.csv'), 
        "{}@{}:{}".format(SSH_USER, SSH_HOST, SSH_IMPORT_DIRECTORY)
    ], stdin=PIPE, stdout=PIPE, stderr=PIPE
)

out, err = p.communicate()

if err == '':
    logger.info("[+] Copied file to Neo4j server\n")
else:
    logger.error("[-] Issue while copying CSV file to Neo4j server")
    logger.error("[-] {}\n".format(err))
    
    

#==============================================================================
# LOADING DATA INTTO NEO4J
#==============================================================================

# Creating schema
schema = ''
schema = schema + ':{}'.format(GRAPH_NODES_LABEL)
schema = schema + ' {' + '\n'
c = ',\n'.join( ['  {}: line[{}]'.format(column, i) for i, column in enumerate(df.columns)] )
schema = schema + c
schema = schema + '\n' + '}'

logger.info("[+] Built Neo4j output schema for nodes with label {}".format(GRAPH_NODES_LABEL))
logger.info("[+]\n{}\n".format(schema))

# Connect to Neo4j
uri = NEO4J_URI
graph = Graph(uri, auth=("{}".format(NEO4J_USER), "{}".format(NEO4J_PASSWORD)))

# Clean data if needed
if GRAPH_NODES_DELETE:
    q = """
      MATCH (n:%s)
      DETACH DELETE n
    """ % (GRAPH_NODES_LABEL)
    try:
        r = graph.run(q)
        logger.info("[+] Deleted existing nodes")
        logger.info( r.stats() )
    except Exception, e:
        logger.error("[-] Failed to delete existing nodes")
        logger.error(str(e))
        
# Actually load the data
q = """
  LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '|' 
  CREATE (%s)
""" % ('export.csv', schema)

logger.info("[+] Loading CSV file into Neo4j...")
try:
    r = graph.run(q)
    logger.info("[+] Loading complete\n")
    logger.info(r.stats())
except Exception, e:
    logger.error("[-] Issue while loading CSV")
    logger.error("[-] {}\n".format(str(e)))
    
# Perform clean up
p = Popen(
    ["ssh", "{}@{}".format(SSH_USER, SSH_HOST), "rm -rf", "{}/export.csv".format(SSH_IMPORT_DIRECTORY)], 
    stdin=PIPE, stdout=PIPE, stderr=PIPE
)
out, err = p.communicate()

