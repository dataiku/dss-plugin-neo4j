import os
import logging
import sys
from subprocess import Popen, PIPE

import dataiku
from dataiku.customrecipe import *
from dku_neo4j import *

# This file contains stuff that is common across this plugin recipes but that are not part of
# our dku_neo4j wrapper library. In particular, dku_neo4j library does not depend on any DSS code
# so anything specific to DSS APIs is here

logger = logging.getLogger()

def setup_logging(output_folder):
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    export_log = os.path.join(output_folder, 'export.log')
    file_handler = logging.FileHandler(export_log, 'w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def get_neo4jhandle():
    neo4jhandle = Neo4jHandle(
        get_plugin_config().get('neo4jUri'),
        get_plugin_config().get('neo4jUsername'),
        get_plugin_config().get('neo4jPassword'),
        get_recipe_config().get('sshHost', None),
        get_recipe_config().get('sshUsername', None),
        get_recipe_config().get('neo4jImportDir')
        )
    neo4jhandle.check()
    return neo4jhandle

def get_nodes_export_params():
    params = NodesExportParams(
        get_recipe_config().get('nodes_label'),
        get_recipe_config().get('graphNodesDelete'),
        )
    params.check()
    return params

def get_relations_export_params():
    params = RelationsExportParams(
        get_recipe_config().get('graphNodesFromLabel'),
        get_recipe_config().get('graphNodesFromKey'),
        get_recipe_config().get('graphNodesToLabel'),
        get_recipe_config().get('graphNodesToKey'),
        get_recipe_config().get('graphRelationshipsFromKey'),
        get_recipe_config().get('graphRelationshipsToKey'),
        get_recipe_config().get('graphRelationshipsVerb'),
        get_recipe_config().get('graphRelationshipsSetProperties', False),
        get_recipe_config().get('graphRelationshipsDelete', True)
        )
    params.check()
    return params

def get_input_output():
    if len(get_input_names_for_role('input_dataset')) == 0:
        raise ValueError('No input dataset.')
    input_dataset_name = get_input_names_for_role('input_dataset')[0]
    input_dataset = dataiku.Dataset(input_dataset_name)

    output_folder_name = get_output_names_for_role('output_folder')[0]
    output_folder = dataiku.Folder(output_folder_name).get_path()
    return (input_dataset, output_folder)

def export_dataset(dataset=None, output_file=None, format="tsv-excel-noheader"):
    '''
    This function exports a Dataiku Dataset to CSV with no
    need to go through a Pandas dataframe first
    '''
    logger.info("[+] Starting file export...")
    with open(output_file, "w") as o:
        with dataset.raw_formatted_data(format=format) as i:
            while True:
                chunk = i.read(32000)
                if len(chunk) == 0:
                    break
                o.write(chunk)
    logger.info("[+] Export done.")

def scp_nopassword_to_server(file_to_copy, sshuser, sshhost, sshpath):
    '''
    This function copies a file to a remote server using SCP.
    It requires a password-less access (i.e SSH public key is available)
    '''
    logger.info("[+] Copying file to remote server...")
    p = Popen(
        ["scp", file_to_copy, "{}@{}:{}".format(sshuser, sshhost, sshpath)],
        stdin=PIPE, stdout=PIPE, stderr=PIPE
    )
    out, err = p.communicate()
    if err != '':
        msg = "[-] Issue while copying CSV file to Neo4j server\n"
        msg = msg + "[-] {}".format(err)
        sys.exit(1) # TODO why not raise Exception
    else:
        logger.info("[+] Copying file complete.")
