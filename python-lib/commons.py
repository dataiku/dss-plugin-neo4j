import os
import logging
import shutil
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
        get_plugin_config().get('neo4jImportDir'),
        get_plugin_config().get('neo4jIsRemote', False),
        get_plugin_config().get('sshHost', None),
        get_plugin_config().get('sshUsername', None)
        )
    neo4jhandle.check()
    return neo4jhandle

def get_input_output():
    if len(get_input_names_for_role('input_dataset')) == 0:
        raise ValueError('No input dataset.')
    input_dataset_name = get_input_names_for_role('input_dataset')[0]
    input_dataset = dataiku.Dataset(input_dataset_name)

    output_folder_name = get_output_names_for_role('output_folder')[0]
    output_folder = dataiku.Folder(output_folder_name).get_path()
    return (input_dataset, output_folder)

def export_dataset(dataset, output_file, format="tsv-excel-noheader"):
    """
    exports a Dataiku Dataset to CSV with no need to go through a Pandas dataframe first (string only)
    """
    logger.info("[+] Starting file export...")
    with open(output_file, "w") as o:
        with dataset.raw_formatted_data(format=format) as i:
            while True:
                chunk = i.read(32000)
                if len(chunk) == 0:
                    break
                o.write(chunk)
    logger.info("[+] Export done.")
