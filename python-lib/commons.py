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
    # FIXME - do we really need this ?
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # FIXME - do we really need this ?
    #export_log = os.path.join(output_folder, 'export.log')
    #file_handler = logging.FileHandler(export_log, 'w')
    #file_handler.setFormatter(formatter)
    #logger.addHandler(file_handler)
    return logger

def get_neo4jhandle():
    neo4jhandle = Neo4jHandle(
        get_plugin_config().get('neo4jUri'),
        get_plugin_config().get('neo4jUsername'),
        get_plugin_config().get('neo4jPassword'),
        #TODO remove all of those when clean
        get_plugin_config().get('neo4jImportDir'),
        get_plugin_config().get('neo4jIsRemote', False),
        get_plugin_config().get('sshHost', None),
        get_plugin_config().get('sshUsername', None)
        )
    #TODO clean this - can be useless
    neo4jhandle.check()
    return neo4jhandle

def get_input_output():
    if len(get_input_names_for_role('input_dataset')) == 0:
        raise ValueError('No input dataset.')
    input_dataset_name = get_input_names_for_role('input_dataset')[0]
    input_dataset = dataiku.Dataset(input_dataset_name)

    output_folder_name = get_output_names_for_role('output_folder')[0]
    #SCTP folder have no "path" return folder instead and upstream load it
    #output_folder = dataiku.Folder(output_folder_name).get_path()
    output_folder = dataiku.Folder(output_folder_name)
    return (input_dataset, output_folder)

#TODO clean this
#def get_export_file_name():
#    output_folder_name = get_output_names_for_role('output_folder')[0]
#    return 'dss_neo4j_export_' + output_folder_name + '.csv'

# TODO clean this
#def export_dataset(dataset, output_file, format="tsv-excel-noheader"):
#    """
#    exports a Dataiku Dataset to CSV with no need to go through a Pandas dataframe first (string only)
#    """
#    logger.info("[+] Starting file export...")
#    with open(output_file, "w") as o:
#        with dataset.raw_formatted_data(format=format) as i:
#            while True:
#                chunk = i.read(32000)
#                if len(chunk) == 0:
#                    break
#                o.write(chunk)
#    logger.info("[+] Export done.")

def get_export_file_name():
    (input_dataset, output_folder) = get_input_output()
    # project hierarchy already exist in the folder
    return 'dss_neo4j_export_{}.csv'.format(input_dataset.short_name)

def export_dataset(dataset, output_folder, format="tsv-excel-noheader"):
    if not isinstance(output_folder, dataiku.Folder):
        raise ValueError("Invalid type for output_folder: {}, must be dataiku.Folder".format(type(output_folder)))
    
    export_file_name = get_export_file_name()
    logger.info("Exporting to {}".format(export_file_name))

    with dataset.raw_formatted_data(format="tsv-excel-noheader") as i:
        output_folder.upload_stream(export_file_name, i)

    

