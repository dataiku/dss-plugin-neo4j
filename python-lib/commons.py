import os
import logging
import pandas as pd
import dataiku
from dataiku.customrecipe import get_input_names_for_role,\
     get_output_names_for_role, get_plugin_config
from dku_neo4j import Neo4jHandle


# This file contains stuff that is common across this plugin
# recipes but that are not part of
# our dku_neo4j wrapper library. In particular, dku_neo4j
# library does not depend on any DSS code
# so anything specific to DSS APIs is here

logger = logging.getLogger()


def setup_logging(output_folder):
    logger.setLevel(logging.INFO)
    return logger


def get_neo4jhandle():
    neo4jhandle = Neo4jHandle(
        get_plugin_config().get('neo4jUri'),
        get_plugin_config().get('neo4jUsername'),
        get_plugin_config().get('neo4jPassword')
        )
    # TODO clean this - can be useless
    neo4jhandle.check()
    return neo4jhandle


def get_input_output():
    if len(get_input_names_for_role('input_dataset')) == 0:
        raise ValueError('No input dataset.')
    input_dataset_name = get_input_names_for_role('input_dataset')[0]
    input_dataset = dataiku.Dataset(input_dataset_name)

    output_folder_name = get_output_names_for_role('output_folder')[0]
    # SCTP folder have no "path" return folder instead and upstream load it
    # output_folder = dataiku.Folder(output_folder_name).get_path()
    output_folder = dataiku.Folder(output_folder_name)
    return (input_dataset, output_folder)


def get_export_file_name():
    (input_dataset, output_folder) = get_input_output()
    # project hierarchy already exist in the folder
    return 'dss_neo4j_export_{}.csv'.format(input_dataset.short_name)


def get_export_file_path_in_folder():
    (input_dataset, output_folder) = get_input_output()
    return os.path.join(output_folder.project_key, output_folder.short_name)


def export_dataset(dataset, output_folder, format="tsv-excel-noheader"):
    if not isinstance(output_folder, dataiku.Folder):
        raise ValueError("Invalid type for output_folder: {}, \
            must be dataiku.Folder".format(type(output_folder)))

    export_file_name = get_export_file_name()
    logger.info("Exporting to {}".format(export_file_name))

    with dataset.raw_formatted_data(format="tsv-excel-noheader") as i:
        output_folder.upload_stream(export_file_name, i)


BATCH_SIZE = 32000


def write_dataset_from_query_result(schema, query_result):
    """
    Write the result of a cypher query to a DSS dataset
    """
    output_dataset_name = get_output_names_for_role('output_dataset')[0]
    output_dataset = dataiku.Dataset(output_dataset_name)
    output_dataset.write_schema(schema)
    writer = output_dataset.get_writer()
    cols = [col["name"] for col in schema]

    """
    print("to df")
    df = query_result.to_data_frame()
    print("start write")
    output_dataset.write_with_schema(df[cols])
    """

    # TO-DO implement a cleaner way to write per batch?
    k = 0
    list_row = []
    for row in query_result:
        list_row.append(row.data())
        k = k + 1

        if k % BATCH_SIZE == 0:
            df = pd.DataFrame(list_row)
            print(k)
            writer.write_dataframe(df[cols])
            list_row = []

    if k % BATCH_SIZE != 0:
        df = pd.DataFrame(list_row)
        writer.write_dataframe(df[cols])

    writer.close()
