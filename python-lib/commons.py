import os
import logging
import dataiku
from dataiku.customrecipe import get_plugin_config, get_input_names_for_role, get_output_names_for_role
from dku_neo4j.neo4j_handle import Neo4jHandle
import gzip

# This file contains stuff that is common across this plugin recipes but that are not part of
# our dku_neo4j wrapper library. In particular, dku_neo4j library does not depend on any DSS code
# so anything specific to DSS APIs is here


def get_neo4jhandle():
    neo4jhandle = Neo4jHandle(
        get_plugin_config().get("neo4jUri"),
        get_plugin_config().get("neo4jUsername"),
        get_plugin_config().get("neo4jPassword"),
    )
    neo4jhandle.check()
    return neo4jhandle


def get_input_output():
    if len(get_input_names_for_role("input_dataset")) == 0:
        raise ValueError("No input dataset.")
    input_dataset_name = get_input_names_for_role("input_dataset")[0]
    input_dataset = dataiku.Dataset(input_dataset_name)

    output_folder_name = get_output_names_for_role("output_folder")[0]
    output_folder = dataiku.Folder(output_folder_name)
    return (input_dataset, output_folder)


class ImportFileHandler:
    """Class to write and delete dataframe as csv file into a dataiku.Folder """

    def __init__(self, folder):
        self.folder = folder

    def write(self, df, path):
        """Write df to path in Folder as a compressed csv. Returns the complete path of the import directory"""
        string_buf = df.to_csv(sep=",", na_rep="", header=False, index=False)
        with self.folder.get_writer(path=path) as writer:
            with gzip.GzipFile(fileobj=writer, mode="wb") as fgzip:
                logging.info(f"Writing file: {path}")
                fgzip.write(string_buf.encode())
        full_path = os.path.join(self.folder.project_key, self.folder.short_name, path)
        return full_path

    def delete(self, path):
        logging.info(f"Deleting file: {path}")
        self.folder.delete_path(path)


def create_dataframe_iterator(dataset, batch_size=10000, columns=None):
    for df in dataset.iter_dataframes(chunksize=batch_size, columns=columns, parse_dates=False):
        yield df


def check_load_from_csv(folder):
    folder_type = folder.get_info()["type"]
    if folder_type not in ["SCP", "SFTP"]:
        raise ValueError(
            f"Output folder must be in a SCP/SFTP connection to the Neo4j server import directory but it is in a {folder_type} connection."
        )
