import os
import logging
import dataiku
from dataiku.customrecipe import get_plugin_config, get_input_names_for_role, get_output_names_for_role
from dku_neo4j import Neo4jHandle
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


def export_dataset(dataset, folder, columns=None, file_size=100000):
    """Export the input dataset as multiple compressed csv files into the output folder.

    Args:
        dataset (dataiku.Dataset): Input dataset
        folder (dataiku.Folder): Output folder stored in a SCP/SFTP connection to the Neo4j server import directory
        columns (list, optional): List of columns that are exported. Defaults to None.
        file_size (int, optional): Number of rows in each exported csv file.

    Returns:
        List of CSV file paths within the import directory of Neo4j
    """
    csv_file_paths = []
    for i, df in enumerate(dataset.iter_dataframes(chunksize=file_size, columns=columns, parse_dates=False)):
        path_in_folder = f"dss_neo4j_export_{dataset.short_name}/{dataset.short_name}_{i+1:03}.csv.gz"
        string_buf = df.to_csv(sep=",", na_rep="", header=False, index=False)
        with folder.get_writer(path=path_in_folder) as writer:
            with gzip.GzipFile(fileobj=writer, mode="wb") as fgzip:
                logging.info(f"Exporting file to: {path_in_folder}")
                fgzip.write(string_buf.encode())

        full_import_path = os.path.join(folder.project_key, folder.short_name, path_in_folder)
        csv_file_paths.append(full_import_path)

    return csv_file_paths


def create_dataframe_iterator(dataset, batch_size, columns=None):
    for df in dataset.iter_dataframes(chunksize=batch_size, columns=columns, parse_dates=False):
        yield df


def check_load_from_csv(folder):
    folder_type = folder.get_info()["type"]
    if folder_type not in ["SCP", "SFTP"]:
        raise ValueError(
            f"Output folder must be in a SCP/SFTP connection to the Neo4j server import directory but it is in a {folder_type} connection."
        )
