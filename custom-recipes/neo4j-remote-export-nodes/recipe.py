import os
import shutil
from subprocess import Popen, PIPE

import dataiku
from dataiku.customrecipe import *
from commons import *

# --- Setup recipe

neo4jhandle = get_neo4jhandle()
params = get_nodes_export_params()
(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()
logger = setup_logging(output_folder)
EXPORT_FILE_NAME = 'export.csv'

# --- Run

logger.info("NEO4J EXPORT START")
export_file = os.path.join(output_folder, EXPORT_FILE_NAME)
export_dataset(input_dataset, export_file)

scp_nopassword_to_server(
    file_to_copy=export_file,
    sshuser=neo4jhandle.ssh_user,
    sshhost=neo4jhandle.ssh_host,
    sshpath=neo4jhandle.import_dir
)

if params.clear_before_run:
    neo4jhandle.delete_nodes_with_label(params.nodes_label)
neo4jhandle.load_nodes_csv(EXPORT_FILE_NAME, params.nodes_label, input_dataset_schema)

# --- Cleanup

# Remote file
# p = Popen(
#     ["ssh", "{}@{}".format(neo4jhandle.ssh_user, neo4jhandle.ssh_host), "rm -rf", "{}/export.csv".format(neo4jhandle.import_dir)],
#     stdin=PIPE, stdout=PIPE, stderr=PIPE
# )
# out, err = p.communicate()

# Local file
os.remove(export_file)

logger.info("NEO4J EXPORT DONE")
