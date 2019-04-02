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
INPUT_DATASET_NAME = get_input_names_for_role('input-dataset')[0]
OUTPUT_FOLDER_NAME = get_output_names_for_role('output-folder')[0]

# Recipe settings



# Set logging config
logging.basicConfig(
    filename
    format='%(asctime)s %(levelname)s:%(message)s', 
    level=logging.INFO
)



# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])