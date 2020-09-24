import os
from dataiku.customrecipe import get_recipe_config
from commons import get_input_output
from commons import get_export_file_path_in_folder, get_export_file_name, export_dataset
from dku_neo4j import RelationshipsExportParams, Neo4jHandle

# --- Setup recipe
recipe_config = get_recipe_config()

neo4j_server_configuration = recipe_config.get("neo4j_server_configuration")
neo4jhandle = Neo4jHandle(
    uri=neo4j_server_configuration.get("neo4j_uri"),
    username=neo4j_server_configuration.get("neo4j_username"),
    password=neo4j_server_configuration.get("neo4j_password")
)

params = RelationshipsExportParams(
    recipe_config.get('source_node_label'),
    recipe_config.get('source_node_id_column'),
    recipe_config.get('source_node_properties'),
    recipe_config.get('target_node_label'),
    recipe_config.get('target_node_id_column'),
    recipe_config.get('target_node_properties'),
    recipe_config.get('relationships_verb'),
    recipe_config.get('relationship_properties'),
    recipe_config.get('property_names_mapping'),
    recipe_config.get('property_names_map'),
    recipe_config.get('clear_before_run', False)
    )
(input_dataset, output_folder) = get_input_output()
input_dataset_schema = input_dataset.read_schema()
params.check(input_dataset_schema)
export_file_fullname = os.path.join(get_export_file_path_in_folder(), get_export_file_name())
# --- Run

export_dataset(input_dataset, output_folder)

neo4jhandle.add_unique_constraint_on_relationship_nodes(params)

if params.clear_before_run:
    neo4jhandle.delete_nodes(params.source_node_label)
    neo4jhandle.delete_nodes(params.target_node_label)

neo4jhandle.load_relationships(export_file_fullname, input_dataset_schema, params)

# --- Cleanup
# neo4jhandle.delete_file_from_import_dir(export_file_name)
