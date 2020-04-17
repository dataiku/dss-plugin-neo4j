from dataiku.customwebapp import get_plugin_config, get_webapp_config

# Access the parameters that end-users filled in using webapp config
# For example, for a parameter called "input_dataset"
# input_dataset = get_webapp_config()["input_dataset"]

import dataiku
import pandas as pd
from flask import request
import json
from py2neo import Graph
import re


# Example:
# As the Python webapp backend is a Flask app, refer to the Flask
# documentation for more information about how to adapt this
# example to your needs.
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

connection_info = get_webapp_config().get('connection_info')
print("connection_info: ", connection_info)
try:
    graph = Graph(connection_info.get("neo4j_uri"), auth=(connection_info.get("neo4j_username"), connection_info.get("neo4j_password")))
except Exception:
    raise ValueError("Fail to connect to neo4j server !")

@app.route('/get_node_labels')
def get_node_labels():
    query = "CALL db.labels()"
    data = graph.run(query)
    node_labels = []
    for row in data:
        node_labels += [row[0]]
        
    # TODO check if no node labels at all
    return json.dumps({"node_labels": node_labels})


@app.route('/get_relationship_types')
def get_relationship_types():
    """
    return a list of relationship types related to the node label
    (with both start and end nodes with this label)
    """
    node_label = request.args.get('node_label', None)
    query = "call db.schema"
    dic = graph.run(query).data()[0]
    relationships = dic['relationships']
    rel_types = []
    for rel in relationships:
        if node_label in rel.labels and len(rel.labels)==1:
            #rel_type = re.search("-\[:(.*?) {}\]-", str(rel)).group(1)
            rel_types += [type(rel).__name__]
    # TODO check if no node labels at all
    return json.dumps({"relationship_types": rel_types})
    

@app.route('/get_node_properties')
def get_node_properties():
    node_label = request.args.get('node_label', None)
    numerical = True if request.args.get('numerical', False)=='true' else False
    
    query = "call db.schema.nodeTypeProperties"
    results = graph.run(query).data()
    node_properties = []
    for row in results:
        if node_label == row['nodeType'][2:-1] and row["propertyName"] is not None:
            if numerical:
                # TODO maybe allow for 'Double' type as well
                if row["propertyTypes"] is not None:
                    if 'Long' in row["propertyTypes"] and len(row["propertyTypes"])==1:
                        node_properties += [row["propertyName"]]
            else:
                node_properties += [row["propertyName"]]
    return json.dumps({"node_properties": node_properties})


@app.route('/get_rel_properties')
def get_rel_properties():
    rel_type = request.args.get('rel_type', None)
    numerical = True if request.args.get('numerical', False)=='true' else False
    
    query = "call db.schema.relTypeProperties"
    results = graph.run(query).data()
    rel_properties = []
    for row in results:
        if rel_type == row['relType'][2:-1] and row["propertyName"] is not None:
            if numerical:
                # TODO maybe allow for 'Double' type as well
                if row["propertyTypes"] is not None:
                    if 'Long' in row["propertyTypes"] and len(row["propertyTypes"])==1:
                        rel_properties += [row["propertyName"]]
            else:
                rel_properties += [row["propertyName"]]
    return json.dumps({"rel_properties": rel_properties})


@app.route('/datasets')
def get_dataset_flow():
    client = dataiku.api_client()
    project_key = dataiku.default_project_key()
    project = client.get_project(project_key)
    datasets = project.list_datasets()
    dataset_names = [datasets[i]["name"] for i in range(len(datasets))]
    return json.dumps({"dataset_names": dataset_names})


def dict_to_nice_str(dic):
    return ", ".join(["{}: {}".format(key, value) for key, value in dic.items()])

def str_or_none(value):
    return str(value) if value is not None else None



def get_node_info(src, src_id, params):
    info = {"id": src_id}
    properties = dict(src)
#     if properties is not None:
    info["label"] = str_or_none(properties.get(params["node_caption"], None))
    try: 
        info["value"] = int(properties.get(params["node_size"], 0))
    except Exception:
        pass
    info["group"] = properties.get(params["node_color"], None)
    info["title"] = dict_to_nice_str(properties)
    return info

def get_rel_info(rel, src_id, tgt_id, params):
    info = {"from": src_id, "to": tgt_id}
    properties = dict(rel)
    info["label"] = str_or_none(properties.get(params["rel_caption"], None))
    try:
        info["value"] = int(properties.get(params["rel_size"], 0))
    except Exception:
        pass
    info["title"] = dict_to_nice_str(properties)
    return info

@app.route('/draw_graph')
def draw_graph():
    node_label = request.args.get('node_label', None)
    rel_type = request.args.get('rel_type', None)
    node_caption = request.args.get('node_caption', None)
    node_color = request.args.get('node_color', None)
    node_size = request.args.get('node_size', None)
    rel_caption = request.args.get('rel_caption', None)
    rel_size = request.args.get('rel_size', None)
    query_limit = request.args.get('query_limit', 0)

    query = """
    MATCH (src:{0})-[rel:{1}]->(tgt:{0})
    RETURN src, id(src) AS src_id, rel, tgt, id(tgt) AS tgt_id
    LIMIT {2}
    """.format(node_label, rel_type, query_limit)

    print("query: ", query)
    
    params = {
            "node_label": node_label,
            "rel_type": rel_type,
            "node_caption": node_caption,
            "node_color": node_color,
            "node_size": node_size,
            "rel_caption": rel_caption,
            "rel_size": rel_size,
            "query_limit": query_limit
        }    

    data = graph.run(query)

    nodes = []
    edges = []
    seen_nodes = set()

    for row in data:
        src = row[0]
        src_id = row[1]
        rel = row[2]
        tgt = row[3]
        tgt_id = row[4]

        if src_id not in seen_nodes:
            seen_nodes.add(src_id)
            src_info = get_node_info(src, src_id, params)
            nodes.append(src_info)

        if tgt_id not in seen_nodes:
            seen_nodes.add(tgt_id)
            tgt_info = get_node_info(tgt, tgt_id, params)
            nodes.append(tgt_info)

        rel_info = get_rel_info(rel, src_id, tgt_id, params)
        edges.append(rel_info)

    results = {"nodes": nodes, "edges": edges}

    return json.dumps(results)

@app.route('/draw_subgraph')
def draw_subgraph():
    node_label = request.args.get('node_label', None)
    rel_type = request.args.get('rel_type', None)
    node_caption = request.args.get('node_caption', None)
    node_color = request.args.get('node_color', None)
    node_size = request.args.get('node_size', None)
    rel_caption = request.args.get('rel_caption', None)
    rel_size = request.args.get('rel_size', None)
    query_limit = request.args.get('query_limit', 0)
    selected_node_id = int(request.args.get('selected_node_id', None))
    
    query = """
    MATCH (n:{0})
    WHERE id(n) = {1}
    CALL apoc.path.subgraphAll(n, {{
        relationshipFilter: '{2}',
        labelFilter: '+{0}',
        minLevel: 0,
        maxLevel: 2,
        limit: {3}
    }})
    YIELD nodes, relationships
    RETURN nodes, relationships
    """.format(node_label, selected_node_id, rel_type, query_limit)

    print("query subgraph: ", query)
    
    params = {
        "node_label": node_label,
        "rel_type": rel_type,
        "node_caption": node_caption,
        "node_color": node_color,
        "node_size": node_size,
        "rel_caption": rel_caption,
        "rel_size": rel_size,
        "query_limit": query_limit
    }    

    data = graph.run(query).data()

    nodes_data = data[0]["nodes"]
    edges_data = data[0]["relationships"]

    nodes = []
    edges = []

    for node in nodes_data:
        node_id = node.identity
        node_info = get_node_info(node, node_id, params)
        if node_id == selected_node_id:
            node_info["physics"] = False
            print(node_info)
        nodes.append(node_info)

    for rel in edges_data:
        rel_info = get_rel_info(rel, rel.start_node.identity, rel.end_node.identity, params)
        edges.append(rel_info)

    results = {"nodes": nodes, "edges": edges}

    return json.dumps(results)

