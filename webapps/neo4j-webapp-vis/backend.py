from dataiku.customwebapp import get_webapp_config
# import dataiku
# import pandas as pd
from flask import request
import json
from py2neo import Graph
from dku_neo4j.graph import GraphVis, GraphError
# import re
import logging

logger = logging.getLogger()

connection_info = get_webapp_config().get('connection_info')
print("connection_info: ", connection_info)
try:
    graph_neo4j = Graph(connection_info.get("neo4j_uri"), auth=(connection_info.get("neo4j_username"), connection_info.get("neo4j_password")))
    print("connected to neo4j !")
    # graph_neo4j = Graph("bolt://52.143.188.126:7687", auth=("neo4j", "test"))
except Exception:
    raise ValueError("Fail to connect to neo4j server !")


@app.route('/draw_graph', methods=["POST"])
def draw_graph():
    try:
        config = json.loads(request.data)
        graph_vis = GraphVis(config)

        if config.get('subgraph', False):
            relations_list = graph_vis.run_local_query(graph_neo4j)
        else:
            relations_list = graph_vis.run_global_query(graph_neo4j)

        graph_vis.create_graph(relations_list)

        # graph_vis.compute_layout()

        nodes, edges = list(graph_vis.nodes.values()), list(graph_vis.edges.values())
        results = {"nodes": nodes, "edges": edges}
        return json.dumps(results)

    except GraphError as ge:
        logger.error(traceback.format_exc())
        return str(ge), 505
    except Exception as e:
        logger.error(traceback.format_exc())
        return "Backend error: {}".format(str(e)), 500


@app.route('/get_node_labels')
def get_node_labels():
    query = "CALL db.labels()"
    data = graph_neo4j.run(query)
    node_labels = []
    for row in data:
        node_labels += [row[0]]
    print("node_labels: ", node_labels)
    return json.dumps({"node_labels": node_labels})


@app.route('/get_relation_types')
def get_relation_types():
    query = "call db.schema"
    relationships = graph_neo4j.run(query).data()[0]['relationships']
    relation_types = set()
    for rel in relationships:
        relation_types.add(type(rel).__name__)
    return json.dumps({"relation_types": list(relation_types)})


@app.route('/get_other_labels', methods=["POST"])
def get_other_labels():
    """ get possible node labels of :other depending on :current_label (node label opposite to :other) and :rel_type"""
    config = json.loads(request.data)
    current_label = config['current_label']
    rel_type = config['rel_type']
    other = config['other']

    query = "call db.schema"
    dic = graph_neo4j.run(query).data()[0]
    relationships = dic['relationships']
    other_labels = set()
    for rel in relationships:
        if other == 'Target':
            if current_label == rel.start_node['name'] and rel_type == type(rel).__name__:
                other_labels.add(rel.end_node['name'])
        elif other == 'Source':
            if current_label == rel.end_node['name'] and rel_type == type(rel).__name__:
                other_labels.add(rel.start_node['name'])

    print("list(other_labels): ", list(other_labels))
    return json.dumps({"other_labels": list(other_labels)})


@app.route('/get_labels_from_rel_type', methods=["POST"])
def get_labels_from_rel_type():
    """ return a list of labels for :current node of relation :rel_type """
    config = json.loads(request.data)
    rel_type = config['rel_type']
    current = config['current']

    query = "call db.schema"
    dic = graph_neo4j.run(query).data()[0]
    relationships = dic['relationships']
    current_labels = set()
    for rel in relationships:
        print(rel)
        if rel_type == type(rel).__name__:
            if current == 'Source':
                current_labels.add(rel.start_node['name'])
            elif current == 'Target':
                current_labels.add(rel.end_node['name'])
    print("current_labels: ", current_labels)
    return json.dumps({"current_labels": list(current_labels)})


@app.route('/get_relationship_types', methods=["POST"])
def get_relationship_types():
    """
    return a list of relationship types related to the source (current==Source) or target (current==Target) node label
    """
    config = json.loads(request.data)
    current_label = config['current_label']
    current = config['current']

    query = "call db.schema"
    dic = graph_neo4j.run(query).data()[0]
    relationships = dic['relationships']
    rel_types = set()

    print("dic: ", dic)
    print("current_label: ", current_label)
    for rel in relationships:
        if current == 'Source':
            if current_label == rel.start_node['name']:
                rel_types.add(type(rel).__name__)
        elif current == 'Target':
            if current_label == rel.end_node['name']:
                rel_types.add(type(rel).__name__)
    print("rel_types: ", rel_types)
    # TODO check if no node labels at all
    return json.dumps({"relationship_types": list(rel_types)})


@app.route('/get_node_properties', methods=["POST"])
def get_node_properties():
    config = json.loads(request.data)
    node_label = config['node_label']

    query = "call db.schema.nodeTypeProperties"
    results = graph_neo4j.run(query).data()
    node_properties_all, node_properties_num = [], []
    for row in results:
        if node_label == row['nodeLabels'][0] and row["propertyName"] is not None:
            node_properties_all += [row["propertyName"]]
            # TODO maybe allow for 'Double' type as well
            if row["propertyTypes"] is not None:
                if 'Long' in row["propertyTypes"] or 'Double' in row["propertyTypes"]:
                    node_properties_num += [row["propertyName"]]

    return json.dumps({"node_properties_all": node_properties_all, "node_properties_num": node_properties_num})


@app.route('/get_rel_properties')
def get_rel_properties():
    rel_type = request.args.get('rel_type', None)
    numerical = True if request.args.get('numerical', False) == 'true' else False

    query = "call db.schema.relTypeProperties"
    results = graph_neo4j.run(query)
    rel_properties = []
    for row in results:
        if rel_type == row['relType'][2:-1] and row["propertyName"] is not None:
            if numerical:
                # TODO maybe allow for 'Double' type as well
                if row["propertyTypes"] is not None:
                    if 'Long' in row["propertyTypes"] or 'Double' in row["propertyTypes"]:
                        rel_properties += [row["propertyName"]]
            else:
                rel_properties += [row["propertyName"]]
    return json.dumps({"rel_properties": rel_properties})


@app.route('/get_selected_node_properties', methods=["POST"])
def get_selected_node_properties():
    config = json.loads(request.data)
    node_id = config['node_id']
    query = "MATCH (n) WHERE id(n) = {} RETURN *".format(node_id)
    data = graph_neo4j.run(query).data()
    properties = dict(data[0]["n"])
    properties_list = [{"key": k, "value": str(v)} for k, v in properties.items()]
    return json.dumps({"properties": properties_list})


@app.route('/get_selected_edge_properties', methods=["POST"])
def get_selected_edge_properties():
    config = json.loads(request.data)
    src_id = config['src_id']
    tgt_id = config['tgt_id']
    rel_type = config['rel_type']
    query = "MATCH (n)-[r:{}]->(m) WHERE id(n)={} AND id(m)={} RETURN *".format(
        rel_type, src_id, tgt_id)
    data = graph_neo4j.run(query).data()
    properties = dict(data[0]["r"])
    properties_list = [{"key": k, "value": str(v)} for k, v in properties.items()]
    return json.dumps({"properties": properties_list})


@app.route('/set_title', methods=["POST"])
def set_title():
    config = json.loads(request.data)
    label = config.get('label', None)
    properties = config.get('properties', None)
    node_id = config.get('node_id', None)


    title_list = []
    if node_id:
        title_list += ["<b>{} ({})</b>".format(label, node_id)]
        # title += "<br>".join(["<b>{} ({})</b>".format(label, node_id)] + ["{}: {}".format(key, str(value)) for key, value in properties.items()])
    else:
        title_list += ["<b>{}</b>".format(label)]
        # title += "<br>".join(["<b>{}</b>".format(label)] + ["{}: {}".format(key, str(value)) for key, value in properties.items()])
    title_list += ["{}: {}".format(p['key'], str(p['value'])) for p in properties if p['key'] and p['value']]
    title = "<br>".join(title_list)

    return json.dumps({"title": title})


# @app.route('/draw_subgraph')
# def draw_subgraph():
#     try:
#         print("starting draw_subgraph backend function !")

#         config = json.loads(request.args.get('config', None))

#         print('config: ', config)

#         query_limit = config['query_limit']
#         relations = config['relations']
#         node_params = config['node_params']
#         rel_params = config['rel_params']
#         selected_node_id = config['selected_node_id']

#         print("relations: ", relations)
#         unique_nodes_labels, unique_edges_types = get_unique_nodes_edges(relations)
#         relations_set = get_relations_set(relations)

#         relationshipFilter = get_relationship_filter(unique_edges_types)
#         labelFilter = get_label_filter(unique_nodes_labels)

#         query = """
#         MATCH (n)
#         WHERE id(n) = {0}
#         CALL apoc.path.subgraphAll(n, {{
#             relationshipFilter: '{1}',
#             labelFilter: '+{2}',
#             bfs: true,
#             minLevel: 0,
#             maxLevel: 10,
#             limit: {3}
#         }})
#         YIELD relationships
#         RETURN relationships
#         """.format(selected_node_id, relationshipFilter, labelFilter, query_limit)

#         print("query: ", query)

#         try:
#             data = graph_neo4j.run(query).data()
#         except Exception as e:
#             raise GraphError("Graph error: {}".format(e))

#         # nodes_data = data[0]["nodes"]
#         edges_data = data[0]["relationships"]

#         nodes_list = []
#         edges_list = []
#         seen_nodes = set()

#         compt = 0    # check if query result is empty
#         for rel in edges_data:
#             start_node = rel.start_node
#             end_node = rel.end_node

#             start_label = get_node_label(start_node, unique_nodes_labels)
#             end_label = get_node_label(end_node, unique_nodes_labels)
#             rel_type = type(rel).__name__

#             if (start_label, rel_type, end_label) in relations_set:
#                 compt += 1

#                 start_node_id = start_node.identity
#                 if start_node_id not in seen_nodes:
#                     seen_nodes.add(start_node_id)
#                     node_info = get_node_info(start_node, start_node_id, node_params.get(start_label, None))
#                     if start_node_id == selected_node_id:
#                         node_info["physics"] = False
#                     nodes_list.append(node_info)

#                 end_node_id = end_node.identity
#                 if end_node_id not in seen_nodes:
#                     seen_nodes.add(end_node_id)
#                     node_info = get_node_info(end_node, end_node_id, node_params.get(end_label, None))
#                     if end_node_id == selected_node_id:
#                         node_info["physics"] = False
#                     nodes_list.append(node_info)

#                 rel_info = get_rel_info(rel, start_node_id, end_node_id, rel_params.get(rel_type, None))
#                 edges_list.append(rel_info)

#         if compt == 0:
#             raise GraphError("Graph error: {}".format("Cypher query result is empty !"))

#         results = {"nodes": nodes_list, "edges": edges_list}
#     #     print("results: ", results)

#         return json.dumps(results)

#     except GraphError as ge:
#         logger.error(traceback.format_exc())
#         return str(ge), 505
#     except Exception as e:
#         logger.error(traceback.format_exc())
#         return "Backend error: {}".format(str(e)), 500
