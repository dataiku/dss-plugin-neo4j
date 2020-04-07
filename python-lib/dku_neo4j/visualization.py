from IPython.display import IFrame
import json
# import uuid
from string import Template
import os
# from dataiku.customrecipe import get_plugin_config
import dataiku
from py2neo import Graph


class GraphError(Exception):
    """Base class for other exceptions"""
    pass


class NeoVisConfig(object):
    def __init__(self, plugin_config, webapp_config, filters, query=None):
        self.container_id = "graph-chart"
        # credentials = self.get_graph_credentials()
        self.server_url = plugin_config["neo4jUri"]
        self.server_user = plugin_config["neo4jUsername"]
        self.server_password = plugin_config["neo4jPassword"]
        self.query_limit = webapp_config["query_limit"]

        self.labels = {
            webapp_config["node_label"]: {
                "caption": webapp_config.get("node_caption", None),
                "community": webapp_config.get("node_color", None),
                "size": webapp_config.get("node_size", None)
            }
        }
        self.relationships = {
            webapp_config["edge_label"]: {
                "caption": webapp_config.get("edge_caption", True),
                "thickness": webapp_config.get("edge_thickness", None)
            }
        }

        self.check_all_exist()

        if query is None:
            self.initial_cypher = self.create_cypher_query(filters)
        else:
            self.initial_cypher = query

    def create_cypher_query(self, filters):
        node_label = list(self.labels.keys())[0]
        edge_label = list(self.relationships.keys())[0]
        where_clause = self.create_where_clause(filters)
        query = """MATCH (src:{})-[r:{}]-(tgt:{}) {}RETURN * LIMIT {}""".format(
                    node_label,
                    edge_label,
                    node_label,
                    where_clause,
                    self.query_limit
                )
        return query

    def create_where_clause(self, filters):
        where_clause = ""
        if len(filters) == 0:
            return where_clause
        graph = Graph(self.server_url, auth=(self.server_user, self.server_password))
        for i, (column, minValue, maxValue) in enumerate(filters):
            if self.check_exists_node_property(graph, column):
                if minValue is not None or maxValue is not None:
                    if i > 0:
                        where_clause += "AND "
                    where_clause += self.where_subclause("src", column, minValue, maxValue)
                    where_clause += "AND "
                    where_clause += self.where_subclause("tgt", column, minValue, maxValue)
            elif self.check_exists_edge_property(graph, column):
                if minValue is not None or maxValue is not None:
                    if i > 0:
                        where_clause += "AND "
                    where_clause += self.where_subclause("r", column, minValue, maxValue)
            else:
                raise GraphError("Filter column {} is neither a node or a relation property".format(column))        
        if len(where_clause) > 0:
            return "WHERE " + where_clause
        return ""

    def where_subclause(self, alias, property, minValue, maxValue):
        subclause = ""
        if minValue is not None:
            subclause += "{} < ".format(minValue)
        subclause += "{}.{} ".format(alias, property)
        if maxValue is not None:
            subclause += "< {} ".format(maxValue)
        return subclause

    def check_column(self, graph, column):
        if self.check_exists_node_property(graph, column):
            return "node_property"
        elif self.check_exists_edge_property(graph, column):
            return "edge_property"
        else:
            raise GraphError("Filter column {} is neither a node or a relation property".format(column)) 

    def check_all_exist(self):
        graph = Graph(self.server_url, auth=(self.server_user, self.server_password))
        node_label = list(self.labels.keys())[0]
        edge_label = list(self.relationships.keys())[0]
        if not self.check_exists_node_label(graph):
            raise GraphError("Node label {} doesn't exist".format(node_label))
        if not self.check_exists_edge_label(graph):
            raise GraphError("Relation verb {} doesn't exist".format(edge_label))
        for node_param in ["caption", "community", "size"]:
            node_property = self.labels[node_label][node_param]
            if node_property is not None and not self.check_exists_node_property(graph, node_property):
                raise GraphError("Node property {} doesn't exist".format(node_property))
        for edge_param in ["thickness"]:
            edge_property = self.relationships[edge_label][edge_param]
            if edge_property is not None and not self.check_exists_edge_property(graph, edge_property):
                raise GraphError("Relation property {} doesn't exist".format(edge_property))

    def check_exists_node_label(self, graph):
        node_label = list(self.labels.keys())[0]
        query = "MATCH (src:{}) RETURN * LIMIT 1".format(node_label)
        return len(graph.run(query).data()) > 0

    def check_exists_edge_label(self, graph):
        edge_label = list(self.relationships.keys())[0]
        query = "MATCH (src)-[r:{}]-(tgt) RETURN * LIMIT 1".format(edge_label)
        return len(graph.run(query).data()) > 0

    def check_exists_node_property(self, graph, node_property):
        node_label = list(self.labels.keys())[0]
        edge_label = list(self.relationships.keys())[0]
        query = "MATCH (src:{})-[r:{}]-(tgt) RETURN EXISTS(src.{}) as exists LIMIT 1".format(node_label, edge_label, node_property)
        return graph.run(query).data()[0]['exists']

    def check_exists_edge_property(self, graph, edge_property):
        node_label = list(self.labels.keys())[0]
        edge_label = list(self.relationships.keys())[0]
        query = "MATCH (src:{})-[r:{}]-(tgt) RETURN EXISTS(r.{}) as exists LIMIT 1".format(node_label, edge_label, edge_property)
        return graph.run(query).data()[0]['exists']


