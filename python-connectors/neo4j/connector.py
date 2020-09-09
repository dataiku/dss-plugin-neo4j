from dataiku.connector import Connector
from py2neo import Graph
import neotime


class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.config = config
        self.plugin_config = plugin_config

        settings = config.get('neo4j_server_configuration')
        try:
            self.graph = Graph(settings.get("neo4j_uri"), auth=(settings.get("neo4j_username"), settings.get("neo4j_password")))
        except Exception:
            raise ValueError("Fail to connect to neo4j server !")

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        if self.config["select_nodes_or_relationships"] == "select_nodes":

            if self.config.get("selected_node", None):
                query = "MATCH (n:{}) RETURN n".format(self.config["selected_node"])
                data = self.graph.run(query)
                for row in data:
                    node_properties = {"neo4j_id": row['n'].identity}
                    node_properties.update(dict(row['n']))
                    self._convert_neotime_properties(node_properties)
                    yield node_properties
            else:
                query = "CALL db.labels()"
                data = self.graph.run(query)
                for row in data:
                    yield {"nodes": row[0]}

        elif self.config["select_nodes_or_relationships"] == "select_relationships":

            if self.config.get("selected_relationship", None):
                query = "MATCH ()-[r:{}]->() RETURN r".format(self.config["selected_relationship"])
                data = self.graph.run(query)
                for row in data:
                    rel_properties = {
                        "source_neo4j_id": row['r'].start_node.identity,
                        "target_neo4j_id": row['r'].end_node.identity
                    }
                    rel_properties.update(dict(row['r']))
                    self._convert_neotime_properties(rel_properties)
                    yield rel_properties
            else:
                query = "CALL db.schema()"
                relationships = self.graph.run(query).data()[0]['relationships']
                for rel in relationships:
                    yield {
                        "source node": rel.start_node['name'],
                        "relationship": type(rel).__name__,
                        "target node": rel.end_node['name']
                    }

    def _convert_neotime_properties(self, properties):
        for k, v in properties.items():
            if isinstance(v, neotime.DateTime):
                properties[k] = v.to_native()
