from py2neo import Graph
from dataiku.connector import Connector

class Neo4jConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.config = config
        self.plugin_config = plugin_config
        uri = self.plugin_config.get("neo4jUri", None)
        if uri is None or uri == "":
            raise ValueError("Plugin settings: Neo4j URI not specified")
        username = self.plugin_config.get("neo4jUsername", None)
        password = self.plugin_config.get("neo4jPassword", None)
        self.graph = Graph(uri, auth=(username, password))

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        if self.config["queryMode"] == "nodes":
            q = "MATCH (n:{}) RETURN n".format(self.config["nodeType"])
            r = self.graph.run(q)
            for record in r.data():
                yield dict(record["n"])
        elif self.config["queryMode"] == "cypher":
            q = self.config["cypherQuery"]
            r = self.graph.run(q)
            for record in r.data():
                yield dict(record)
