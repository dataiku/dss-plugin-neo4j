import json
from py2neo import Graph
from six.moves import xrange
from dataiku.connector import Connector

class Neo4jConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        # Read plugin parameters
        self.config = config
        # Create Neo4j connection
        uri = self.config.get("neo4jUri", None)
        username = self.config.get("neo4jUsername", None)
        password = self.config.get("neo4jPassword", None)
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
            
       