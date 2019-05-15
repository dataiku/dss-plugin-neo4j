import json
from py2neo import Graph
from six.moves import xrange
from dataiku.connector import Connector

class Neo4jConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        print(80*'*')
        print("CONFIG:")
        print(json.dumps(config, indent=2))
        print(80*'*')
        # Read plugin parameters
        self.config = config
        # Create Neo4j connection
        uri = self.config.get("neo4jUri", "bolt://localhost:7687")
        username = self.config.get("neo4jUsername", "neo4j")
        password = self.config.get("neo4jPassword", "dataiku")
        self.graph = Graph(uri, auth=(username, password))
        
    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):            
        if self.config["queryMode"] == "nodes":
            q = "MATCH (n:{}) RETURN n".format(self.config["nodeType"])
            print q
            r = self.graph.run(q)
            for record in r.data():
                yield dict(record["n"])