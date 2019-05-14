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
        self.plugin_config = plugin_config
        # Create Neo4j connection
        uri = self.config.get("neo4jUri", "bolt://localhost:7687")
        username = self.config.get("neo4jUsername", "neo4j")
        password = self.config.get("neo4jPassword", "dataiku")
        self.graph = Graph(uri, auth=(username, password))

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        if self.config["queryMode"] == "nodes":
            q = "MATCH (n:{}) RETURN n".format(self.config["nodeType"])
            r = self.graph.run(q)
            for record in r.data():
                #yield dict(record["n"])
                yield { "first_col" : "a", "my_string" : "Yes" }

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        raise Exception("Unimplemented")


    def get_partitioning(self):
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        raise Exception("unimplemented")