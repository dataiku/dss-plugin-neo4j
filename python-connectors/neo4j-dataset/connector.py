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
        for i in xrange(1,10):
            yield { "first_col" : str(i), "my_string" : "Yes" }
        print(80*'*')
        print self.config["queryMode"] == "nodes"
        #if self.config["queryMode"] == "nodes":
        #    q = "MATCH (n:{}) RETURN n".format(self.config["nodeType"])
        #    r = self.graph.run(q)
        #    for record in r.data():
        #        print(record["n"])
        #        #yield dict(record["n"])
        #        yield { "first_col" : "a", "my_string" : "Yes" }