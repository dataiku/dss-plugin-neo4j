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
        graph = Graph(uri, auth=("neo4j", "dataiku"))

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        for i in xrange(1,10):
            yield { "first_col" : str(i), "my_string" : "Yes" }


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