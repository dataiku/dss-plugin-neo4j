from neo4j.graph import Node
from dataiku.connector import Connector
import neo4j
from dku_neo4j.neo4j_handle import Neo4jHandle
import logging


class MyConnector(Connector):
    NODE = "node"
    RELATIONSHIP = "relationship"

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.config = config
        self.plugin_config = plugin_config

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        settings = self.config.get("neo4j_server_configuration")
        with Neo4jHandle(
            settings.get("neo4j_uri"), settings.get("neo4j_username"), settings.get("neo4j_password")
        ) as neo4jhandle:
            with neo4jhandle.driver.session(default_access_mode=neo4j.READ_ACCESS, database=settings.get("neo4j_database", neo4j.DEFAULT_DATABASE)) as session:

                if self.config["select_nodes_or_relationships"] == "select_nodes":
                    if self.config.get("selected_node", None):
                        self._check_query_input(self.config["selected_node"], "node label")
                        query = f"MATCH ({self.NODE}:{self.config['selected_node']}) RETURN {self.NODE}"
                        if records_limit > -1:
                            query += f" LIMIT {records_limit}"
                        for row in session.run(query):
                            yield self._process_node(row)
                    else:
                        query = "CALL db.labels()"
                        for row in session.run(query):
                            yield {"nodes": row[0]}

                elif self.config["select_nodes_or_relationships"] == "select_relationships":

                    if self.config.get("selected_relationship", None):
                        self._check_query_input(self.config["selected_relationship"], "relationship type")
                        query = f"MATCH ()-[{self.RELATIONSHIP}:{self.config['selected_relationship']}]->() RETURN {self.RELATIONSHIP}"
                        if records_limit > -1:
                            query += f" LIMIT {records_limit}"
                        for row in session.run(query):
                            yield self._process_relationship(row)
                    else:
                        query = "CALL db.schema.visualization()"
                        relationships = session.run(query).data()[0]["relationships"]
                        for rel in relationships:
                            if len(rel) > 2:
                                yield {
                                    "source node": rel[0]["name"],
                                    "relationship": rel[1],
                                    "target node": rel[2]["name"],
                                }

                elif self.config["select_nodes_or_relationships"] == "custom_query":
                    custom_query = self.config.get("selected_custom_query")
                    if custom_query is None:
                        raise Exception("Custom Cypher query is empty. Please enter a valid query.")
                    logging.info(f"Neo4j plugin - Dataset - User custom query: {custom_query}")
                    if records_limit > -1:
                        custom_query = f"CALL {{{custom_query}}} RETURN * LIMIT {records_limit}"
                    try:
                        for record in session.run(custom_query):
                            yield self._process_custom_record(record)
                    except neo4j.exceptions.Neo4jError as neo4j_error:
                        logging.error(
                            f"Neo4j plugin - Dataset - Error while running custom cypher query: {custom_query}. Error: {neo4j_error}"
                        )
                        raise Exception(
                            f"Custom cypher query failed with error: '{neo4j_error.message}'. Check the logs for more details about the error."
                        )

    def _convert_neotime_properties(self, properties):
        for key, value in properties.items():
            if isinstance(value, neo4j.time.DateTime):
                properties[key] = value.to_native()

    def _check_query_input(self, input_str, label):
        if "|" in input_str:
            raise ValueError("You must enter only one valid {} !".format(label))

    def _process_node(self, row):
        node_properties = {"node_label": self.config["selected_node"], "neo4j_id": row[self.NODE].id}
        node_properties.update(dict(row[self.NODE]))
        self._convert_neotime_properties(node_properties)
        return node_properties

    def _process_relationship(self, row):
        rel_properties = {
            "relationship_type": self.config["selected_relationship"],
            "source_neo4j_id": row[self.RELATIONSHIP].start_node.id,
            "target_neo4j_id": row[self.RELATIONSHIP].end_node.id,
        }
        rel_properties.update(dict(row[self.RELATIONSHIP]))
        self._convert_neotime_properties(rel_properties)
        return rel_properties

    def _process_custom_record(self, record):
        if not isinstance(record, neo4j.data.Record):
            raise ValueError("Query return type cannot be converted into a dataset. It must be a record.")
        processed_record = {}
        for key, value in record.items():
            if isinstance(value, (neo4j.graph.Node, neo4j.graph.Relationship)):
                processed_record[key] = dict(value)
                self._convert_neotime_properties(processed_record[key])
            elif isinstance(value, neo4j.time.DateTime):
                processed_record[key] = value.to_native()
            elif isinstance(value, (int, float, bool, str)):
                processed_record[key] = value
            else:
                logging.info(f"Neo4j plugin - Dataset - Conversion for return type '{type(value)}' is not implemented.")
                processed_record[key] = str(value)

        return processed_record
