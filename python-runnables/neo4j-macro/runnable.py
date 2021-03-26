import logging
import pandas as pd
import neo4j
from neo4j import GraphDatabase
from dataiku.runnables import Runnable


class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        self.neo4j_server_configuration = config.get("neo4j_server_configuration")

    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        raw_queries = self.config.get("cypherQuery")
        queries_split = raw_queries.split(";")
        queries = [query.strip() for query in queries_split if query.strip()]

        uri = self.neo4j_server_configuration.get("neo4j_uri")
        username = self.neo4j_server_configuration.get("neo4j_username")
        password = self.neo4j_server_configuration.get("neo4j_password")

        all_results_counters = []
        try:
            with GraphDatabase.driver(uri, auth=(username, password)) as driver:
                with driver.session() as session:
                    with session.begin_transaction() as tx:
                        for query in queries:
                            try:
                                results = tx.run(query)
                            except Exception as e:
                                raise ValueError(f"Query '{query}' failed with error: {e}")
                            results_counters = results.consume().counters.__dict__
                            results_counters["query"] = query
                            all_results_counters.append(results_counters)
                        tx.commit()
                        logging.info("Neo4j plugin macro - All queries were commited")
        except neo4j.exceptions.ConfigurationError as neo4j_error:
            raise Exception(f"Failed to connect to the Neo4j server. Please check your preset credentials and URI.")

        df = pd.DataFrame(all_results_counters)
        temp = df["query"]
        df = df.drop("query", axis=1)
        df.insert(0, "query", temp)

        return df.to_html(index=False, max_rows=50)
