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
        comments_removed = "".join([line for line in raw_queries.strip().split("\n") if not line.startswith("//")])
        queries_split = comments_removed.split(";")
        queries = [query.strip() for query in queries_split if query.strip()]

        uri = self.neo4j_server_configuration.get("neo4j_uri")
        username = self.neo4j_server_configuration.get("neo4j_username")
        password = self.neo4j_server_configuration.get("neo4j_password")

        queries_statistics, queries_data = [], []
        try:
            with GraphDatabase.driver(uri, auth=(username, password)) as driver:
                with driver.session() as session:
                    with session.begin_transaction() as tx:
                        for query in queries:
                            try:
                                results = tx.run(query)
                            except Exception as e:
                                raise ValueError(f"Query '{query}' failed with error: {e}")
                            queries_data.append(results.data())
                            query_statistics = results.consume().counters.__dict__.copy()
                            query_statistics["Cypher query"] = query
                            queries_statistics.append(query_statistics)
                        tx.commit()
                        logging.info("Neo4j plugin macro - All queries were commited")
        except neo4j.exceptions.ConfigurationError as neo4j_error:
            logging.error(f"Neo4j plugin - Macro - Error while connecting to Neo4j server: {neo4j_error}")
            raise Exception(f"Failed to connect to the Neo4j server. Please check your preset credentials and URI.")

        df = pd.DataFrame(queries_statistics)
        temp = df["Cypher query"]
        df = df.drop("Cypher query", axis=1)
        df.insert(0, "Cypher query", temp)

        html = "<h5>Queries statistics</h5>"
        html = html + '<pre style="font-size: 11px">'
        html += df.to_html(index=False, justify="left", na_rep="")
        html = html + "</pre>"

        html += "<h5>Queries data (if any, truncated to first 10 records}</h5>"

        for index, query in enumerate(queries):
            html += f"<h6>{query}</h6>"
            html = html + '<pre style="font-size: 11px">'
            if len(queries_data[index]) > 0:
                html += pd.DataFrame(queries_data[index]).to_html(index=False, justify="left", max_rows=10, na_rep="")
            else:
                html += "No results"
            html = html + "</pre>"

        return html
