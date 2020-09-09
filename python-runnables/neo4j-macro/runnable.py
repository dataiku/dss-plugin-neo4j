import json
from py2neo import Graph
from dataiku.runnables import Runnable


class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config

        settings = config.get('neo4j_server_configuration')
        self.graph = Graph(settings.get("neo4j_uri"), auth=(settings.get("neo4j_username"), settings.get("neo4j_password")))

    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        html = ""
        q = self.config.get("cypherQuery")

        html = html + "<h5>Query</h5>"
        html = html + '<pre style="font-size: 11px">'
        html = html + q
        html = html + "</pre>"

        r = self.graph.run(q)
        html = html + "<h5>Query statistics</h5>"
        html = html + '<pre style="font-size: 11px">'
        html = html + json.dumps(dict(r.stats()), indent=2)
        html = html + "</pre>"

        html = html + "<h5>Query results (if any, truncated to 50)</h5>"
        try:
            df = r.to_data_frame()
            html = html + df.to_html(index=False, max_rows=50)
        except Exception as e:
            html = html + "<pre>No results to display. Error: {}</pre>".format(e)

        return html
