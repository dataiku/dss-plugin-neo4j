import json
from py2neo import Graph
from dataiku.runnables import Runnable

class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        # Create Neo4j connection
        uri = self.plugin_config.get("neo4jUri", None)
        username = self.plugin_config.get("neo4jUsername", None)
        password = self.plugin_config.get("neo4jPassword", None)
        self.graph = Graph(uri, auth=(username, password))
        
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
        except Exception, e:
            html = html + "<pre>No result to display.</pre>"
            
        return html
        