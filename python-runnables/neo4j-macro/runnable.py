import json
from py2neo import Graph
from dataiku.runnables import Runnable

class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.config = config
        # Create Neo4j connection
        uri = self.config.get("neo4jUri", None)
        username = self.config.get("neo4jUsername", None)
        password = self.config.get("neo4jPassword", None)
        self.graph = Graph(uri, auth=(username, password))
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        html = ""
        #html = html + "<hr/>"
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
        
        html = html + "<h5>Query results (if any, truncated to 10)</h5>"
        try:
            df = r.to_data_frame()
            if df.shape[0] > 10:
                do = df.head(10)
            html = html + do.to_html()
        except Exception, e:
            print(str(e))
            html = html + "<pre>No result to display.</pre>"
            
        return html
        