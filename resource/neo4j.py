from py2neo import Graph

def do(payload, config, plugin_config, inputs):
    uri = plugin_config.get("neo4jUri")
    if uri is None or uri == "":
        raise ValueError("Plugin setting: URI not specified")
    username = plugin_config.get("neo4jUsername", None)
    password = plugin_config.get("neo4jPassword", None)
    graph = Graph(uri, auth=(username, password))
    r = graph.run("MATCH (n) RETURN DISTINCT LABELS(n) AS Labels")
    nodes = [n["Labels"][0] for n in r.data()]
    return {'nodes': nodes}