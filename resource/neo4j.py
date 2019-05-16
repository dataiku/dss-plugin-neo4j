from py2neo import Graph

def do(payload, config, plugin_config, inputs):
    # Create Neo4j connection
    uri = plugin_config.get("neo4jUri", None)
    username = plugin_config.get("neo4jUsername", None)
    password = plugin_config.get("neo4jPassword", None)
    graph = Graph(uri, auth=(username, password))
    # Get distinct nodes
    q = """MATCH (n) RETURN DISTINCT LABELS(n) AS Labels"""
    r = graph.run(q)
    nodes = [n["Labels"][0] for n in r.data()]
    return {'nodes': nodes}