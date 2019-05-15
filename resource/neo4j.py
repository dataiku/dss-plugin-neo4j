from py2neo import Graph

def do(payload, config, plugin_config, inputs):
    # Create Neo4j connection
    uri = config.get("neo4jUri", None)
    username = config.get("neo4jUsername", None)
    password = config.get("neo4jPassword", None)
    graph = Graph(uri, auth=(username, password))
    # Get distinct nodes
    q = """MATCH (n) RETURN DISTINCT LABELS(n) AS Labels"""
    r = graph.run(q)
    nodes = [n["Labels"][0] for n in r.data()]
    return {'nodes': nodes}