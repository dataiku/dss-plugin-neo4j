from py2neo import Graph

def do(payload, config, plugin_config, inputs):
    # Create Neo4j connection
    uri = config.get("neo4jUri", "bolt://localhost:7687")
    username = config.get("neo4jUsername", "neo4j")
    password = config.get("neo4jPassword", "dataiku")
    graph = Graph(uri, auth=(username, password))
    # Get distinct nodes
    q = """MATCH (n) RETURN DISTINCT LABELS(n) AS Labels"""
    return {'nodes': ['a', 'b', 'c']}