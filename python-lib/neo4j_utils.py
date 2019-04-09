import sys
import logging
import dataiku
from subprocess import Popen, PIPE

logger = logging.getLogger()


#==============================================================================
# GENERAL HELPERS
#==============================================================================

def export_dataset(dataset=None, output_file=None, format="tsv-excel-noheader"):
    '''
    This function exports a Dataiku Dataset to CSV with no
    need to go through a Pandas dataframe first
    '''
    logger.info("[+] Starting file export...")
    ds = dataiku.Dataset(dataset)
    with open(output_file, "w") as o:
        with ds.raw_formatted_data(format=format) as i:
            while True:
                chunk = i.read(32000)
                if len(chunk) == 0:
                    break
                o.write(chunk)
    logger.info("[+] Export done.")


def scp_nopassword_to_server(file_to_copy=None, sshuser=None, sshhost=None, sshpath=None):
    '''
    This function copies a file to a remote server using SCP.
    It requires a password-less access (i.e SSH public key is available)
    '''
    logger.info("[+] Copying file to remote server...")
    p = Popen(
        ["scp", file_to_copy, "{}@{}:{}".format(sshuser, sshhost, sshpath)],
        stdin=PIPE, stdout=PIPE, stderr=PIPE
    )
    out, err = p.communicate()
    if err != '':
        msg = "[-] Issue while copying CSV file to Neo4j server\n"
        msg = msg + "[-] {}".format(err)
        sys.exit(1)
    else:
        logger.info("[+] Copying file complete.")

def delete_nodes_with_label(graph=None, node_label=None):
    '''
    Simple helper to delete all nodes with a given label within
    a Neo4j graph
    '''
    q = """
      MATCH (n:%s)
      DETACH DELETE n
    """ % (node_label)
    logger.info("[+] Start deleting existing nodes with label {}...".format(node_label))
    try:
        r = graph.run(q)
        logger.info("[+] Existing nodes with label {} deleted.".format(node_label))
    except Exception, e:
        logger.error("[-] Issue while deleting nodes with label {}".format(node_label))
        logger.error("[-] {}".format( str(e) ))
        sys.exit(1)

def delete_relationships(graph=None, nodes_a_label=None, nodes_b_label=None, relationships_verb=None):
    q = """
      MATCH (:%s)-[r:%s]-(:%s)
      DELETE r
    """ % (nodes_a_label, relationships_verb, nodes_b_label)
    logger.info("[+] Start deleting existing relationships between {} and {} with verb {}...".format(nodes_a_label, nodes_b_label, relationships_verb))
    try:
        r = graph.run(q)
        logger.info("[+] Existing relationships deleted.")
    except Exception, e:
        logger.error("[-] Issue while deleting relationships")
        logger.error("[-] {}".format( str(e) ))
        sys.exit(1)


#==============================================================================
# HELPERS FOR NODES EXPORT
#==============================================================================

def build_node_schema(node_label=None, dataset=None):
    '''
    This specific function generates the "schema" for a
    node from a Dataiku Dataset
    '''
    ds = dataiku.Dataset(dataset)
    schema = ''
    schema = schema + ':{}'.format(node_label)
    schema = schema + ' {' + '\n'
    c = ',\n'.join( ["  {}: line[{}]".format(r["name"], i) for i, r in enumerate(ds.read_schema())] )
    schema = schema + c
    schema = schema + '\n' + '}'
    logger.info("[+] Schema generation complete for node {}.".format(node_label))
    return schema


def create_nodes_from_csv(graph=None, csv=None, schema=None):
    '''
    Actually creates Neo4j nodes from a Dataiku Dataset
    '''
    q = """
      LOAD CSV FROM 'file:///%s' AS line
      CREATE (%s)
    """ % (csv, schema)
    logger.info("[+] Start importing nodes into Neo4j...")
    logger.info("[+] %s" % (q))
    try:
        r = graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())
    except Exception, e:
        logger.error("[-] Issue while loading CSV file")
        logger.error("[-] {}".format( str(e) ))
        sys.exit(1)


#==============================================================================
# HELPERS FOR RELATIONSHIPS EXPORT
#==============================================================================

def build_relationships_schema(dataset=None, key_a=None, key_b=None, set_properties=False):
    # CSV input schema
    ds = dataiku.Dataset(dataset)
    schema = ', '.join( ["line[{}] AS {}".format(i, r["name"]) for i, r in enumerate(ds.read_schema())] )
    # Edges attributes
    attributes = ""
    if set_properties:
        logger.info("[+] Setting relationships properties")
        attributes = attributes + "{"
        o = []
        for c in ds.read_schema():
            if c["name"] not in[key_a, key_b]:
                o.append("{}:{}".format(c["name"], c["name"]))
        attributes = attributes + ", ".join(o)
        attributes = attributes + "}"
    logger.info("[+] Schema generation complete for relationships CSV file and attributes.")
    return (schema, attributes)


def create_relationships_from_csv(graph=None, csv=None, schema=None,
                                  graph_nodes_left_label=None, graph_nodes_left_key=None, graph_relationships_left_key=None,
                                  graph_nodes_right_label=None, graph_nodes_right_key=None, graph_relationships_right_key=None,
                                  graph_relationships_verb=None, graph_relationships_attributes=None):
    q = """
      USING PERIODIC COMMIT
      LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
      WITH %s
      MATCH (f:%s {%s: %s})
      MATCH (t:%s {%s: %s})
      MERGE (f)-[rel:%s %s]->(t)
    """ % (
        csv,
        schema,
        graph_nodes_left_label, graph_nodes_left_key, graph_relationships_left_key,
        graph_nodes_right_label, graph_nodes_right_key, graph_relationships_right_key,
        graph_relationships_verb, graph_relationships_attributes
    )
    logger.info("[+] Start importing relationships into Neo4j...")
    logger.info("[+] %s" % (q))
    try:
        r = graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())
    except Exception, e:
        logger.error("[-] Issue while loading CSV")
        logger.error("[-] {}".format(str(e)))
        sys.exit(1)
