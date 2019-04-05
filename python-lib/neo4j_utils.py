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
    logger.info("[+] Copying file complete.")
    return out, err


def delete_nodes_with_label(graph=None, node_label=None):
    '''
    Simple helper to delete all nodes with a given label within
    a Neo4j graph
    '''
    q = """
      MATCH (n:%s)
      DETACH DELETE n
    """ % (node_label)
    logger.info("[+] Starting deleting existing nodes with label {}...".format(node_label))
    try:
        r = graph.run(q)
        logger.info("[+] Existing nodes with label {} deleted.".format(node_label))
    except Exception, e:
        logger.error("[-] Issue while deleting nodes with label {}".format(node_label))
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
      LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
      CREATE (%s)
    """ % (csv, schema)
    logger.info("[+] Starting CSV import into Neo4j ...")
    try:
        r = graph.run(q)
        logger.info("[+] CSV import complete.")
        logger.info(r.stats())
    except Exception, e:
        logger.error("[-] Issue while loading CSV file")
        logger.error("[-] {}".format( str(e) ))
        sys.exit(1)
        
        
#==============================================================================
# HELPERS FOR RELATIONSHIPS EXPORT
#==============================================================================

def build_relationships_schema(dataset=None):
    ds = dataiku.Dataset(dataset)
    schema = ', '.join( ["line[{}] AS {}".format(i, r["name"]) for i, r in enumerate(ds.read_schema())] )
    logger.info("[+] Schema generation complete for relationships CSV file.")
    return schema


def create_relationships_from_csv(graph=None, csv=None, schema=None):
    q = """
      USING PERIODIC COMMIT
      LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
      WITH %s
      MATCH (f:%s {%s: %s})
      MATCH (t:%s {%s: %s})
      MERGE (f)-[rel:%s]->(t)
    """ % (
        csv, 
        schema, 
        graph_nodes_left_label, graph_nodes_left_key, graph_relationships_left_key,
        graph_nodes_right_label, graph_nodes_right_key, graph_relationships_right_key,
        graph_relationships_verb
    )
    logger.info("[+] Start CSV import into Neo4j using query:...")
    logger.info("[+] %s" % (q))
    try:
        r = graph.run(q)
        logger.info("[+] CSV import complete.")
        logger.info(r.stats())
    except Exception, e:
        logger.error("[-] Issue while loading CSV")
        logger.error("[-] {}".format(str(e)))