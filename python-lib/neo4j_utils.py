import dataiku
from subprocess import Popen, PIPE

def export_dataset(dataset=None, output_file=None, format="tsv-excel-noheader"):
    '''
    This function exports a Dataiku Dataset to CSV with no 
    need to go through a Pandas dataframe first
    '''
    ds = dataiku.Dataset(dataset)
    with open(output_file, "w") as o:
        with ds.raw_formatted_data(format=format) as i:
            while True:
                chunk = i.read(32000)
            if len(chunk) == 0:
                break
            o.write(chunk)
    
    
def scp_nopassword_to_server(file_to_copy=None, sshuser=None, sshhost=None, sshpath=None):
    '''
    This function copies a file to a remote server using SCP. 
    It requires a password-less access (i.e SSH public key is available)
    '''
    p = Popen(
        ["scp", file_to_copy, "{}@{}:{}".format(sshuser, sshhost, sshpath)], 
        stdin=PIPE, stdout=PIPE, stderr=PIPE
    )
    out, err = p.communicate()
    return out, err


def build_node_schema(node_label=None, dataset=None):
    '''
    This specific function generates the "schema" for a 
    node from a Dataiku Dataset
    '''
    schema = ''
    schema = schema + ':{}'.format(node_label)
    schema = schema + ' {' + '\n'
    c = ',\n'.join( ["  {}: line[{}]".format(r["name"], i) for i, r in enumerate(dataset.read_schema())] )
    schema = schema + c
    schema = schema + '\n' + '}'
    return schema


def delete_nodes_with_label(graph=None, node_label=None):
    '''
    Simple helper to delete all nodes with a given label within
    a Neo4j graph
    '''
    q = """
      MATCH (n:%s)
      DETACH DELETE n
    """ % (node_label)
    try:
        r = graph.run(q)
    except Exception, e:
        msg = "[-] Issue while deleting nodes with label {}".format(node_label)
        msg = msg + "[-] {}".format( str(e) )
        raise Exception(msg)
    
    