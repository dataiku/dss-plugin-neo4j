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
    p = Popen(
        ["scp", file_to_copy, "{}@{}:{}".format(sshuser, sshhost, sshpath)], 
        stdin=PIPE, stdout=PIPE, stderr=PIPE
    )
    out, err = p.communicate()