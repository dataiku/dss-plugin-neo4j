# This file is the actual code for the Python backend of your webapp neo4j_neo4j-browser

import dataiku
import pandas as pd


# Example:
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

@app.route('/first_api_call')
def first_call():
    mydataset = dataiku.Dataset("REPLACE_WITH_YOUR_DATASET_NAME")
    mydataset_df = mydataset.get_dataframe(sampling='head', limit=500)

    #Pandas dataFrames are not directly JSON serializable, use to_json()
    data = mydataset_df.to_json()
    return json.dumps({"status": "ok", "data": data})
