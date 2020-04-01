import dataiku
from flask import request
import pandas as pd
import numpy as np
import json
import traceback
import logging
logger = logging.getLogger(__name__)


def build_complete_df(df, category_column, value_column, max_displayed_values, group_others):
    try:
        df = df.groupby([category_column], as_index=False).agg({value_column:['sum','count']})
        df.columns = [category_column, value_column+'_sum', value_column+'_count']
    except:
        raise TypeError("Cannot perform Groupby for column %s" % (category_column,))

    n = df.shape[0]

    if n > max_displayed_values:
        df = df.sort_values(by=[value_column+'_count'], ascending=False).reset_index(drop=True)
        df['rank'] = df.index
        df['rank'] = df.apply(lambda row: row['rank'] if row['rank'] < max_displayed_values-1 else max_displayed_values-1, axis=1)
        if group_others:
            df[category_column] = df.apply(lambda row: row[category_column] if row['rank'] < max_displayed_values-1 else 'others', axis=1)
            df = df.groupby(['rank'], as_index=False).agg({category_column:'min', value_column+'_sum':'sum'})
        else:
            df = df.head(max_displayed_values)

    df = df[[category_column, value_column+'_sum']]
    df.columns = [category_column, value_column]

    df = df.sort_values(by=[value_column], ascending=False).reset_index(drop=True)

    df['max'] = df[value_column]
    df[value_column] = df[value_column].shift(1)
    df.loc[0, value_column] = 0
    df[[value_column, 'max']] = df[[value_column, 'max']].cumsum()

    return df

@app.route('/reformat_data')
def reformat_data():
    try:
        # x = 1/0
        relation_caption = request.args.get('relation_caption')
        logger.warning("youhou")
        print("youpiiii")
        return "test"
        return json.dumps({'result': [relation_caption]})

        # dataset_name = request.args.get('dataset_name')
        # category_column = request.args.get('category_column')
        # value_column = request.args.get('value_column')
        # max_displayed_values = int(request.args.get('max_displayed_values'))
        # group_others = True if request.args.get('group_others') == 'true' else False

        # columns_list = [x for x in [category_column, value_column] if x is not None]

        # df = dataiku.Dataset(dataset_name).get_dataframe(columns=columns_list)

        # try:
        #     df[value_column] = pd.to_numeric(df[value_column])
        # except:
        #     raise TypeError("Values must be of numerical types")

        # df = build_complete_df(df, category_column, value_column, max_displayed_values, group_others)

        # return json.dumps({'result': df.values.tolist()})
    except Exception as e:
        logger.error(traceback.format_exc())
        return str(e), 500
