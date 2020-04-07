import dataiku
from flask import request
from dku_neo4j.visualization import NeoVisConfig
import pandas as pd
import numpy as np
import json
import traceback
import logging
from dku_neo4j.visualization import GraphError
from dataiku.customwebapp import get_plugin_config

logger = logging.getLogger(__name__)


def parse_filter(filters_list):
    parsed_filters = []
    for filter in filters_list:
        if filter["filterType"] == "NUMERICAL_FACET":
            parsed_filters += [(filter["column"], filter["minValue"], filter["maxValue"])]
        else:
            raise GraphError("Can only filter on numerical properties")
    return parsed_filters

@app.route("/reformat_data")
def reformat_data():
    try:
        logger.warning("coucou 0 ----------")
        plugin_config = get_plugin_config()
        logger.info("plugin_config: ", plugin_config)
        webAppConfig = request.args.get("webAppConfig")
        filters = request.args.get("filters")

        dict_config = json.loads(webAppConfig)
        filters_list = json.loads(filters)

        logger.info("filters: ", filters_list)

        parsed_filters = parse_filter(filters_list)

        logger.info("parsed_filters: ", parsed_filters)

        config = NeoVisConfig(plugin_config, dict_config, parsed_filters)

        config_dump = json.dumps(config.__dict__)

        return config_dump
    except GraphError as ge:
        logger.error(traceback.format_exc())
        return str(ge), 505
    except Exception as e:
        logger.error(traceback.format_exc())
        return str(e), 500
