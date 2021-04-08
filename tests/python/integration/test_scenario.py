import pytest

from dku_plugin_test_utils import dss_scenario


pytestmark = pytest.mark.usefixtures("plugin", "dss_target")


test_kwargs = {"user": "data_scientist_1", "project_key": "TESTNEO4JPLUGIN"}


def test_run_neo4j(dss_user_clients):
    dss_scenario.run(dss_user_clients, scenario_id="all_test", **test_kwargs)
