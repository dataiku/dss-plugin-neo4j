from dku_neo4j.neo4j_handle import NodesExportParams
from mocking import MockNeo4jHandle, MockImportFileHandler
import pandas as pd

# import pytest


class TestNodesExport:
    def setup_method(self):
        self.recipe_config = {
            "properties_mode": "SELECT_COLUMNS",
            "property_names_mapping": True,
            "property_names_map": {
                "player_name": "name",
                "player_age": "age",
                "player_country": "country",
                "timestamp": "birthdate",
                "fee": "value",
            },
            "expert_mode": True,
            "clear_before_run": True,
            "load_from_csv": False,
            "node_properties": ["player_age", "player_country", "timestamp", "fee"],
            "nodes_label": "Player",
            "node_id_column": "player_name",
        }

        self.dataset_schema = [
            {"name": "player_name", "type": "string"},
            {"name": "timestamp", "type": "date"},
            {"name": "fee", "type": "double"},
            {"name": "player_age", "type": "int"},
            {"name": "player_country", "type": "string"},
        ]

        self.params = NodesExportParams(
            nodes_label=self.recipe_config.get("nodes_label"),
            node_id_column=self.recipe_config.get("node_id_column"),
            properties_mode=self.recipe_config.get("properties_mode"),
            node_properties=self.recipe_config.get("node_properties"),
            property_names_mapping=self.recipe_config.get("property_names_mapping"),
            property_names_map=self.recipe_config.get("property_names_map"),
            clear_before_run=self.recipe_config.get("clear_before_run", False),
            columns_list=self.dataset_schema,
        )
        self.params.check(self.dataset_schema)

    def test_add_unique_constraint_on_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.add_unique_constraint_on_nodes(self.params)
            assert neo4jhandle.queries[0] == "CREATE CONSTRAINT IF NOT EXISTS ON (n:`Player`) ASSERT n.`name` IS UNIQUE"

    def test_delete_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.delete_nodes(self.params.nodes_label)
            assert (
                neo4jhandle.queries[0]
                == """
MATCH (n:`Player`)
DETACH DELETE n
"""
            )

    def test_load_nodes_from_csv(self):
        file_handler = MockImportFileHandler()
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.load_nodes_from_csv(df_iterator, self.dataset_schema, self.params, file_handler)
            assert (
                neo4jhandle.queries[0]
                == """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///dss_neo4j_export_temp_file_001.csv.gz' AS line FIELDTERMINATOR ','
WITH line[0] AS `player_name`, line[1] AS `player_age`, line[2] AS `player_country`, line[3] AS `timestamp`, line[4] AS `fee`
MERGE (n:`Player` {`name`: `player_name`})
ON CREATE SET n.`age` = toInteger(`player_age`)
ON MATCH SET n.`age` = toInteger(`player_age`)
ON CREATE SET n.`country` = `player_country`
ON MATCH SET n.`country` = `player_country`
ON CREATE SET n.`birthdate` = datetime(`timestamp`)
ON MATCH SET n.`birthdate` = datetime(`timestamp`)
ON CREATE SET n.`value` = toFloat(`fee`)
ON MATCH SET n.`value` = toFloat(`fee`)
"""
            )

    def test_insert_nodes_by_batch(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.insert_nodes_by_batch(df_iterator, self.dataset_schema, self.params)

            assert len(neo4jhandle.queries) == len(df_iterator)
            assert (
                neo4jhandle.queries[1]
                == """
WITH $data AS dataset
UNWIND dataset AS rows
MERGE (n:`Player` {`name`: rows.`player_name`})
ON CREATE SET n.`age` = toInteger(rows.`player_age`)
ON MATCH SET n.`age` = toInteger(rows.`player_age`)
ON CREATE SET n.`country` = rows.`player_country`
ON MATCH SET n.`country` = rows.`player_country`
ON CREATE SET n.`birthdate` = datetime(rows.`timestamp`)
ON MATCH SET n.`birthdate` = datetime(rows.`timestamp`)
ON CREATE SET n.`value` = toFloat(rows.`fee`)
ON MATCH SET n.`value` = toFloat(rows.`fee`)
"""
            )

    def _create_dataframe_iterator(self):
        df = pd.DataFrame(
            {
                "player_name": ["Zidane", "Ronaldo", "Messi", "Neymar"],
                "timestamp": ["2010-02-01", "2015-06-01", "2018-02-01", "2020-06-01"],
                "fee": [122345.5, 4352.5, 3245234, 34535],
                "player_age": [32, 24, 26, 18],
                "player_country": ["France", "Portugal", "Argentina", "Brazil"],
            }
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(tz=None)
        return [df.iloc[:2], df.iloc[2:]]
