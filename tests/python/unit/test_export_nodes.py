from dku_neo4j.neo4j_handle import NodesExportParams
from mocking import MockNeo4jHandle, MockImportFileHandler, compare_queries
import pandas as pd


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
            "na_values": [],
            "keep_default_na": False,
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
            na_values=self.recipe_config.get("na_values"),
            keep_default_na=self.recipe_config.get("keep_default_na", True),
        )
        self.params.check(self.dataset_schema)
        self.params.set_periodic_commit(500)

    def test_add_unique_constraint_on_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.add_unique_constraint_on_nodes(self.params)
            reference_query = """
CREATE CONSTRAINT IF NOT EXISTS ON (n:`Player`)
ASSERT n.`name` IS UNIQUE
"""
            compare_queries(neo4jhandle.queries[0], reference_query)

    def test_delete_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.delete_nodes(self.params.nodes_label)
            reference_query = """
CALL apoc.periodic.iterate("MATCH (n:`Player`) return n", "DETACH DELETE n", {batchSize:1000})
YIELD batches, total RETURN batches, total
"""
            compare_queries(neo4jhandle.queries[0], reference_query)

    def test_load_nodes_from_csv(self):
        file_handler = MockImportFileHandler()
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.load_nodes_from_csv(df_iterator, self.dataset_schema, self.params, file_handler)
            reference_query = """
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///dss_neo4j_export_temp_file_001.csv.gz' AS line FIELDTERMINATOR ','
WITH line[0] AS `player_name`, line[1] AS `player_age`, line[2] AS `player_country`, line[3] AS `timestamp`, line[4] AS `fee`
MERGE (src:`Player` {`name`: `player_name`})
ON CREATE SET src.`age` = toInteger(`player_age`)
ON MATCH SET src.`age` = toInteger(`player_age`)
ON CREATE SET src.`country` = `player_country`
ON MATCH SET src.`country` = `player_country`
ON CREATE SET src.`birthdate` = datetime(`timestamp`)
ON MATCH SET src.`birthdate` = datetime(`timestamp`)
ON CREATE SET src.`value` = toFloat(`fee`)
ON MATCH SET src.`value` = toFloat(`fee`)
"""
            compare_queries(neo4jhandle.queries[0], reference_query)

    def test_insert_nodes_by_batch(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.insert_nodes_by_batch(df_iterator, self.dataset_schema, self.params)
            assert len(neo4jhandle.queries) == len(df_iterator)
            reference_query = """
WITH $data AS dataset
UNWIND dataset AS rows
MERGE (src:`Player` {`name`: rows.`player_name`})
ON CREATE SET src.`age` = toInteger(rows.`player_age`)
ON MATCH SET src.`age` = toInteger(rows.`player_age`)
ON CREATE SET src.`country` = rows.`player_country`
ON MATCH SET src.`country` = rows.`player_country`
ON CREATE SET src.`birthdate` = datetime(rows.`timestamp`)
ON MATCH SET src.`birthdate` = datetime(rows.`timestamp`)
ON CREATE SET src.`value` = toFloat(rows.`fee`)
ON MATCH SET src.`value` = toFloat(rows.`fee`)
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

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