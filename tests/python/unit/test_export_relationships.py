from dku_neo4j.neo4j_handle import RelationshipsExportParams
from mocking import MockNeo4jHandle, MockImportFileHandler, compare_queries
import pandas as pd
import copy


class TestRelationshipsExport:
    def setup_method(self):
        self.recipe_config = {
            "properties_mode": "SELECT_COLUMNS",
            "property_names_mapping": True,
            "property_names_map": {
                "player_name": "name",
                "club_name": "name",
                "club_country": "country",
                "timestamp": "transfer_date",
                "fee": "transfer_fee",
            },
            "expert_mode": True,
            "clear_before_run": True,
            "load_from_csv": False,
            "node_properties": ["player_age", "player_country", "timestamp", "fee"],
            "source_node_label": "Player",
            "relationships_verb": "TRANSFER_TO",
            "target_node_label": "Club",
            "source_node_id_column": "player_name",
            "target_node_id_column": "club_name",
            "node_count_property": False,
            "edge_weight_property": True,
            "source_node_properties": [],
            "target_node_properties": ["club_name", "club_country"],
            "relationship_properties": ["timestamp", "fee", "player_age"],
            "na_values": [],
            "keep_default_na": False,
        }

        self.dataset_schema = [
            {"name": "player_name", "type": "string"},
            {"name": "timestamp", "type": "date"},
            {"name": "fee", "type": "double"},
            {"name": "player_age", "type": "int"},
            {"name": "club_name", "type": "string"},
            {"name": "club_country", "type": "string"},
        ]

        self.params = RelationshipsExportParams(
            source_node_label=self.recipe_config.get("source_node_label"),
            source_node_id_column=self.recipe_config.get("source_node_id_column"),
            source_node_properties=self.recipe_config.get("source_node_properties"),
            target_node_label=self.recipe_config.get("target_node_label"),
            target_node_id_column=self.recipe_config.get("target_node_id_column"),
            target_node_properties=self.recipe_config.get("target_node_properties"),
            relationships_verb=self.recipe_config.get("relationships_verb"),
            relationship_id_column=self.recipe_config.get("relationship_id_column"),
            relationship_properties=self.recipe_config.get("relationship_properties"),
            property_names_mapping=self.recipe_config.get("property_names_mapping"),
            property_names_map=self.recipe_config.get("property_names_map"),
            expert_mode=self.recipe_config.get("expert_mode"),
            clear_before_run=self.recipe_config.get("clear_before_run", False),
            node_count_property=self.recipe_config.get("node_count_property", False),
            edge_weight_property=self.recipe_config.get("edge_weight_property", False),
            na_values=self.recipe_config.get("na_values"),
            keep_default_na=self.recipe_config.get("keep_default_na", True),
        )
        self.params.check(self.dataset_schema)
        self.params.set_periodic_commit(500)

    def test_add_unique_constraint_on_relationship_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.add_unique_constraint_on_relationship_nodes(self.params)
            reference_query = """
CREATE CONSTRAINT IF NOT EXISTS ON (n:`Club`)
ASSERT n.`name` IS UNIQUE
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

    def test_delete_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.delete_nodes(self.params.target_node_label, batch_size=500)
            reference_query = """
CALL apoc.periodic.iterate("MATCH (n:`Club`) return n", "DETACH DELETE n", {batchSize:500})
YIELD batches, total RETURN batches, total
"""
            compare_queries(neo4jhandle.queries[0], reference_query)

    def test_load_relationships_from_csv(self):
        file_handler = MockImportFileHandler()
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            print(f"self.params.used_columns: {self.params.used_columns}")
            neo4jhandle.load_relationships_from_csv(df_iterator, self.dataset_schema, self.params, file_handler)
            print(f"neo4jhandle.queries[0]:\n{neo4jhandle.queries[0]}")
            reference_query = """
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///dss_neo4j_export_temp_file_001.csv.gz' AS line FIELDTERMINATOR ','
WITH line[0] AS `club_country`, line[1] AS `club_name`, line[2] AS `fee`, line[3] AS `player_age`, line[4] AS `player_name`, line[5] AS `timestamp`
MERGE (src:`Player` {`name`: `player_name`})

MERGE (tgt:`Club` {`name`: `club_name`})
ON CREATE SET tgt.`country` = `club_country`
ON MATCH SET tgt.`country` = `club_country`
MERGE (src)-[rel:`TRANSFER_TO`]->(tgt)
ON CREATE SET rel.`transfer_date` = datetime(`timestamp`)
ON MATCH SET rel.`transfer_date` = datetime(`timestamp`)
ON CREATE SET rel.`transfer_fee` = toFloat(`fee`)
ON MATCH SET rel.`transfer_fee` = toFloat(`fee`)
ON CREATE SET rel.`player_age` = toInteger(`player_age`)
ON MATCH SET rel.`player_age` = toInteger(`player_age`)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
"""
            compare_queries(neo4jhandle.queries[0], reference_query)

    def test_insert_relationships_by_batch(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.insert_relationships_by_batch(df_iterator, self.dataset_schema, self.params)
            print(f"neo4jhandle.queries[1]: {neo4jhandle.queries[1]}")
            assert len(neo4jhandle.queries) == len(df_iterator)

            reference_query = """
WITH $data AS dataset
UNWIND dataset AS rows
MERGE (src:`Player` {`name`: rows.`player_name`})

MERGE (tgt:`Club` {`name`: rows.`club_name`})
ON CREATE SET tgt.`country` = rows.`club_country`
ON MATCH SET tgt.`country` = rows.`club_country`
MERGE (src)-[rel:`TRANSFER_TO`]->(tgt)
ON CREATE SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON MATCH SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON CREATE SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON MATCH SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON CREATE SET rel.`player_age` = toInteger(rows.`player_age`)
ON MATCH SET rel.`player_age` = toInteger(rows.`player_age`)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

    def test_insert_relationships_by_batch_skip_row_if_not_both_nodes(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            params_temp = copy.copy(self.params)
            params_temp.skip_row_if_not_source = True
            params_temp.skip_row_if_not_target = True
            neo4jhandle.insert_relationships_by_batch(df_iterator, self.dataset_schema, params_temp)
            print(f"neo4jhandle.queries[1]: {neo4jhandle.queries[1]}")
            assert len(neo4jhandle.queries) == len(df_iterator)
            reference_query = """
WITH $data AS dataset
UNWIND dataset AS rows
MATCH (src:`Player` {`name`: rows.`player_name`})
MATCH (tgt:`Club` {`name`: rows.`club_name`})

SET tgt.`country` = rows.`club_country`
MERGE (src)-[rel:`TRANSFER_TO`]->(tgt)
ON CREATE SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON MATCH SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON CREATE SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON MATCH SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON CREATE SET rel.`player_age` = toInteger(rows.`player_age`)
ON MATCH SET rel.`player_age` = toInteger(rows.`player_age`)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

    def test_insert_relationships_by_batch_skip_row_if_not_source_nodes(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            params_temp = copy.copy(self.params)
            params_temp.skip_row_if_not_source = True
            neo4jhandle.insert_relationships_by_batch(df_iterator, self.dataset_schema, params_temp)
            print(f"neo4jhandle.queries[1]: {neo4jhandle.queries[1]}")
            assert len(neo4jhandle.queries) == len(df_iterator)
            reference_query = """
WITH $data AS dataset
UNWIND dataset AS rows
MATCH (src:`Player` {`name`: rows.`player_name`})

MERGE (tgt:`Club` {`name`: rows.`club_name`})
ON CREATE SET tgt.`country` = rows.`club_country`
ON MATCH SET tgt.`country` = rows.`club_country`
MERGE (src)-[rel:`TRANSFER_TO`]->(tgt)
ON CREATE SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON MATCH SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON CREATE SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON MATCH SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON CREATE SET rel.`player_age` = toInteger(rows.`player_age`)
ON MATCH SET rel.`player_age` = toInteger(rows.`player_age`)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

    def test_insert_relationships_by_batch_skip_row_if_not_target_nodes(self):
        df_iterator = self._create_dataframe_iterator()
        with MockNeo4jHandle() as neo4jhandle:
            params_temp = copy.copy(self.params)
            params_temp.skip_row_if_not_target = True
            neo4jhandle.insert_relationships_by_batch(df_iterator, self.dataset_schema, params_temp)
            print(f"neo4jhandle.queries[1]: {neo4jhandle.queries[1]}")
            assert len(neo4jhandle.queries) == len(df_iterator)
            reference_query = """
WITH $data AS dataset
UNWIND dataset AS rows
MATCH (tgt:`Club` {`name`: rows.`club_name`})
SET tgt.`country` = rows.`club_country`
MERGE (src:`Player` {`name`: rows.`player_name`})

MERGE (src)-[rel:`TRANSFER_TO`]->(tgt)
ON CREATE SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON MATCH SET rel.`transfer_date` = datetime(rows.`timestamp`)
ON CREATE SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON MATCH SET rel.`transfer_fee` = toFloat(rows.`fee`)
ON CREATE SET rel.`player_age` = toInteger(rows.`player_age`)
ON MATCH SET rel.`player_age` = toInteger(rows.`player_age`)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
"""
            compare_queries(neo4jhandle.queries[1], reference_query)

    def _create_dataframe_iterator(self):
        df = pd.DataFrame(
            {
                "player_name": ["Zidane", "Ronaldo", "Messi", "Neymar"],
                "timestamp": ["2010-02-01", "2015-06-01", "2018-02-01", "2020-06-01"],
                "fee": [122345.5, 4352.5, 3245234, 34535],
                "player_age": [32, 24, 26, 18],
                "club_name": ["Real Madrid", "Real Madrid", "Barcelona", "PSG"],
                "club_country": ["Spain", "Spain", "Spain", "France"],
            }
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(tz=None)
        return [df.iloc[:2], df.iloc[2:]]
