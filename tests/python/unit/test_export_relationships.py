from dku_neo4j import RelationshipsExportParams
from mocking import MockNeo4jHandle
import pandas as pd

# import pytest


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
            self.recipe_config.get("source_node_label"),
            self.recipe_config.get("source_node_id_column"),
            self.recipe_config.get("source_node_properties"),
            self.recipe_config.get("target_node_label"),
            self.recipe_config.get("target_node_id_column"),
            self.recipe_config.get("target_node_properties"),
            self.recipe_config.get("relationships_verb"),
            self.recipe_config.get("relationship_properties"),
            self.recipe_config.get("property_names_mapping"),
            self.recipe_config.get("property_names_map"),
            self.recipe_config.get("clear_before_run", False),
            self.recipe_config.get("node_count_property", False),
            self.recipe_config.get("edge_weight_property", False),
        )
        self.params.check(self.dataset_schema)

    def test_add_unique_constraint_on_relationship_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.add_unique_constraint_on_relationship_nodes(self.params)
            assert neo4jhandle.queries[1] == "CREATE CONSTRAINT IF NOT EXISTS ON (n:`Club`) ASSERT n.`name` IS UNIQUE"

    def test_delete_nodes(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.delete_nodes(self.params.target_node_label)
            assert (
                neo4jhandle.queries[0]
                == """
MATCH (n:`Club`)
DETACH DELETE n
"""
            )

    def test_load_nodes_from_csv(self):
        with MockNeo4jHandle() as neo4jhandle:
            neo4jhandle.load_relationships_from_csv("mock_path", self.dataset_schema, self.params)
            print(neo4jhandle.queries[0])
            assert (
                neo4jhandle.queries[0]
                == """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///mock_path' AS line FIELDTERMINATOR '	'
WITH line[0] AS `player_name`, line[1] AS `timestamp`, line[2] AS `fee`, line[3] AS `player_age`, line[4] AS `club_name`, line[5] AS `club_country`
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
            )

    def test_insert_relationships_by_batch(self):
        with MockNeo4jHandle() as neo4jhandle:
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

            df_iterator = [df.iloc[:2], df.iloc[2:]]

            neo4jhandle.insert_relationships_by_batch(df_iterator, self.dataset_schema, self.params)

            assert len(neo4jhandle.queries) == len(df_iterator)
            assert (
                neo4jhandle.queries[1]
                == """
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
            )
