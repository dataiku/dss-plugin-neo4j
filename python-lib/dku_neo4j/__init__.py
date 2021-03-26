import logging
import pandas as pd
from neo4j import GraphDatabase


class Neo4jHandle(object):
    DATA = "data"
    ROWS = "rows"

    def __init__(self, uri, username, password):
        self.uri = uri
        self.username = username
        self.password = password

    def __enter__(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        logging.info("Neo4j plugin - Closing driver ...")
        self.driver.close()

    def check(self):
        try:
            query = "MATCH (n) RETURN n LIMIT 1"
            self.run(query)
        except Exception:
            raise ValueError("Could not connect to graph database using the preset")
        return

    def run(self, query, data=None, log_results=False):
        """Run a cypher query with a session from the driver. If some data is specified, it calls a write_transaction with the data.

        Args:
            query (str): Cypher query to execute.
            data (list of dict, optional): Data used in an unwind query. Defaults to None.
            log_results (bool, optional): Log statistics about the query execution. Defaults to False.
        """
        with self.driver.session() as session:
            if data:
                results = session.write_transaction(self.unwind_transaction, query=query, data=data)
            else:
                results = session.run(query)
        if log_results:
            logging.info(f"Neo4j plugin - Query results: {results.consume().counters}")

    def unwind_transaction(self, tx, query, data):
        results = tx.run(query, parameters={self.DATA: data})
        return results

    def delete_nodes(self, nodes_label):
        query = f"""
          MATCH (n:`{nodes_label}`)
          DETACH DELETE n
        """
        logging.info(f"Neo4j plugin - Deleting nodes: {query}")
        self.run(query, log_results=True)

    def delete_relationships(self, params):
        query = f"""
          MATCH (:`{params.source_node_label}`)-[r:`{params.relationships_verb}`]-(:`{params.target_node_label}`)
          DELETE r
        """
        logging.info("Neo4j plugin - Delete relationships: {query}")
        self.run(query, log_results=True)

    def load_nodes_from_csv(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        node_primary_key_statement = self._primary_key_statement(
            columns_list, params.node_lookup_key, params.node_id_column
        )
        properties = self._properties(columns_list, params.node_properties, "n", params.property_names_map)
        query = f"""
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///{csv_file_path}' AS line FIELDTERMINATOR '\t'
WITH {definition}
MERGE (n:`{params.nodes_label}` {node_primary_key_statement})
{properties}
"""
        logging.info(f"Neo4j plugin - Importing nodes into Neo4j: {query}")
        self.run(query, log_results=True)

    def insert_nodes_by_batch(self, df_iterator, columns_list, params):
        node_primary_key_statement = self._primary_key_statement(
            columns_list, params.node_lookup_key, params.node_id_column, unwind=True
        )
        properties = self._properties(columns_list, params.node_properties, "n", params.property_names_map, unwind=True)
        query = f"""
WITH ${self.DATA} AS dataset
UNWIND dataset AS {self.ROWS}
MERGE (n:`{params.nodes_label}` {node_primary_key_statement})
{properties}
"""
        logging.info(f"Neo4j plugin - Inserting nodes into Neo4j: {query}")
        for df in df_iterator:
            data = self._get_cleaned_data(df, mandatory_columns=[params.node_id_column])
            self.run(query, data=data, log_results=True)

    def add_unique_constraint_on_relationship_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.source_node_label, params.source_node_lookup_key)
        self._add_unique_constraint_if_not_exist(params.target_node_label, params.target_node_lookup_key)

    def add_unique_constraint_on_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.nodes_label, params.node_lookup_key)

    def _add_unique_constraint_if_not_exist(self, label, property_key):
        query = f"CREATE CONSTRAINT IF NOT EXISTS ON (n:`{label}`) ASSERT n.`{property_key}` IS UNIQUE"
        logging.info(f"Neo4j plugin - Creating uniqueness constraint on {label}.{property_key}")
        self.run(query, log_results=True)

    def load_relationships_from_csv(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        source_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.source_node_lookup_key, params.source_node_id_column
        )
        target_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.target_node_lookup_key, params.target_node_id_column
        )
        node_incremented_property = "count" if params.node_count_property else None
        edge_incremented_property = "weight" if params.edge_weight_property else None
        source_node_properties = self._properties(
            columns_list,
            params.source_node_properties,
            "src",
            params.property_names_map,
            incremented_property=node_incremented_property,
        )
        target_node_properties = self._properties(
            columns_list,
            params.target_node_properties,
            "tgt",
            params.property_names_map,
            incremented_property=node_incremented_property,
        )
        relationship_properties = self._properties(
            columns_list,
            params.relationship_properties,
            "rel",
            params.property_names_map,
            incremented_property=edge_incremented_property,
        )
        query = f"""
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///{csv_file_path}' AS line FIELDTERMINATOR '\t'
WITH {definition}
MERGE (src:`{params.source_node_label}` {source_node_primary_key_statement})
{source_node_properties}
MERGE (tgt:`{params.target_node_label}` {target_node_primary_key_statement})
{target_node_properties}
MERGE (src)-[rel:`{params.relationships_verb}`]->(tgt)
{relationship_properties}
"""
        logging.info(f"Neo4j plugin - Import relationships and nodes into Neo4j: {query}")
        self.run(query, log_results=True)

    def insert_relationships_by_batch(self, df_iterator, columns_list, params):
        node_incremented_property = "count" if params.node_count_property else None
        edge_incremented_property = "weight" if params.edge_weight_property else None
        source_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.source_node_lookup_key, params.source_node_id_column, unwind=True
        )
        target_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.target_node_lookup_key, params.target_node_id_column, unwind=True
        )
        source_node_properties = self._properties(
            columns_list,
            params.source_node_properties,
            "src",
            params.property_names_map,
            incremented_property=node_incremented_property,
            unwind=True,
        )
        target_node_properties = self._properties(
            columns_list,
            params.target_node_properties,
            "tgt",
            params.property_names_map,
            incremented_property=node_incremented_property,
            unwind=True,
        )
        relationship_properties = self._properties(
            columns_list,
            params.relationship_properties,
            "rel",
            params.property_names_map,
            incremented_property=edge_incremented_property,
            unwind=True,
        )
        query = f"""
WITH ${self.DATA} AS dataset
UNWIND dataset AS {self.ROWS}
MERGE (src:`{params.source_node_label}` {source_node_primary_key_statement})
{source_node_properties}
MERGE (tgt:`{params.target_node_label}` {target_node_primary_key_statement})
{target_node_properties}
MERGE (src)-[rel:`{params.relationships_verb}`]->(tgt)
{relationship_properties}
"""
        logging.info(f"Neo4j plugin - Inserting nodes into Neo4j: {query}")
        for df in df_iterator:
            data = self._get_cleaned_data(
                df, mandatory_columns=[params.source_node_id_column, params.target_node_id_column]
            )
            self.run(query, data=data, log_results=True)

    def _build_nodes_definition(self, nodes_label, columns_list):
        definition = ":{}".format(nodes_label)
        definition += " {" + "\n"
        definition += ",\n".join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        definition += "\n" + "}"
        return definition

    def _schema(self, columns_list):
        return ", ".join(["line[{}] AS `{}`".format(i, c["name"]) for i, c in enumerate(columns_list)])

    def _properties(
        self, all_columns_list, properties_list, identifier, property_names_map, incremented_property=None, unwind=False
    ):
        type_per_column = {}
        for c in all_columns_list:
            type_per_column[c["name"]] = c["type"]
        properties_strings = []
        for colname in properties_list:
            if colname in property_names_map:
                neo4j_property_name = property_names_map[colname]
            else:
                neo4j_property_name = colname
            propstr = self._property(colname, neo4j_property_name, type_per_column[colname], identifier, unwind=unwind)
            properties_strings.append(propstr)
        if incremented_property:
            incremented_property_statement = f"ON CREATE SET {identifier}.{incremented_property} = 1"
            incremented_property_statement += (
                f"\nON MATCH SET {identifier}.{incremented_property} = {identifier}.{incremented_property} + 1"
            )
            properties_strings.append(incremented_property_statement)
        return "\n".join(properties_strings)

    def _primary_key_statement(self, all_columns_list, node_lookup_key, node_id_column, unwind=False):
        """Create a node merge statement in the form of '{node_lookup_key: node_id_column}'"""
        node_id_column_type = next((c["type"] for c in all_columns_list if c["name"] == node_id_column), None)
        typed_value = self._cast_property_type(node_id_column, node_id_column_type, unwind)
        return f"{{`{node_lookup_key}`: {typed_value}}}"

    def _property(self, colname, prop, coltype, identifier, unwind=False):
        typedValue = self._cast_property_type(colname, coltype, unwind)
        oncreate = f"ON CREATE SET {identifier}.`{prop}` = {typedValue}"
        onmatch = f"ON MATCH SET {identifier}.`{prop}` = {typedValue}"
        return oncreate + "\n" + onmatch

    def _cast_property_type(self, colname, coltype, unwind):
        if unwind:
            colname_reference = f"{self.ROWS}.`{colname}`"
        else:
            colname_reference = f"`{colname}`"
        if coltype in ["int", "bigint", "smallint", "tinyint"]:
            typedValue = f"toInteger({colname_reference})"
        elif coltype in ["double", "float"]:
            typedValue = f"toFloat({colname_reference})"
        elif coltype == "boolean":
            typedValue = f"toBoolean({colname_reference})"
        elif coltype == "date":
            typedValue = f"datetime({colname_reference})"
        else:
            typedValue = colname_reference
        return typedValue

    def _get_cleaned_data(self, df, mandatory_columns=None):
        """Make sure primary key columns don't have missing values and remove missing values from other properties columns"""
        if mandatory_columns:
            if df[mandatory_columns].isnull().any().any():
                raise ValueError(f"The primary key columns {mandatory_columns} cannot have missing values.")
        data = self._remove_nan_values_from_records(df.to_dict(orient="records"))
        return data

    def _remove_nan_values_from_records(self, data):
        return [{k: v for k, v in row.items() if not pd.isnull(v)} for row in data]


class NodesExportParams(object):
    def __init__(
        self,
        nodes_label,
        node_id_column,
        properties_mode,
        node_properties,
        property_names_mapping,
        property_names_map,
        clear_before_run=False,
        columns_list=None,
    ):
        self.nodes_label = nodes_label
        self.node_id_column = node_id_column
        self.properties_mode = properties_mode
        self.node_properties = node_properties or []
        self.property_names_map = property_names_map or {} if property_names_mapping else {}
        self.clear_before_run = clear_before_run

        if properties_mode == "SELECT_COLUMNS":
            if node_id_column in node_properties:
                self.node_properties.remove(node_id_column)
        else:
            self.node_properties = [col["name"] for col in columns_list if col["name"] != self.node_id_column]

        if node_id_column in property_names_map:
            self.node_lookup_key = property_names_map[node_id_column]
        else:
            self.node_lookup_key = node_id_column

        self.used_columns = [self.node_id_column] + self.node_properties

    def check(self, input_dataset_schema):
        if self.nodes_label is None:
            raise ValueError("nodes_label not specified")
        if self.node_id_column is None:
            raise ValueError("Primary key not specified")
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.properties_mode == "SELECT_COLUMNS":
            for colname in self.node_properties:
                if colname and colname not in existing_colnames:
                    raise ValueError("node_properties. Column does not exist in input dataset: " + str(colname))
        # TODO sanitize label and properties name entered by user (remove `back tick`)


class RelationshipsExportParams(object):
    def __init__(
        self,
        source_node_label,
        source_node_id_column,
        source_node_properties,
        target_node_label,
        target_node_id_column,
        target_node_properties,
        relationships_verb,
        relationship_properties,
        property_names_mapping,
        property_names_map,
        clear_before_run=False,
        node_count_property=False,
        edge_weight_property=False,
    ):

        self.source_node_label = source_node_label
        self.source_node_id_column = source_node_id_column
        self.source_node_properties = source_node_properties or []
        self.target_node_label = target_node_label
        self.target_node_id_column = target_node_id_column
        self.target_node_properties = target_node_properties or []
        self.relationships_verb = relationships_verb
        self.relationship_properties = relationship_properties
        self.property_names_map = property_names_map or {} if property_names_mapping else {}
        self.clear_before_run = clear_before_run
        self.node_count_property = node_count_property
        self.edge_weight_property = edge_weight_property

        if source_node_id_column in source_node_properties:
            self.source_node_properties.remove(source_node_id_column)
        if source_node_id_column in property_names_map:
            self.source_node_lookup_key = property_names_map[source_node_id_column]
        else:
            self.source_node_lookup_key = source_node_id_column

        if target_node_id_column in target_node_properties:
            self.target_node_properties.remove(target_node_id_column)
        if target_node_id_column in property_names_map:
            self.target_node_lookup_key = property_names_map[target_node_id_column]
        else:
            self.target_node_lookup_key = target_node_id_column

        self.used_columns = list(
            set(
                [self.source_node_id_column, self.target_node_id_column]
                + self.source_node_properties
                + self.target_node_properties
                + self.relationship_properties
            )
        )

    def check(self, input_dataset_schema):
        if self.source_node_label is None or self.source_node_label == "":
            raise ValueError("Source nodes label not specified")
        if self.target_node_label is None or self.target_node_label == "":
            raise ValueError("Target nodes label not specified")
        if self.source_node_id_column is None or self.source_node_id_column == "":
            raise ValueError("Source nodes primary key not specified")
        if self.target_node_id_column is None or self.target_node_id_column == "":
            raise ValueError("Target nodes primary key not specified")
        if self.relationships_verb is None or self.relationships_verb == "":
            raise ValueError("Relationships type not specified")
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.source_node_id_column not in existing_colnames:
            raise ValueError(
                "Source nodes primary key. Column does not exist in input dataset: " + str(self.source_node_id_column)
            )
        if self.target_node_id_column not in existing_colnames:
            raise ValueError(
                "Target nodes primary key. Column does not exist in input dataset: " + str(self.target_node_id_column)
            )
        for colname in self.source_node_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Source nodes properties. Column does not exist in input dataset: " + str(colname))
        for colname in self.target_node_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Target nodes properties. Column does not exist in input dataset: " + str(colname))
        for colname in self.relationship_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Relationship properties. Column does not exist in input dataset: " + str(colname))
