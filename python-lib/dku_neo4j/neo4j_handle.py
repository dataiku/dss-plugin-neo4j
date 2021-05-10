import logging
import pandas as pd
from neo4j import GraphDatabase
from dku_neo4j.query_templates import (
    LOAD_FROM_CSV_PREFIX,
    UNWIND_PREFIX,
    EXPORT_NODES_SUFFIX,
    EXPORT_RELATIONSHIPS_SUFFIX,
    EXPORT_RELATIONSHIPS_EXISTING_NODES_ONLY_SUFFIX,
    BATCH_DELETE_NODES,
)


class Neo4jHandle(object):
    DATA = "data"
    ROWS = "rows"

    def __init__(self, uri, username, password):
        self.uri = uri
        self.username = username
        self.password = password

    def __enter__(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        except Exception as e:
            raise Exception(f"Failed to connect to the Neo4j server. Please check your preset credentials and URI.")
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

    def delete_nodes(self, nodes_label, batch_size=1000):
        query = BATCH_DELETE_NODES.format(nodes_label=nodes_label, batch_size=batch_size)
        logging.info(f"Neo4j plugin - Deleting nodes: {query}")
        self.run(query, log_results=True)

    def load_nodes_from_csv(self, df_iterator, columns_list, params, file_handler):
        definition = self._schema(params.used_columns)
        node_primary_key_statement = self._primary_key_statement(
            columns_list, params.node_lookup_key, params.node_id_column
        )
        properties = self._properties(columns_list, params.node_properties, "n", params.property_names_map)
        for index, df in enumerate(df_iterator):
            self._check_no_empty_primary_key(df, mandatory_columns=[params.node_id_column])
            local_path = f"dss_neo4j_export_temp_file_{index+1:03}.csv.gz"
            import_file_path = file_handler.write(df, local_path)
            query_template = LOAD_FROM_CSV_PREFIX + EXPORT_NODES_SUFFIX
            query = query_template.format(
                periodic_commit=params.periodic_commit,
                import_file_path=import_file_path,
                definition=definition,
                nodes_label=params.nodes_label,
                node_primary_key_statement=node_primary_key_statement,
                properties=properties,
            )

            if index == 0:
                logging.info(f"Neo4j plugin - Importing nodes into Neo4j: {query}")
            else:
                logging.info(f"Neo4j plugin - Same query using file: {import_file_path}")
            self.run(query, log_results=True)
            file_handler.delete(local_path)

    def insert_nodes_by_batch(self, df_iterator, columns_list, params):
        node_primary_key_statement = self._primary_key_statement(
            columns_list, params.node_lookup_key, params.node_id_column, unwind=True
        )
        properties = self._properties(columns_list, params.node_properties, "n", params.property_names_map, unwind=True)
        query_template = UNWIND_PREFIX + EXPORT_NODES_SUFFIX
        query = query_template.format(
            data=self.DATA,
            rows=self.ROWS,
            nodes_label=params.nodes_label,
            node_primary_key_statement=node_primary_key_statement,
            properties=properties,
        )

        logging.info(f"Neo4j plugin - Inserting nodes into Neo4j: {query}")
        rows_processed = 0
        for df in df_iterator:
            rows_processed += len(df.index)
            data = self._get_cleaned_data(df, mandatory_columns=[params.node_id_column])
            self.run(query, data=data, log_results=True)
            logging.info(f"Neo4j plugin - Processed rows: {rows_processed}")

    def add_unique_constraint_on_relationship_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.source_node_label, params.source_node_lookup_key)
        self._add_unique_constraint_if_not_exist(params.target_node_label, params.target_node_lookup_key)

    def add_unique_constraint_on_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.nodes_label, params.node_lookup_key)

    def _add_unique_constraint_if_not_exist(self, label, property_key):
        query = f"CREATE CONSTRAINT IF NOT EXISTS ON (n:`{label}`) ASSERT n.`{property_key}` IS UNIQUE"
        logging.info(f"Neo4j plugin - Creating uniqueness constraint on {label}.{property_key}")
        self.run(query, log_results=True)

    def load_relationships_from_csv(self, df_iterator, columns_list, params, file_handler):
        definition = self._schema(params.used_columns)
        source_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.source_node_lookup_key, params.source_node_id_column
        )
        target_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.target_node_lookup_key, params.target_node_id_column
        )

        relationship_primary_key_statement = ""
        if params.relationship_id_column:
            relationship_primary_key_statement = self._primary_key_statement(
                columns_list, params.relationship_lookup_key, params.relationship_id_column
            )

        node_incremented_property = "count" if params.node_count_property else None
        edge_incremented_property = "weight" if params.edge_weight_property else None
        source_node_properties = self._properties(
            columns_list,
            params.source_node_properties,
            "src",
            params.property_names_map,
            incremented_property=node_incremented_property,
            existing_nodes_only=params.existing_nodes_only,
        )
        target_node_properties = self._properties(
            columns_list,
            params.target_node_properties,
            "tgt",
            params.property_names_map,
            incremented_property=node_incremented_property,
            existing_nodes_only=params.existing_nodes_only,
        )
        relationship_properties = self._properties(
            columns_list,
            params.relationship_properties,
            "rel",
            params.property_names_map,
            incremented_property=edge_incremented_property,
        )
        for i, df in enumerate(df_iterator):
            self._check_no_empty_primary_key(
                df, mandatory_columns=[params.source_node_id_column, params.target_node_id_column]
            )
            local_path = f"dss_neo4j_export_temp_file_{i+1:03}.csv.gz"
            import_file_path = file_handler.write(df, local_path)
            query_template = LOAD_FROM_CSV_PREFIX
            if params.existing_nodes_only:
                query_template += EXPORT_RELATIONSHIPS_EXISTING_NODES_ONLY_SUFFIX
            else:
                query_template += EXPORT_RELATIONSHIPS_SUFFIX
            query = query_template.format(
                periodic_commit=params.periodic_commit,
                import_file_path=import_file_path,
                definition=definition,
                source_node_label=params.source_node_label,
                source_node_primary_key_statement=source_node_primary_key_statement,
                source_node_properties=source_node_properties,
                target_node_label=params.target_node_label,
                target_node_primary_key_statement=target_node_primary_key_statement,
                target_node_properties=target_node_properties,
                relationships_verb=params.relationships_verb,
                relationship_primary_key_statement=relationship_primary_key_statement,
                relationship_properties=relationship_properties,
            )

            if i == 0:
                logging.info(f"Neo4j plugin - Importing relationships and nodes into Neo4j: {query}")
            else:
                logging.info(f"Neo4j plugin - Same query using file: {import_file_path}")
            self.run(query, log_results=True)
            file_handler.delete(local_path)

    def insert_relationships_by_batch(self, df_iterator, columns_list, params):
        node_incremented_property = "count" if params.node_count_property else None
        edge_incremented_property = "weight" if params.edge_weight_property else None
        source_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.source_node_lookup_key, params.source_node_id_column, unwind=True
        )
        target_node_primary_key_statement = self._primary_key_statement(
            columns_list, params.target_node_lookup_key, params.target_node_id_column, unwind=True
        )

        relationship_primary_key_statement = ""
        mandatory_columns = [params.source_node_id_column, params.target_node_id_column]
        if params.relationship_id_column:
            relationship_primary_key_statement = self._primary_key_statement(
                columns_list, params.relationship_lookup_key, params.relationship_id_column, unwind=True
            )
            mandatory_columns.append(params.relationship_id_column)

        source_node_properties = self._properties(
            columns_list,
            params.source_node_properties,
            "src",
            params.property_names_map,
            incremented_property=node_incremented_property,
            unwind=True,
            existing_nodes_only=params.existing_nodes_only,
        )
        target_node_properties = self._properties(
            columns_list,
            params.target_node_properties,
            "tgt",
            params.property_names_map,
            incremented_property=node_incremented_property,
            unwind=True,
            existing_nodes_only=params.existing_nodes_only,
        )
        relationship_properties = self._properties(
            columns_list,
            params.relationship_properties,
            "rel",
            params.property_names_map,
            incremented_property=edge_incremented_property,
            unwind=True,
        )
        query_template = UNWIND_PREFIX
        if params.existing_nodes_only:
            query_template += EXPORT_RELATIONSHIPS_EXISTING_NODES_ONLY_SUFFIX
        else:
            query_template += EXPORT_RELATIONSHIPS_SUFFIX
        query = query_template.format(
            data=self.DATA,
            rows=self.ROWS,
            source_node_label=params.source_node_label,
            source_node_primary_key_statement=source_node_primary_key_statement,
            source_node_properties=source_node_properties,
            target_node_label=params.target_node_label,
            target_node_primary_key_statement=target_node_primary_key_statement,
            target_node_properties=target_node_properties,
            relationships_verb=params.relationships_verb,
            relationship_primary_key_statement=relationship_primary_key_statement,
            relationship_properties=relationship_properties,
        )

        logging.info(f"Neo4j plugin - Inserting nodes into Neo4j: {query}")
        rows_processed = 0
        for df in df_iterator:
            rows_processed += len(df.index)
            data = self._get_cleaned_data(df, mandatory_columns=mandatory_columns)
            self.run(query, data=data, log_results=True)
            logging.info(f"Neo4j plugin - Processed rows: {rows_processed}")

    def _schema(self, columns_list):
        return ", ".join([f"line[{index}] AS `{column}`" for index, column in enumerate(columns_list)])

    def _properties(
        self,
        all_columns_list,
        properties_list,
        identifier,
        property_names_map,
        incremented_property=None,
        unwind=False,
        existing_nodes_only=False,
    ):
        type_per_column = {}
        for column in all_columns_list:
            type_per_column[column["name"]] = column["type"]
        properties_strings = []
        for colname in properties_list:
            if colname in property_names_map:
                neo4j_property_name = property_names_map[colname]
            else:
                neo4j_property_name = colname
            property_string = self._property(
                colname,
                neo4j_property_name,
                type_per_column[colname],
                identifier,
                unwind=unwind,
                existing_nodes_only=existing_nodes_only,
            )
            properties_strings.append(property_string)
        if incremented_property and not existing_nodes_only:
            incremented_property_statement = f"ON CREATE SET {identifier}.{incremented_property} = 1"
            incremented_property_statement += (
                f"\nON MATCH SET {identifier}.{incremented_property} = {identifier}.{incremented_property} + 1"
            )
            properties_strings.append(incremented_property_statement)
        return "\n".join(properties_strings)

    def _primary_key_statement(self, all_columns_list, lookup_key, id_column, unwind=False):
        """Create a merge statement in the form of '{lookup_key: id_column}'"""
        id_column_type = next((column["type"] for column in all_columns_list if column["name"] == id_column), None)
        typed_value = self._cast_property_type(id_column, id_column_type, unwind)
        return f" {{`{lookup_key}`: {typed_value}}}"

    def _property(self, colname, prop, coltype, identifier, unwind=False, existing_nodes_only=False):
        typedValue = self._cast_property_type(colname, coltype, unwind)
        set_statement = f"{identifier}.`{prop}` = {typedValue}"
        if existing_nodes_only:
            return f"SET {set_statement}"
        else:
            return f"ON CREATE SET {set_statement}\nON MATCH SET {set_statement}"

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
            self._check_no_empty_primary_key(df, mandatory_columns)
        return self._remove_nan_values_from_records(df.to_dict(orient="records"))

    def _check_no_empty_primary_key(self, df, mandatory_columns=None):
        if df[mandatory_columns].isnull().any().any():
            raise ValueError(f"The primary key columns {mandatory_columns} cannot have missing values.")

    def _remove_nan_values_from_records(self, data):
        return [{key: value for key, value in row.items() if not pd.isnull(value)} for row in data]


class ExportParams(object):
    def __init__(self):
        pass

    def set_periodic_commit(self, periodic_commit):
        self.periodic_commit = periodic_commit


class NodesExportParams(ExportParams):
    def __init__(
        self,
        nodes_label,
        node_id_column,
        properties_mode,
        node_properties,
        property_names_mapping,
        property_names_map,
        expert_mode=False,
        clear_before_run=False,
        columns_list=None,
    ):
        self.nodes_label = nodes_label
        self.node_id_column = node_id_column
        self.properties_mode = properties_mode
        self.node_properties = node_properties or []
        self.property_names_map = property_names_map or {} if property_names_mapping else {}

        self.clear_before_run = clear_before_run if expert_mode else False

        if properties_mode == "SELECT_COLUMNS":
            if node_id_column in node_properties:
                self.node_properties.remove(node_id_column)
        else:
            self.node_properties = [column["name"] for column in columns_list if column["name"] != self.node_id_column]

        if node_id_column in self.property_names_map:
            self.node_lookup_key = self.property_names_map[node_id_column]
        else:
            self.node_lookup_key = node_id_column

        self.used_columns = [self.node_id_column] + self.node_properties

    def check(self, column_list):
        existing_colnames = [column["name"] for column in column_list]

        if not self.nodes_label:
            raise ValueError("Node label is not specified.")
        check_backtick(self.nodes_label, "Node label")

        if not self.node_id_column or self.node_id_column not in existing_colnames:
            raise ValueError(f"Primary key column '{self.node_id_column}' is invalid.")

        if self.properties_mode == "SELECT_COLUMNS":
            for colname in self.node_properties:
                if colname not in existing_colnames:
                    raise ValueError(f"Node properties column '{colname}' is invalid.")

        check_property_names_map(self.property_names_map, self.used_columns)


class RelationshipsExportParams(ExportParams):
    def __init__(
        self,
        source_node_label,
        source_node_id_column,
        source_node_properties,
        target_node_label,
        target_node_id_column,
        target_node_properties,
        relationships_verb,
        relationship_id_column,
        relationship_properties,
        property_names_mapping,
        property_names_map,
        expert_mode=False,
        clear_before_run=False,
        node_count_property=False,
        edge_weight_property=False,
        existing_nodes_only=False,
    ):

        self.source_node_label = source_node_label
        self.source_node_id_column = source_node_id_column
        self.source_node_properties = source_node_properties or []
        self.target_node_label = target_node_label
        self.target_node_id_column = target_node_id_column
        self.target_node_properties = target_node_properties or []
        self.relationships_verb = relationships_verb
        self.relationship_id_column = relationship_id_column
        self.relationship_properties = relationship_properties
        self.property_names_map = property_names_map or {} if property_names_mapping else {}

        self.clear_before_run = clear_before_run if expert_mode else False
        self.node_count_property = node_count_property if expert_mode else False
        self.edge_weight_property = edge_weight_property if expert_mode else False
        self.existing_nodes_only = existing_nodes_only if expert_mode else False

        if source_node_id_column in source_node_properties:
            self.source_node_properties.remove(source_node_id_column)
        if source_node_id_column in self.property_names_map:
            self.source_node_lookup_key = self.property_names_map[source_node_id_column]
        else:
            self.source_node_lookup_key = source_node_id_column

        if target_node_id_column in target_node_properties:
            self.target_node_properties.remove(target_node_id_column)
        if target_node_id_column in self.property_names_map:
            self.target_node_lookup_key = self.property_names_map[target_node_id_column]
        else:
            self.target_node_lookup_key = target_node_id_column

        if relationship_id_column in relationship_properties:
            self.relationship_properties.remove(relationship_id_column)
        if relationship_id_column in property_names_map:
            self.relationship_lookup_key = property_names_map[relationship_id_column]
        else:
            self.relationship_lookup_key = relationship_id_column

        self.used_columns = sorted(
            list(
                set(
                    [self.source_node_id_column, self.target_node_id_column]
                    + self.source_node_properties
                    + self.target_node_properties
                    + self.relationship_properties
                )
            )
        )
        if self.relationship_id_column:
            self.used_columns.append(self.relationship_id_column)

    def check(self, column_list):
        existing_colnames = [column["name"] for column in column_list]
        if not self.source_node_label:
            raise ValueError("Source nodes label not specified")
        check_backtick(self.source_node_label, "Source node label")

        if not self.target_node_label:
            raise ValueError("Target nodes label not specified")
        check_backtick(self.target_node_label, "Target node label")

        if not self.relationships_verb:
            raise ValueError("Relationships type not specified")
        check_backtick(self.relationships_verb, "Relationships type")

        if not self.source_node_id_column or self.source_node_id_column not in existing_colnames:
            raise ValueError(
                f"Source nodes primary key '{self.source_node_id_column}' is invalid. It is mandatory and must be a valid column"
            )

        if not self.target_node_id_column or self.target_node_id_column not in existing_colnames:
            raise ValueError(
                f"Target nodes primary key '{self.target_node_id_column}' is invalid. It is mandatory and must be a valid column"
            )

        if self.relationship_id_column and self.relationship_id_column not in existing_colnames:
            raise ValueError(f"Relationship primary key '{self.relationship_id_column}' is not a valid column")

        for colname in self.source_node_properties:
            if colname not in existing_colnames:
                raise ValueError(f"Source nodes property '{colname}' is invalid.")
        for colname in self.target_node_properties:
            if colname not in existing_colnames:
                raise ValueError(f"Target nodes property '{colname}' is invalid.")
        for colname in self.relationship_properties:
            if colname not in existing_colnames:
                raise ValueError(f"Relationship property '{colname}' is invalid.")

        check_property_names_map(self.property_names_map, self.used_columns)


def check_property_names_map(property_names_map, used_columns):
    """Check that all key -> values in the DSS column -> Neo4j name mapping are valid """
    if property_names_map:
        for dss_column, neo4j_property in property_names_map.items():
            if dss_column not in used_columns:
                raise ValueError(f"'{dss_column}' is not a valid DSS column name for changing names in Neo4j.")

            if not neo4j_property:
                raise ValueError(f"Neo4j property for DSS column '{dss_column}' is not specified.")
            check_backtick(neo4j_property, "Neo4j property name")


def check_backtick(value, label):
    """Raise an error if the value contain any backtick """
    if "`" in value:
        raise ValueError(f"{label} '{value}' cannot contain backticks (`). Please remove any backtick.")