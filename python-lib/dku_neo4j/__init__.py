import logging
from py2neo import Graph


class Neo4jHandle(object):
    def __init__(self, uri, username, password):
        self.graph = Graph(uri, auth=("{}".format(username), "{}".format(password)))

    def check(self):
        try:
            query = "MATCH (n) RETURN n LIMIT 1"
            self.graph.run(query)
        except Exception:
            raise ValueError("Could not connect to graph database using the preset")
        # TODO was used with self managed SSH connection - going through SFTP folder now, anything to do here?
        return

    def run(self, cypher_query):
        return self.graph.run(cypher_query)

    def delete_nodes(self, nodes_label):
        q = """
          MATCH (n:`%s`)
          DETACH DELETE n
        """ % (nodes_label)
        logging.info("[+] Delete nodes: {}".format(q))
        self.run(q)

    def delete_relationships(self, params):
        q = """
          MATCH (:`%s`)-[r:`%s`]-(:`%s`)
          DELETE r
        """ % (params.source_node_label, params.relationships_verb, params.target_node_label)
        logging.info("[+] Delete relationships: {}".format(q))
        self.run(q)

    def load_nodes(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        if params.properties_mode == 'SELECT_COLUMNS':
            node_properties = params.node_properties
        else:
            node_properties = [col['name'] for col in columns_list if col['name'] != params.node_id_column]
        properties = self._properties(columns_list, node_properties, 'n', params.property_names_map)
        q = """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MERGE (n:`%s` {`%s`: `%s`})
%s
        """ % (
            csv_file_path,
            definition,
            params.nodes_label, params.node_lookup_key, params.node_id_column,
            properties
            )
        logging.info("[+] Importing nodes into Neo4j: %s" % (q))
        r = self.run(q)
        logging.info(r.stats())

    def add_unique_constraint_on_relationship_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.source_node_label, params.source_node_lookup_key)
        self._add_unique_constraint_if_not_exist(params.target_node_label, params.target_node_lookup_key)

    def add_unique_constraint_on_nodes(self, params):
        self._add_unique_constraint_if_not_exist(params.nodes_label, params.node_lookup_key)

    def _add_unique_constraint_if_not_exist(self, label, property_key):
        if property_key not in self.graph.schema.get_uniqueness_constraints(label=label):
            self.graph.schema.create_uniqueness_constraint(label=label, property_key=property_key)
            logging.info("[+] Created uniqueness constraint on {}.{}".format(label, property_key))

    def load_relationships(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        q = """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MERGE (src:`%s` {`%s`: `%s`})
%s
ON CREATE SET src.count = 1
ON MATCH SET src.count = src.count + 1
MERGE (tgt:`%s` {`%s`: `%s`})
%s
ON CREATE SET tgt.count = 1
ON MATCH SET tgt.count = tgt.count + 1
MERGE (src)-[rel:`%s`]->(tgt)
%s
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
        """ % (
            csv_file_path,
            definition,
            params.source_node_label, params.source_node_lookup_key, params.source_node_id_column,
            self._properties(columns_list, params.source_node_properties, 'src', params.property_names_map),
            params.target_node_label, params.target_node_lookup_key, params.target_node_id_column,
            self._properties(columns_list, params.target_node_properties, 'tgt', params.property_names_map),
            params.relationships_verb,
            self._properties(columns_list, params.relationship_properties, 'rel', params.property_names_map)
        )
        logging.info("[+] Import relationships and nodes into Neo4j: %s" % (q))
        r = self.run(q)
        logging.info(r.stats())

    def _build_nodes_definition(self, nodes_label, columns_list):
        definition = ':{}'.format(nodes_label)
        definition += ' {' + '\n'
        definition += ',\n'.join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        definition += '\n' + '}'
        return definition

    def _schema(self, columns_list):
        return ', '.join(["line[{}] AS `{}`".format(i, c["name"]) for i, c in enumerate(columns_list)])

    def _properties(self, all_columns_list, properties_list, identifier, property_names_map):
        type_per_column = {}
        for c in all_columns_list:
            type_per_column[c['name']] = c['type']
        properties_strings = []
        for colname in properties_list:
            if colname in property_names_map:
                neo4j_property_name = property_names_map[colname]
            else:
                neo4j_property_name = colname
            propstr = self._property(colname, neo4j_property_name, type_per_column[colname], identifier)
            properties_strings.append(propstr)
        return "\n".join(properties_strings)

    def _property(self, colname, prop, coltype, identifier):
        if coltype in ['int', 'bigint', 'smallint', 'tinyint']:
            typedValue = "toInteger(`{}`)".format(colname)
        elif coltype in ['double', 'float']:
            typedValue = "toFloat(`{}`)".format(colname)
        elif coltype == 'boolean':
            typedValue = "toBoolean(`{}`)".format(colname)
        elif coltype == 'date':
            typedValue = "datetime(`{}`)".format(colname)
        else:
            typedValue = "`{}`".format(colname)
        oncreate = "ON CREATE SET `{}`.`{}` = {}".format(identifier, prop, typedValue)
        onmatch = "ON MATCH SET `{}`.`{}` = {}".format(identifier, prop, typedValue)
        return oncreate + '\n' + onmatch


class NodesExportParams(object):
    def __init__(self,
                 nodes_label,
                 node_id_column,
                 properties_mode,
                 node_properties,
                 property_names_mapping,
                 property_names_map,
                 clear_before_run=False):
        self.nodes_label = nodes_label
        self.node_id_column = node_id_column
        self.properties_mode = properties_mode
        self.node_properties = node_properties or []
        self.property_names_map = property_names_map or {} if property_names_mapping else {}
        self.clear_before_run = clear_before_run

        if properties_mode == 'SELECT_COLUMNS':
            if node_id_column in node_properties:
                # self.node_lookup_key = properties_map[node_id_column]
                self.node_properties.remove(node_id_column)
        if node_id_column in property_names_map:
            self.node_lookup_key = property_names_map[node_id_column]
        else:
            self.node_lookup_key = node_id_column

    def check(self, input_dataset_schema):
        if self.nodes_label is None:
            raise ValueError('nodes_label not specified')
        if self.node_id_column is None:
            raise ValueError('Primary key not specified')
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.properties_mode == 'SELECT_COLUMNS':
            for colname in self.node_properties:
                if colname and colname not in existing_colnames:
                    raise ValueError("node_properties. Column does not exist in input dataset: "+str(colname))


class RelationshipsExportParams(object):
    def __init__(self,
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
                 clear_before_run=False):

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
            raise ValueError("Source nodes primary key. Column does not exist in input dataset: "+str(self.source_node_id_column))
        if self.target_node_id_column not in existing_colnames:
            raise ValueError("Target nodes primary key. Column does not exist in input dataset: "+str(self.target_node_id_column))
        for colname in self.source_node_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Source nodes properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.target_node_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Target nodes properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.relationship_properties:
            if colname and colname not in existing_colnames:
                raise ValueError("Relationship properties. Column does not exist in input dataset: "+str(colname))
