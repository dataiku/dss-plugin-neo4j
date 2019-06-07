import logging
import os
import shutil
from py2neo import Graph

logger = logging.getLogger()


class Neo4jHandle(object):
    def __init__(self, uri, username, password, import_dir, is_remote, ssh_host, ssh_user):
        self.graph = Graph(uri, auth=("{}".format(username), "{}".format(password)))
        self.import_dir = import_dir
        self.is_remote = is_remote
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user

    def check(self):
        if self.is_remote:
            if self.ssh_host is None or self.ssh_host == "":
                raise ValueError("Plugin settings: SSH host is required for remote Neo4j server")
            if self.ssh_user is None or self.ssh_user == "":
                raise ValueError("Plugin settings: SSH user is required for remote Neo4j server")

    def run(self, cypher_query):
        return self.graph.run(cypher_query)

    def delete_nodes(self, nodes_label):
        q = """
          MATCH (n:`%s`)
          DETACH DELETE n
        """ % (nodes_label)
        logger.info("[+] Delete nodes: {}".format(q))
        self.run(q)

    def delete_relationships(self, params):
        q = """
          MATCH (:`%s`)-[r:`%s`]-(:`%s`)
          DELETE r
        """ % (params.source_node_label, params.relationships_verb, params.target_node_label)
        logger.info("[+] Delete relationships: {}".format(q))
        self.run(q)

    def load_nodes(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        if params.properties_mode == 'SELECT_COLUMNS':
            properties_map = params.properties_map
        else:
            properties_map = {}
            for col in columns_list:
                if col['name'] not in [params.node_id_column]:
                    properties_map[col['name']] = col['name']
        properties = self._properties(columns_list, properties_map, 'n')
        # TODO no PERIODIC COMMIT?
        q = """
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
        logger.info("[+] Importing nodes into Neo4j: %s" % (q))
        r = self.run(q)
        logger.info(r.stats())

    def add_unique_constraint_on_relationship_nodes(self, params):
        const1 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.source_node_label, params.source_node_lookup_key)
        const2 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.target_node_label, params.target_node_lookup_key)
        logger.info("[+] Create constraints on nodes: \n\t" + const1 + "\n\t" + const2)
        self.run(const1)
        self.run(const2)

    def load_relationships(self, csv_file_path, columns_list, params):
        definition = self._schema(columns_list)
        if params.properties_mode == 'SELECT_COLUMNS':
            properties_map = params.properties_map
        else:
            properties_map = {}
            for col in columns_list:
                if col['name'] not in [params.source_node_id_column, params.target_node_id_column]:
                    properties_map[col['name']] = col['name']
        properties = self._properties(columns_list, properties_map, 'rel')
        q = """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MATCH (f:`%s` {`%s`: `%s`})
MATCH (t:`%s` {`%s`: `%s`})
MERGE (f)-[rel:`%s`]->(t)
ON CREATE SET rel.weight = 1
ON MATCH SET rel.weight = rel.weight + 1
%s
        """ % (
            csv_file_path,
            definition,
            params.source_node_label, params.source_node_lookup_key, params.source_node_id_column,
            params.target_node_label, params.target_node_lookup_key, params.target_node_id_column,
            params.relationships_verb,
            properties
        )
        logger.info("[+] Import relationships into Neo4j: %s" % (q))
        r = self.run(q)
        logger.info(r.stats())

    def load_combined(self, csv_file_path, params, columns_list):
        print(params.properties_map)
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
            self._properties(columns_list, params.source_node_properties, 'src'),
            params.target_node_label, params.target_node_lookup_key, params.target_node_id_column,
            self._properties(columns_list, params.source_node_properties, 'tgt'),
            params.relationships_verb,
            self._properties(columns_list, params.properties_map, 'rel')
        )
        logger.info("[+] Import relationships and nodes into Neo4j: %s" % (q))
        r = self.run(q)
        logger.info(r.stats())

    def move_to_import_dir(self, file_to_move):
        if self.is_remote:
            self._scp_nopassword_to_server(file_to_move)
        else:
            logger.info("[+] Move file to Neo4j import dir...")
            filename = os.path.basename(file_to_move)
            outfile = os.path.join(self.import_dir, filename)
            shutil.move(file_to_move, outfile)

    def delete_file_from_import_dir(self, file_path):
        outfile = os.path.join(self.import_dir, file_path)
        if self.is_remote:
            self._ssh_remove_file(outfile)
        else:
            os.remove(outfile)

    def _scp_nopassword_to_server(self, file_to_send):
        """
        copies a file to a remote server using SCP. Requires a password-less access (i.e SSH public key is available)
        """
        logger.info("[+] Send file to Neo4j import dir through SCP...")
        p = Popen(["scp", file_to_send, "{}@{}:{}".format(self.ssh_user, self.ssh_host, self.import_dir)], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err != '':
            os.remove(file_to_send)
            raise Exception(str(err))
        os.remove(file_to_send)

    def _ssh_remove_file(self, file_path):
        p = Popen(["ssh", "{}@{}".format(self.ssh_user, self.ssh_host), "rm -rf", file_path], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err != '':
            logger.error(str(err))

    def _build_nodes_definition(self, nodes_label, columns_list):
        definition = ':{}'.format(nodes_label)
        definition += ' {' + '\n'
        definition += ',\n'.join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        definition += '\n' + '}'
        return definition

    def _schema(self, columns_list):
        return ', '.join(["line[{}] AS `{}`".format(i, c["name"]) for i, c in enumerate(columns_list)])

    def _properties(self, all_columns_list, properties_map, identifier):
        type_per_column = {}
        for c in all_columns_list:
            type_per_column[c['name']] = c['type']
        properties_strings = []
        for colname in properties_map:
            propstr = self._property(colname, properties_map[colname], type_per_column[colname], identifier)
            properties_strings.append(propstr)
        return "\n".join(properties_strings)

    def _property(self, colname, prop, coltype, identifier):
        if coltype in ['int', 'bigint', 'smallint', 'tinyint']:
            typedValue = "toInteger(`{}`)".format(colname)
        elif coltype in ['double', 'float']:
            typedValue = "toFloat(`{}`)".format(colname)
        elif coltype == 'boolean':
            typedValue = "toBoolean(`{}`)".format(colname)
        else:
            typedValue = "`{}`".format(colname)
        oncreate = "ON CREATE SET `{}`.`{}` = {}".format(identifier, prop, typedValue)
        onmatch = "" # "ON MATCH SET `{}`.`{}` = {}".format(identifier, prop, typedValue)
        return oncreate + '\n' + onmatch


class NodesExportParams(object):
    def __init__(self, nodes_label, node_id_column, properties_mode, properties_map, clear_before_run=False):
        self.nodes_label = nodes_label
        self.node_id_column = node_id_column
        self.properties_mode = properties_mode
        self.properties_map = properties_map or {}
        self.clear_before_run = clear_before_run

        if self.properties_mode == 'SELECT_COLUMNS':
            if node_id_column in properties_map:
                self.node_lookup_key = properties_map[node_id_column]
                properties_map.pop(node_id_column)
            else:
                self.node_lookup_key = node_id_column
        else:
            self.node_lookup_key = node_id_column

    def check(self, input_dataset_schema):
        if self.nodes_label is None:
            raise ValueError('nodes_label not specified')
        if self.node_id_column is None:
            raise ValueError('node_id_column not specified')
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.properties_mode == 'SELECT_COLUMNS':
            for colname in self.properties_map:
                if colname not in existing_colnames:
                    raise ValueError("properties_map. Column does not exist in input dataset: "+str(colname))


class RelationshipsExportParams(object):

    def __init__(self,
            source_node_label,
            source_node_lookup_key,
            source_node_id_column,
            target_node_label,
            target_node_lookup_key,
            target_node_id_column,
            relationships_verb,
            properties_mode,
            properties_map,
            clear_before_run=False,
        ):
        self.source_node_label = source_node_label
        self.source_node_lookup_key = source_node_lookup_key
        self.source_node_id_column = source_node_id_column
        self.target_node_label = target_node_label
        self.target_node_lookup_key = target_node_lookup_key
        self.target_node_id_column = target_node_id_column
        self.relationships_verb = relationships_verb
        self.properties_mode = properties_mode
        self.properties_map = properties_map or {}
        self.clear_before_run = clear_before_run

    def check(self, input_dataset_schema):
        if self.source_node_label is None or self.source_node_label == "":
            raise ValueError('source_node_label not specified')
        if self.source_node_lookup_key is None or self.source_node_lookup_key == "":
            raise ValueError('source_node_lookup_key not specified')
        if self.source_node_id_column is None or self.source_node_id_column == "":
            raise ValueError('source_node_id_column not specified')
        if self.target_node_label is None or self.target_node_label == "":
            raise ValueError('target_node_label not specified')
        if self.target_node_lookup_key is None or self.target_node_lookup_key == "":
            raise ValueError('target_node_lookup_key not specified')
        if self.target_node_id_column is None or self.target_node_id_column == "":
            raise ValueError('target_node_id_column not specified')
        if self.relationships_verb is None or self.relationships_verb == "":
            raise ValueError('relationships_verb not specified')
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.source_node_id_column not in existing_colnames:
            raise ValueError("source_node_id_column. Column does not exist in input dataset: "+str(self.source_node_id_column))
        if self.target_node_id_column not in existing_colnames:
            raise ValueError("target_node_id_column. Column does not exist in input dataset: "+str(self.target_node_id_column))
        if self.properties_mode == 'SELECT_COLUMNS':
            for colname in self.properties_map:
                if colname not in existing_colnames:
                    raise ValueError("properties_map. Column does not exist in input dataset: "+str(colname))

class CombinedExportParams(object):
    def __init__(self,
            source_node_label,
            source_node_id_column,
            source_node_properties,
            target_node_label,
            target_node_id_column,
            target_node_properties,
            relationships_verb,
            properties_map,
            clear_before_run=False):
        self.source_node_label = source_node_label
        self.source_node_id_column = source_node_id_column
        self.source_node_properties = source_node_properties or {}
        self.target_node_label = target_node_label
        self.target_node_id_column = target_node_id_column
        self.target_node_properties = target_node_properties or {}
        self.relationships_verb = relationships_verb
        self.properties_map = properties_map or {}
        self.clear_before_run = clear_before_run

        if source_node_id_column in source_node_properties:
            self.source_node_lookup_key = source_node_properties[source_node_id_column]
            source_node_properties.pop(source_node_id_column)
        else:
            self.source_node_lookup_key = source_node_id_column

        if target_node_id_column in target_node_properties:
            self.target_node_lookup_key = target_node_properties[target_node_id_column]
            target_node_properties.pop(target_node_id_column)
        else:
            self.target_node_lookup_key = target_node_id_column


    def check(self, input_dataset_schema):
        if self.source_node_label is None or self.source_node_label == "":
            raise ValueError("source_node_label not specified")
        if self.target_node_label is None or self.target_node_label == "":
            raise ValueError("target_node_label not specified")
        if self.source_node_id_column is None or self.source_node_id_column == "":
            raise ValueError("source_node_id_column not specified")
        if self.target_node_id_column is None or self.target_node_id_column == "":
            raise ValueError("target_node_id_column not specified")
        if self.relationships_verb is None or self.relationships_verb == "":
            raise ValueError("relationships_verb not specified")
        existing_colnames = [c["name"] for c in input_dataset_schema]
        if self.source_node_id_column not in existing_colnames:
            raise ValueError("source_node_id_column. Column does not exist in input dataset: "+str(self.source_node_id_column))
        if self.target_node_id_column not in existing_colnames:
            raise ValueError("target_node_id_column. Column does not exist in input dataset: "+str(self.target_node_id_column))
        for colname in self.source_node_properties:
            if colname not in existing_colnames:
                raise ValueError("source_node_properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.target_node_properties:
            if colname not in existing_colnames:
                raise ValueError("target_node_properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.properties_map:
            if colname not in existing_colnames:
                raise ValueError("properties_map. Column does not exist in input dataset: "+str(colname))

