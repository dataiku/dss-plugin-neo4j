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

    def load_nodes(self, csv_file_path, nodes_label, node_id_column, columns_list):
        definition = self._schema(columns_list)
        properties = self._properties(columns_list, [node_id_column], True, 'n')
        # TODO no PERIODIC COMMIT?
        q = """
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MERGE (n:`%s` {`%s`: `%s`})
%s
        """ % (
            csv_file_path,
            definition,
            nodes_label, node_id_column, node_id_column,
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
        properties = self._properties(columns_list, [params.source_node_id_column, params.target_node_id_column], params.set_relationship_properties, 'rel')
        q = """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MATCH (f:`%s` {`%s`: `%s`})
MATCH (t:`%s` {`%s`: `%s`})
MERGE (f)-[rel:`%s`]->(t)
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
        print(params.relationship_properties)
        definition = self._schema(columns_list)
        # properties = self._properties(columns_list, excluded_columns, params.set_relationship_properties, 'rel')
        q = """
USING PERIODIC COMMIT
LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
WITH %s
MERGE (src:`%s` {`%s`: `%s`})
%s
MERGE (tgt:`%s` {`%s`: `%s`})
%s
MERGE (src)-[rel:`%s`]->(tgt)
%s
        """ % (
            csv_file_path,
            definition,
            params.source_node_label, params.source_node_lookup_key, params.source_node_id_column,
            self._properties2(columns_list, params.source_node_properties, 'src'),
            params.target_node_label, params.target_node_lookup_key, params.target_node_id_column,
            self._properties2(columns_list, params.source_node_properties, 'tgt'),
            params.relationships_verb,
            self._properties2(columns_list, params.relationship_properties, 'rel')
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

    def _properties2(self, all_columns_list, properties_map, identifier):
        type_per_column = {}
        for c in all_columns_list:
            type_per_column[c['name']] = c['type']
        properties_strings = []
        for colname in properties_map:
            propstr = self._property2(colname, properties_map[colname], type_per_column[colname], identifier)
            properties_strings.append(propstr)
        return "\n".join(properties_strings)

    def _properties(self, columns_list, excluded_columns, set_properties, obj):
        """
        Generates the definition (name the columns) and the properties to set.
        Note that we don't use the syntax to set the properties in the match clause because it does not work well with NULL values
        """
        if not set_properties:
            return ""
        properties_columns = [c for c in columns_list if c["name"] not in excluded_columns]
        return "\n".join([self._property(c, obj) for c in properties_columns])

    def _property(self, col, obj):
        if col["type"] in ['int', 'bigint', 'smallint', 'tinyint']:
            typedValue = "toInteger(`{}`)".format(col["name"])
        elif col["type"] in ['double', 'float']:
            typedValue = "toFloat(`{}`)".format(col["name"])
        elif col["type"] == 'boolean':
            typedValue = "toBoolean(`{}`)".format(col["name"])
        else:
            typedValue = "`{}`".format(col["name"])
        return "ON CREATE SET {}.`{}` = {}\nON MATCH SET {}.`{}` = {}".format(obj, col["name"], typedValue, obj, col["name"], typedValue)

    def _property2(self, colname, prop, coltype, identifier):
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
    def __init__(self, nodes_label, node_id_column, clear_before_run=False):
        self.nodes_label = nodes_label
        self.node_id_column = node_id_column
        self.clear_before_run = clear_before_run

    def check(self):
        if self.nodes_label is None:
            raise ValueError('nodes_label not specified')
        if self.node_id_column is None:
            raise ValueError('node_id_column not specified')


class RelationshipsExportParams(object):

    def __init__(self,
            source_node_label,
            source_node_lookup_key,
            source_node_id_column,
            target_node_label,
            target_node_lookup_key,
            target_node_id_column,
            relationships_verb,
            set_relationship_properties=True,
            clear_before_run=False,
        ):
        self.source_node_label = source_node_label
        self.source_node_lookup_key = source_node_lookup_key
        self.source_node_id_column = source_node_id_column
        self.target_node_label = target_node_label
        self.target_node_lookup_key = target_node_lookup_key
        self.target_node_id_column = target_node_id_column
        self.relationships_verb = relationships_verb
        self.set_relationship_properties = set_relationship_properties
        self.clear_before_run = clear_before_run

    def check(self):
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

class CombinedExportParams(object):
    def __init__(self,
            source_node_label,
            source_node_id_column,
            source_node_properties,
            target_node_label,
            target_node_id_column,
            target_node_properties,
            relationships_verb,
            relationship_properties,
            clear_before_run=False):
        self.source_node_label = source_node_label
        self.source_node_id_column = source_node_id_column
        self.source_node_properties = source_node_properties or {}
        self.target_node_label = target_node_label
        self.target_node_id_column = target_node_id_column
        self.target_node_properties = target_node_properties or {}
        self.relationships_verb = relationships_verb
        self.relationship_properties = relationship_properties or {}
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


    def check(self, input_dataset_columns):
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
        existing_colnames = [c["name"] for c in input_dataset_columns]
        for colname in self.source_node_properties:
            if colname not in existing_colnames:
                raise ValueError("source_node_properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.target_node_properties:
            if colname not in existing_colnames:
                raise ValueError("target_node_properties. Column does not exist in input dataset: "+str(colname))
        for colname in self.relationship_properties:
            if colname not in existing_colnames:
                raise ValueError("relationship_properties. Column does not exist in input dataset: "+str(colname))

