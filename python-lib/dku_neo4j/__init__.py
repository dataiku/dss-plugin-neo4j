import logging
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

    def delete_nodes(self, params):
        q = """
          MATCH (n:`%s`)
          DETACH DELETE n
        """ % (params.nodes_label)
        logger.info("[+] Delete nodes: {}".format(q))
        self.graph.run(q)

    def delete_relationships(self, params):
        q = """
          MATCH (:`%s`)-[r:`%s`]-(:`%s`)
          DELETE r
        """ % (params.source_node_label, params.relationships_verb, params.target_node_label)
        logger.info("[+] Delete relationships: {}".format(q))
        self.graph.run(q)

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
        r = self.graph.run(q)
        logger.info(r.stats())

    def add_unique_constraint_on_relationship_nodes(self, params):
        const1 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.source_node_label, params.source_node_lookup_key)
        const2 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.target_node_label, params.target_node_lookup_key)
        logger.info("[+] Create constraints on nodes: \n\t" + const1 + "\n\t" + const2)
        self.graph.run(const1)
        self.graph.run(const2)

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
        r = self.graph.run(q)
        logger.info(r.stats())

    def _build_nodes_definition(self, nodes_label, columns_list):
        definition = ':{}'.format(nodes_label)
        definition += ' {' + '\n'
        definition += ',\n'.join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        definition += '\n' + '}'
        return definition

    def _schema(self, columns_list):
        return ', '.join(["line[{}] AS `{}`".format(i, c["name"]) for i, c in enumerate(columns_list)])

    def _properties(self, columns_list, excluded_columns, set_properties, obj):
        """
        Generates the definition (name the columns) and the properties to set.
        Note that we don't use the syntax to set the properties in the match clause because it does not work well with NULL values
        """
        if not set_properties:
            return ""
        properties_columns = [c["name"] for c in columns_list if c["name"] not in excluded_columns]
        return "\n".join([self._property(name, obj) for name in properties_columns])

    def _property(self, name, obj='rel'):
        return "ON CREATE SET {}.`{}` = `{}`\nON MATCH SET {}.`{}` = `{}`".format(obj, name, name, obj, name, name)


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
