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
        logger.info("[+] Deleting existing nodes: {}".format(q))
        r = self.graph.run(q)

    def delete_relationships(self, params):
        q = """
          MATCH (:`%s`)-[r:`%s`]-(:`%s`)
          DELETE r
        """ % (params.nodes_from_label, params.relationships_verb, params.nodes_to_label)
        logger.info("[+] Delete existing relationships: {}".format(q))
        r = self.graph.run(q)

    def load_nodes_csv(self, csv_file_path, nodes_label, columns_list):
        definition = self._build_nodes_definition(nodes_label, columns_list)
        # TODO no PERIODIC COMMIT?
        q = """
          LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
          CREATE (%s)
        """ % (csv_file_path, definition)
        logger.info("[+] Importing nodes into Neo4j: %s" % (q))
        r = self.graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())

    def add_unique_constraint_on_relationship_nodes(self, params):
        const1 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.nodes_from_label, params.nodes_from_key)
        const2 = "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE" % (params.nodes_to_label, params.nodes_to_key)
        logger.info("[+] Create constraints on nodes: \n\t" + const1 + "\n\t" + const2)
        self.graph.run(const1)
        self.graph.run(const2)

    def load_relationships_csv(self, csv_file_path, columns_list, params):
        (definition, attributes) = self._build_relationships_definition(
            columns_list=columns_list,
            key_a=params.nodes_from_key,
            key_b=params.nodes_to_key,
            set_properties=params.relationships_set_properties
        )
        q = """
          USING PERIODIC COMMIT
          LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
          WITH %s
          MATCH (f:`%s` {`%s`: `%s`})
          MATCH (t:`%s` {`%s`: `%s`})
          MERGE (f)-[rel:`%s` %s]->(t)
        """ % (
            csv_file_path,
            definition,
            params.nodes_from_label, params.nodes_from_key, params.relationships_from_key,
            params.nodes_to_label, params.nodes_to_key, params.relationships_to_key,
            params.relationships_verb, attributes
        )
        logger.info("[+] Import relationships into Neo4j: %s" % (q))
        r = self.graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())

    def _build_nodes_definition(self, nodes_label, columns_list):
        definition = ':{}'.format(nodes_label)
        definition += ' {' + '\n'
        definition += ',\n'.join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        definition += '\n' + '}'
        return definition

    def _build_relationships_definition(self, columns_list, key_a, key_b, set_properties=False):
        definition = ', '.join(["line[{}] AS `{}`".format(i, r["name"]) for i, r in enumerate(columns_list)])
        attributes = ""
        if set_properties:
            logger.info("[+] Setting relationships properties")
            attributes += "{"
            o = []
            for c in columns_list:
                if c["name"] not in[key_a, key_b]:
                    o.append("`{}`:`{}`".format(c["name"], c["name"]))
            attributes += ", ".join(o)
            attributes += "}"
        return (definition, attributes)

class NodesExportParams(object):
    def __init__(self, nodes_label, clear_before_run):
        self.nodes_label = nodes_label
        self.clear_before_run = clear_before_run

    def check(self):
        if self.nodes_label is None:
            raise ValueError('Nodes label not specified')

class RelationshipsExportParams(object):

    def __init__(self,
            nodes_from_label,
            nodes_from_key,
            nodes_to_label,
            nodes_to_key,
            relationships_from_key,
            relationships_to_key,
            relationships_verb,
            relationships_set_properties,
            clear_before_run,
        ):
        self.nodes_from_label = nodes_from_label
        self.nodes_from_key = nodes_from_key
        self.nodes_to_label = nodes_to_label
        self.nodes_to_key = nodes_to_key
        self.relationships_from_key = relationships_from_key
        self.relationships_to_key = relationships_to_key
        self.relationships_verb = relationships_verb
        self.relationships_set_properties = relationships_set_properties
        self.clear_before_run = clear_before_run

    def check(self):
        pass # TODO
