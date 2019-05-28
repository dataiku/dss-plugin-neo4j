import logging
from py2neo import Graph

logger = logging.getLogger()


class Neo4jHandle(object):
    def __init__(self, uri, username, password, ssh_host, ssh_user, import_dir=None):
        self.graph = Graph(uri, auth=("{}".format(username), "{}".format(password)))
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.import_dir = import_dir

    def check(self):
        pass # TODO

    def delete_nodes_with_label(self, nodes_label):
        q = """
          MATCH (n:%s)
          DETACH DELETE n
        """ % (nodes_label)
        logger.info("[+] Start deleting existing nodes with label {}...".format(nodes_label))
        r = self.graph.run(q)
        logger.info("[+] Existing nodes with label {} deleted.".format(nodes_label))

    def delete_relationships(self, nodes_a_label, nodes_b_label, relationships_verb):
        q = """
          MATCH (:%s)-[r:%s]-(:%s)
          DELETE r
        """ % (nodes_a_label, relationships_verb, nodes_b_label)
        logger.info("[+] Start deleting existing relationships between {} and {} with verb {}...".format(nodes_a_label, nodes_b_label, relationships_verb))
        try:
            r = graph.run(q)
            logger.info("[+] Existing relationships deleted.")
        except Exception, e:
            logger.error("[-] Issue while deleting relationships")
            logger.error("[-] {}".format( str(e) ))
            sys.exit(1)

    def load_nodes_csv(self, csv_file_path, nodes_label, columns_list):
        declaration = self._build_nodes_declaration(nodes_label, columns_list)

        q = """
          LOAD CSV FROM 'file:///%s' AS line FIELDTERMINATOR '\t'
          CREATE (%s)
        """ % (csv_file_path, declaration)
        logger.info("[+] Start importing nodes into Neo4j...")
        logger.info("[+] %s" % (q))
        r = self.graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())

    def add_relationships_unique_constraint(self, params):
        logger.info(  "[+] Creating constraints on nodes...")
        self.graph.run("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE" % (params.nodes_from_label, params.nodes_from_key))
        self.graph.run("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE" % (params.nodes_to_label, params.nodes_to_key))
        logger.info(  "[+] Done creating constraints on nodes.")

    def load_relationships_csv(self, csv_file_path, columns_list, params):
        (definition, attributes) = neo4jhandle.build_relationships_declaration(
            columns_list=columns_list,
            key_a=params.nodes_from_key,
            key_b=params.nodes_to_key,
            set_properties=params.relationships_set_properties
        )

        q = """
          USING PERIODIC COMMIT
          LOAD CSV FROM 'file://%s' AS line FIELDTERMINATOR '\t'
          WITH %s
          MATCH (f:%s {%s: %s})
          MATCH (t:%s {%s: %s})
          MERGE (f)-[rel:%s %s]->(t)
        """ % (
            csv_file_path,
            definition,
            params.nodes_left_label, params.nodes_left_key, params.relationships_left_key,
            params.nodes_right_label, params.nodes_right_key, params.relationships_right_key,
            params.relationships_verb, params.relationships_attributes
        )
        logger.info("[+] Start importing relationships into Neo4j...")
        logger.info("[+] %s" % (q))
        r = this.graph.run(q)
        logger.info("[+] Import complete.")
        logger.info(r.stats())

    def _build_nodes_declaration(self, nodes_label, columns_list):
        declaration = ':{}'.format(nodes_label)
        declaration = declaration + ' {' + '\n'
        c = ',\n'.join(["  `{}`: line[{}]".format(r["name"], i) for i, r in enumerate(columns_list)])
        declaration = declaration + c
        declaration = declaration + '\n' + '}'
        return declaration

    def build_relationships_declaration(self, columns_list, key_a, key_b, set_properties=False):
        declaration = ', '.join(["line[{}] AS `{}`".format(i, r["name"]) for i, r in enumerate(columns_list)])
        # Edges attributes
        attributes = ""
        if set_properties:
            logger.info("[+] Setting relationships properties")
            attributes = attributes + "{"
            o = []
            for c in columns_list:
                if c["name"] not in[key_a, key_b]:
                    o.append("{}:{}".format(c["name"], c["name"]))
            attributes = attributes + ", ".join(o)
            attributes = attributes + "}"
        return (declaration, attributes)

class NodesExportParams(object):
    def __init__(self, nodes_label, clear_before_run):
        self.nodes_label = nodes_label
        self.clear_before_run = clear_before_run

    def check(self):
        if self.nodes_label is None:
            raise ValueError('Nodes label not specified')

class RelationshipsExportParams(object):
    def __init__(nodes_from_label,
            nodes_from_key,
            nodes_to_label,
            nodes_to_key,
            relationships_from_key,
            relationships_to_key,
            relationships_verb,
            relationships_set_properties,
            relationships_delete,
        ):
        self.nodes_from_label = nodes_from_label
        self.nodes_from_key = nodes_from_key
        self.nodes_to_label = nodes_to_label
        self.nodes_to_key = nodes_to_key
        self.relationships_from_key = relationships_from_key
        self.relationships_to_key = relationships_to_key
        self.relationships_verb = relationships_verb
        self.relationships_set_properties = relationships_set_properties
        self.relationships_delete = relationships_delete

    def check(self):
        pass # TODO
