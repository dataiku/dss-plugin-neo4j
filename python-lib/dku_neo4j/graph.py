import time
import networkx as nx


class GraphError(Exception):
    """Base class for other exceptions"""
    pass


class GraphVis:

    def __init__(self, graph_params):
        self.node_params = graph_params.get('node_params', None)
        self.rel_params = graph_params.get('rel_params', None)
        self.relations = graph_params.get('relations', None)
        self.query_limit = graph_params.get('query_limit', None)
        
        self.local_selection = graph_params.get('local_selection', None)

        print(self.__dict__)

    def run_global_query(self, graph_neo4j):
        assert self.relations is not None
        self.node_labels = self._get_node_labels()
        self.relation_types = self._get_relation_types()

        query = self._create_multiple_relation_query()
        print("query: ", query)
        data = graph_neo4j.run(query).data()

        if len(data) == 0:
            raise GraphError("Graph error: {}".format("Cypher query result is empty !"))

        relations_list = []
        for row in data:
            for k, v in row.items():
                relations_list += [v]
        return relations_list

    def run_local_query(self, graph_neo4j):
        assert self.local_selection is not None
        self.node_labels = self.local_selection["nodes"]
        self.relation_types = self.local_selection["relations"]

        print("local self.node_labels: ", self.node_labels)
        print("local self.relation_types: ", self.relation_types)

        selected_node_id = self.local_selection["nodeId"]
        query = self._create_subgraph_query(selected_node_id)
        print("query: ", query)
        data = graph_neo4j.run(query).data()
        if len(data) == 0:
            raise GraphError("Graph error: {}".format("Cypher query result is empty !"))
        return data[0]['relationships']

    def create_graph(self, relations):
        nodes = {}
        edges = {}
        for rel in relations:
            src, tgt = rel.start_node, rel.end_node

            if src.identity not in nodes:
                nodes[src.identity] = self._create_node(src)

            if tgt.identity not in nodes:
                nodes[tgt.identity] = self._create_node(tgt)

            if rel.identity not in edges:
                edges[rel.identity] = self._create_edge(rel)

        self.nodes = nodes
        self.edges = edges

    def _create_node(self, node):
        info = {"id": node.identity}
        properties = dict(node)
        node_label = self._get_node_label(node)
        params = self.node_params.get(node_label, None)

        if params:
            if "caption" in params:
                info["label"] = self._str_or_none(properties.get(params["caption"], None))
            # if "size" in params:
            #     info["value"] = int(properties.get(params["size"], 0))
            # if "shape" in node_params:
            #     info["shape"] = node_params["shape"]

        info["group"] = node_label
        info["title"] = self._get_title(node_label, properties, node_id=node.identity)
        return info

    def _create_edge(self, rel):
        info = {'from': rel.start_node.identity, 'to': rel.end_node.identity}
        properties = dict(rel)
        rel_type = type(rel).__name__
        params = self.rel_params.get(rel_type, None)

        if params:
            # if "caption" in params:
            #    info["label"] = self._str_or_none(properties.get(params["caption"], None))
            if "width" in params:
                info["value"] = int(properties.get(params["width"], 0))

        info["label"] = rel_type
        info["title"] = self._get_title(rel_type, properties)
        return info

    def _get_node_label(self, node):
        # can sometimes have multiple labels (get first label in list that is in node_params)
        node_labels = list(node.labels)
        if len(node_labels) == 1:
            node_label = node_labels[0]
        else:
            node_label = next((label for label in node_labels if label in self.node_labels), None)
            if node_label is None:
                raise ValueError("Cypher error with a node with multiple labels {}".format(node_labels))
        return node_label

    def _get_title(self, label, properties, node_id=None):
        if node_id:
            return "<br>".join(["<b>{} ({})</b>".format(label, node_id)] + ["{}: {}".format(key, str(value)) for key, value in properties.items()])
        else:
            return "<br>".join(["<b>{}</b>".format(label)] + ["{}: {}".format(key, str(value)) for key, value in properties.items()])

    def _str_or_none(self, value):
        return str(value) if value is not None else None

    def _get_node_labels(self):
        node_labels = []
        for rel in self.relations:
            if rel['source'] not in node_labels:
                node_labels.append(rel['source'])
            if rel['target'] not in node_labels:
                node_labels.append(rel['target'])
        return node_labels

    def _get_relation_types(self):
        relation_types = []
        for rel in self.relations:
            if rel['relation'] not in relation_types:
                relation_types.append(rel['relation'])
        return relation_types

    def _get_relationship_filter(self):
        return "|".join(self.relation_types)

    def _get_label_filter(self):
        return "|".join(self.node_labels)

    def _create_multiple_relation_query(self):
        match_list = []
        return_list = []
        nodes_id = set()
        edges_id = set()

        for index, rel in enumerate(self.relations):
            match_list += ["({0}:{1})-[{2}:{3}]->({4}:{5})".format(rel['src_id'], rel['source'], rel['rel_id'], rel['relation'], rel['tgt_id'], rel['target'])]

            if rel['src_id'] not in nodes_id:
                nodes_id.add(rel['src_id'])
            if rel['tgt_id'] not in nodes_id:
                nodes_id.add(rel['tgt_id'])
    #         if rel[2] not in unique_edges:
            edges_id.add(rel['rel_id'])

        # for node in nodes_id:
        #     return_list += ["{}".format(node), "id({0}) AS {0}_id".format(node)]
        for edge in edges_id:
            return_list += [edge]

        match_statement = "MATCH\n" + ",\n".join(match_list)
        return_statement = "\nRETURN\n" + ", ".join(return_list)
        limit_statement = "\nLIMIT {}".format(self.query_limit)

        return match_statement + return_statement + limit_statement

    def _create_subgraph_query(self, selected_node_id):
        relationship_filter = self._get_relationship_filter()
        label_filter = self._get_label_filter()

        query = """
        MATCH (n)
        WHERE id(n) = {0}
        CALL apoc.path.subgraphAll(n, {{
            relationshipFilter: '{1}',
            labelFilter: '+{2}',
            bfs: true,
            minLevel: 0,
            maxLevel: 10,
            limit: {3}
        }})
        YIELD relationships
        RETURN relationships
        """.format(selected_node_id, relationship_filter, label_filter, self.query_limit)

        return query

    # def compute_layout(self):
    #     logger.info("Computing layout ...")
    #     start = time.time()
    #     if self.directed_edges:
    #         G = nx.Graph()
    #     else:
    #         G = nx.DiGraph()

    #     G.add_nodes_from(list(self.nodes.keys()))
    #     G.add_edges_from(list(self.edges.keys()))

    #     positions = nx.nx_agraph.graphviz_layout(G, prog='sfdp')
    #     # positions = nx.nx_pydot.pydot_layout(G, prog='sfdp')

    #     # for node, pos in positions.items():
    #     #     self.nodes[node].update({'x': pos[0], 'y': pos[1]})

    #     # logger.info("Layout computed in {:.4f} seconds".format(time.time()-start))