import logging
import os
import shutil
from py2neo import Graph
import time

logger = logging.getLogger()


class Neo4jHandle(object):
    def __init__(self, uri, username, password):
        self.graph = Graph(uri, auth=("{}".format(username), "{}".format(password)))

    def check(self):
        # TODO was used with self managed SSH connection - going through SFTP folder now, anything to do here?
        return

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

    def load_combined(self, csv_file_path, columns_list, params):
        #print(params.properties_map)
        print("---- file is at: %s" % (csv_file_path,))
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




# Dictionnary of info to generate CALL cypher functions depending on the algorithms selected
ALGO_GENERAL_CONFIG = {
    "pagerank": {
        "function": "pageRank",
        "result_name": "score",
        "result_type": "float",
        "weight_support": True,
        "additional_params": {},
        "algo_type": "centrality"
    },
    "betweenness": {
        "function": "betweenness.sampled",
        "result_name": "centrality",
        "result_type": "float",
        "weight_support": False,
        "additional_params": {},
        "algo_type": "centrality"
    },
    "closeness": {
        "function": "closeness",
        "result_name": "centrality",
        "result_type": "float",
        "weight_support": False,
        "additional_params": {},
        "algo_type": "centrality"
    },
    "indegree": {
        "function": "degree",
        "result_name": "score",
        "result_type": "float",
        "weight_support": True,
        "additional_params": {"direction":"incoming"},
        "algo_type": "centrality"
    },
    "outdegree": {
        "function":"degree",
        "result_name":"score",
        "result_type":"float",
        "weight_support":True,
        "additional_params":{"direction":"outgoing"},
        "algo_type":"centrality"
    },
    "louvain": {
        "function":"louvain",
        "result_name":"community",
        "result_type":"int",
        "weight_support":True,
        "additional_params": {},
        "algo_type":"community"
    },
    "labelpropagation": {
        "function":"labelPropagation",
        "result_name":"label",
        "result_type":"int",
        "weight_support":True,
        "additional_params": {},
        "algo_type":"community"
    },
    "connectedcomponents": {
        "function":"unionFind",
        "result_name":"setId",
        "result_type":"int",
        "weight_support":True,
        "additional_params": {},
        "algo_type":"community"
    }
}

class GraphAnalyticsParams(object):
    def __init__(self, recipe_params):
        self.node_label = recipe_params.get("node_label")
        self.relationship = recipe_params.get("relationship")
        self.computation_mode = recipe_params.get("computation_mode")        
        self.node_properties = recipe_params.get("node_properties")
        
        if recipe_params.get("weight") is None or recipe_params.get("weight") == "":
            self.is_weight = False
        else:
            self.is_weight = True
            self.weight = recipe_params.get("weight")
            
        #Build the list of features to compute and retrieve custom parameters for each CALL function
        if recipe_params.get("computation_mode") == "COMPUTE_ALL_FEATS":
            self.list_features = ALGO_GENERAL_CONFIG.keys()
        elif recipe_params.get("computation_mode") == "SELECT_FEATS":
            self.list_features = [p.split("feat_")[1]
                                  for p in recipe_params.keys()
                                  if p.startswith('feat_') and recipe_params[p]== True
                                 ]
            custom_params = {}
            for f in self.list_features:
                custom_params[f] = recipe_params.get("params_" + f)
            self.custom_params = custom_params
                
            
    def check(self):
        #TO-DO check if node_label, relationship, weight exist in neo4j 
        if self.node_label is None or self.node_label == "":
            raise ValueError('node_label not specified')
            
        if len(self.node_properties.keys()) == 0:
            raise ValueError('No node property given, need at least one')
            
        for prop in self.node_properties.keys():
            if prop in [None, "", "undefined"]:
                raise ValueError('Undefined node property')
            if self.node_properties[prop] in [None, "", "undefined"]:
                self.node_properties[prop] = prop
            
        if self.relationship is None or self.relationship == "":
            raise ValueError('relationship not specified')
            
        if self.computation_mode not in ["COMPUTE_ALL_FEATS", "SELECT_FEATS"]:
            raise ValueError('Invalid computation_mode, should be COMPUTE_ALL_FEATS or SELECT_FEATS')
            
        if len(self.list_features) == 0:
            raise ValueError('No algorithm selected')
            
        for f in self.list_features:
            if f not in ALGO_GENERAL_CONFIG.keys():
                raise ValueError('Algo %s not implemented'%f)
                
            
        if self.computation_mode == "SELECT_FEATS":
            
            for f in self.list_features:
                
                algo_params = self.custom_params.get(f)
                mandatory_params = ALGO_GENERAL_CONFIG.get(f).get("additional_params")
                
                for p in algo_params.keys():
                    
                    #check that no mandatory parameters defined in ALGO_GLOBAL_SETTINGS have been overrided. If yes, ignore custom params
                    #print(mandatory_params.keys())
                    if p in mandatory_params.keys():
                        print("WARNING. Parameter %s is set to %s by design in the computation of %s, %s will be ignored"%(p, mandatory_params[p], f, algo_params[p]))
                        self.custom_params[f][p] = mandatory_params[p]
                    
                    #check custom parameters value is not empty
                    if algo_params[p] is None or algo_params[p] == "":
                        raise ValueError('Parameter %s of %s is not specified')%(p, f)
                        
                #check that we don't have write = false and writeProperty set
                if ("write" in algo_params.keys()) and (algo_params["write"] == "false") and (algo_params["writeProperty"] is not None):
                    raise ValueError('Having write:false and writeProperty not empty is not compatible for %s')%f
                
                

class AnalyticsQueryRecipe(object):
    """
    Object to generate a cypher query and get the DSS schema to be written in output 
    """
    def __init__(self, params, neo4jhandle):
        self.algo_general_config = ALGO_GENERAL_CONFIG
        self.params = params
        self.ts = int(time.time())
        self.neo4jhandle = neo4jhandle

    def get_algo_call(self, feat_name):
        """
        Generate the cypher CALL function associated to the feature to compute feat_name
        """
        feature_settings = self.algo_general_config.get(feat_name)
        config = feature_settings.get('additional_params')
        
        if self.params.is_weight and feature_settings.get("weight_support"):
            config["weightProperty"] = self.params.weight
        
        if self.params.computation_mode == "SELECT_FEATS":
            print("config before; %s" % (config,))
            print("params.custom_params: %s" % (self.params.custom_params,))
            config.update(self.params.custom_params[feat_name])
            print("config after; %s" % (config,))
            
        #if writeProperty is set, the property won't be deleted at the end of the recipe
        config["write"] = "true"
        if config.get("writeProperty") is None:
            wp = "DKU_TMP_%s_%s"%(self.ts, feat_name)
            config["writeProperty"] = wp
        else:
            wp = config["writeProperty"]
            
        q = """CALL algo.%s('%s','%s', %s)"""%(
            feature_settings.get('function'),
            self.params.node_label,
            self.params.relationship,
            self.stringify_config(config)
        )
        
        return q, wp
    
    def stringify_config(self, config):
        """
        Convert a dictionary of optional parameters into a string cypher compatible
        """
        list_str = []

        for key in config.keys():  
            try :
                val = int(config[key])
            except:
                try :
                    val = float(config[key])
                except:
                    if config[key] in ["true", "false"]:
                        val = config[key]
                    else:
                        val = "'" + config[key] + "'"
            list_str.append( "%s:%s"%(key, val))
            
        return '{' + (', ').join(list_str) + '}'
    
    def generate_queries(self):
        """
        Generate final cypher queries
        """
        
        node_prop_query_part = (', ').join(
            ["n.%s as %s"%(prop, self.params.node_properties[prop]) for prop in self.params.node_properties.keys()]
        )
        
        algo_call_queries = {}
        wp_to_delete = []
        feat_query_part = ""
        
        for feat_name in self.params.list_features:
            q, wp = self.get_algo_call(feat_name)
            algo_call_queries[feat_name] = q
            
            feat_query_part = feat_query_part + ", n.%s as %s"%(wp, feat_name)
            
            if wp.startswith("DKU_TMP_"):
                wp_to_delete.append(wp)
                
        main_query = """MATCH (n:%s) RETURN %s%s"""%(self.params.node_label, node_prop_query_part, feat_query_part)
        
        if len(wp_to_delete) == 0:
            delete_query = None
        else:
            delete_query = """MATCH (n:%s) REMOVE %s"""%(
                self.params.node_label,
                (', ').join(["n.%s"%dku_wp for dku_wp in wp_to_delete])
            )
            
        query_dict = {"algo_calls":algo_call_queries, "main":main_query, "delete":delete_query}
        return query_dict

            
    def get_schema(self):
        """
        Return DSS output schema
        """
        schema = []
          
        #First infer properties type from 10000 first rows
        type_query = """MATCH (n:%s) RETURN %s LIMIT 10000"""%(
            self.params.node_label,
            (', ').join(["apoc.meta.type(n.{0}) as {0}".format(prop) for prop in self.params.node_properties.keys()])
        )
        df_type = self.neo4jhandle.run(type_query).to_data_frame()
        
        
        for prop in self.params.node_properties.keys():
            
            list_type = list(df_type[prop].unique())
            list_type_no_null = [t for t in list_type if t != "NULL"]
            
            print(prop, list_type)
            
            #NULL only
            if len(list_type_no_null) == 0:
                print("WARNING : propery type %s has been infered from 10000 nodes and, all properies were empty. Will use type string"%(prop))
                schema.append({"name": self.params.node_properties[prop], "type":"string"})
            
            #One single type
            elif len(list_type_no_null) == 1:
                if list_type_no_null[0] == "INTEGER":
                    schema.append({"name": self.params.node_properties[prop], "type":"int"})
                elif list_type_no_null[0] == "FLOAT":
                    schema.append({"name": self.params.node_properties[prop], "type":"double"})
                elif list_type_no_null[0] == "BOOLEAN":
                    schema.append({"name": self.params.node_properties[prop], "type":"boolean"})
                else:
                    if list_type[0] != "STRING":
                        print("WARNING : type %s not supported, property %s will be stored as string"%(list_type_no_null[0], prop))
                    schema.append({"name": self.params.node_properties[prop], "type":"string"})
                    
            elif len(list_type_no_null) == 2 and "INTEGER" in list_type_no_null and "FLOAT" in list_type_no_null:
                schema.append({"name": self.params.node_properties[prop], "type":"double"})
                
            else:
                print("Type %s has been found in property %s, will be stored as string"%((', ').join(list_type_no_null), prop))
                schema.append({"name": self.params.node_properties[prop], "type":"string"})
        
        #Then use predefined types for computed features
        for f in self.params.list_features:
            schema.append({"name": f, "type":self.algo_general_config.get(f).get("result_type")})
        return schema
    
    
    def run_queries(self, query_dict):
                
        #Compute algo calls
        print("Start computing algo calls")
        for f in query_dict["algo_calls"].keys():
            print("Computing %s"%f)
            print(query_dict["algo_calls"][f])
            self.neo4jhandle.run(query_dict["algo_calls"][f])    
        print("Done computing algo calls")
        
        #Run merge query
        print("Start computing merge query")
        print(query_dict["main"])
        query_result = self.neo4jhandle.run(query_dict["main"])
        print("Done computing merge query")
        
        #Remove computed scores for neo4j unless specified in custom params
        if query_dict["delete"]:
            print("Start deleting temporary properties")
            print(query_dict["delete"])
            self.neo4jhandle.run(query_dict["delete"])
            print("Done deleting temporary properties")
        else:
            print("Nothing to delete")
        
        return query_result