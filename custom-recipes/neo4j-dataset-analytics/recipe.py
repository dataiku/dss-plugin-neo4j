from dataiku.customrecipe import get_recipe_config
from commons import write_dataset_from_query_result, get_neo4jhandle
from dku_neo4j import GraphAnalyticsParamsNew, AnalyticsQueryRecipeNew

recipe_params = get_recipe_config()
print("recipe_params: %s" % (recipe_params,))
params = GraphAnalyticsParamsNew(recipe_params)
# params.check()

print("----- params -----: %s" % (params,))
# print("params.weight: %s" % (params.weight,))
print("params.computation_mode %s " % (params.computation_mode,))

neo4jhandle = get_neo4jhandle()
queries_handle = AnalyticsQueryRecipeNew(params, neo4jhandle)

# Get output schema
schema = queries_handle.get_schema()
print("schema: ", schema)

# Run queries
query_dict = queries_handle.generate_queries()

# raise ValueError("right after the generate query")

query_result = queries_handle.run_queries(query_dict)

write_dataset_from_query_result(schema, query_result)
