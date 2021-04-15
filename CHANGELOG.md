# Changelog

## [Version 2.0.0](https://github.com/dataiku/dss-plugin-neo4j/tree/v2.0.0) - Feature improvements release - 2021-04-00

- Retrieve data from a custom Cypher query in the Dataset component
- Option to export nodes and relationships without using the Neo4j import directory as output folder
- Use of the official Neo4j python driver instead of py2neo
- Parameter to choose the batch size in the export recipes
- Split the export into multiple CSV files if using the Neo4j import directory in the export recipes
- Execute multiple Cypher queries in the Macro component
