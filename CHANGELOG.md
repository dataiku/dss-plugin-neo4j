# Changelog

## [Version 1.3.1](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.3.1) - Bugfix release - 2021-11

- Allow missing values in int columns (instead of converting to float)

## [Version 1.3.0](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.3.0) - Feature improvements release - 2021-04

### General
- Use of the official Neo4j python driver instead of py2neo

### Export recipes
- Option to export nodes and relationships without using the Neo4j import directory as output folder
- Parameter to choose the batch size and the PERIODIC COMMIT size in the export recipes
- Export the dataset using multiple CSV files when exporting from the Neo4j import directory
- Optional parameters to create node count and relationship weight when exporting relationships
- Options to ignore row where either the source or target node doesn't exist when exporting relationships

### Dataset
- Retrieve data from a custom Cypher query in the Dataset component

### Macro
- Execute multiple Cypher queries in the Macro component