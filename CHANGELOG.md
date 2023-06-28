# Changelog

## [Version 1.5.0](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.5.0) - New feature releas - 2023-06

- Add python 3.7, 3.8, 3.9, 3.10, 3.11 support

## [Version 1.4.1](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.4.1) - Bugfix release - 2023-05

- Specify whether or not to include the default pandas NaN values and use additional strings to recognize as NaN values when exporting data to Neo4j

## [Version 1.4.0](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.4.0) - New feature release - 2022-12

- Support neo4j 4.4 and higher (4.2 and 4.3 not supported anymore)

## [Version 1.3.2](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.3.2) - New feature release - 2021-12

- Support connection to multiple databases

## [Version 1.3.1](https://github.com/dataiku/dss-plugin-neo4j/tree/v1.3.1) - Bugfix release - 2021-11

- Allow missing values in int columns (instead of raising a specific error message)

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
