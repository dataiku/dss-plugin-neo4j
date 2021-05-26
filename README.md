# Neo4j Plugin

The purpose of this [Dataiku Plugin](https://www.dataiku.com/dss/plugins/info/neo4j.html) is to allow DSS users to read and write from/to the [Neo4j](https://neo4j.com/) graph platform.


## Plugin Components

The Plugin is made of the following components:

* a **Dataset** type to get data out of Neo4j: you can retrieve either all nodes with a specific label, all relationships with a specific type or records from a custom Cypher query
* 2 **Recipes** to export Dataiku datasets into Neo4j
* a **Macro** to run arbitrary Cypher statements against the Neo4j database, to be used when there is no need to output a Dataiku dataset

## Using the Plugin

### Prerequisites

- The main prerequisite to use this Plugin is to know the credentials to connect to the Neo4j database (URI, username, password). You need to create a plugin preset with these parameters (or ask your admin) in *Plugins > Neo4j > Settings > Neo4j server configuration*.

### Usage scenario
The different components allow for different use cases:

* the Macro can be used when a user needs to simply **interact with the database** and when no output Dataset is required.  
It could be used for instance to perform maintenance tasks on the database, create indices, test Cypher queries, delete nodes...
* the Dataset can be used when a user needs to **get data outside of Neo4j and into DSS**. This connector allows to retrieve all nodes (and their properties) with a given label, all relationships (and their properties) with a given type or all records from a custom Cypher query. Note that if you don't enter any node label or relationship type, then it will retrieve a list of either all node labels or all relationship types.
* The Recipes can be used when a users needs to **load DSS data into Neo4j**. The output of the export recipes are folders.
These recipes need to be used as follows:
  * Export nodes
    * This recipe will create new nodes (and add their properties) from the input dataset.
    * If some nodes already exist in the Neo4j database, then only the new properties are added.
  * Export relationships
    * This recipe will create new nodes and relationships (and add their properties) from the input dataset.
    * If some nodes and/or relationships already exist in the Neo4j database, then only the new properties are added and the new relationships are attached to the existing nodes.

Note: If you select *Import data from CSV files*, you need to create a SCP/SFTP connection in DSS to the Neo4j import directory. The output of the export recipes need to be folders stored in this connection. Dataiku unix user must have SSH access to the machine hosting Neo4j.

## Plugin limitations and improvements

* Neo4j 4.2 and higher required

## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.
