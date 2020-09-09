# Neo4j Plugin

The purpose of this Dataiku Plugin is to allow DSS users to read and write from/to the [Neo4j](https://neo4j.com/) graph platform.
Neo4j offers a popular database and language (Cypher), which lets users query and analyze graph data structures.


## Plugin Components

The Plugin is made of the following components:

* a **Dataset** type to get data out of Neo4j, either entire groups of nodes, or using a custom Cypher query
* 2 **Recipes** to export a Dataiku datasets into Neo4j
* a **Macro** to run arbitrary Cypher statements against the Neo4j database, to be used when there is no need to output a Dataiku dataset

## Using the Plugin

### Prerequisites

- The main prerequisite to use this Plugin is to know the credentials to connect to the Neo4j database (URI, username, password). You need to create a plugin preset with these parameters (or ask your admin) in *Plugins > Neo4j > Settings > Neo4j server configuration*.

### Usage scenario
The different components allow for different use cases:

* the Macro can be used when a user needs to simply **interact with the database** and when no output Dataset is required.  
It could be used for instance to perform maintenance tasks on the database, create indices, test Cypher queries, delete nodes...
* the Dataset can be used when a user needs to **get data outside of Neo4j and into DSS**. The connector allows to retrieve all nodes with a given label,  
or to perform an arbitrary [Cypher](https://neo4j.com/docs/cypher-manual/current/) query. The resulting DSS Dataset can be used in a larger Flow and  
blended with other data sources as required, and could serve as an input for a ML model (specifically, one could use Cypher to create graph-related features for a model)
* The Recipes can be used when a users needs to **load DSS data into Neo4j**. A typical use case would be to use Neo4j specific features to perform analytics  
and investigations on the resulting graph. These recipes need to be used as follows:
  * Load Nodes first
    * Create a DSS Dataset by type of Nodes, with a node key and a set of node attributes (columns)
    * Use the "Export Node" functionality for each of these Datasets to create the Nodes in Neo4j
  * Then load Relationships
    * Create a DSS Dataset by type of Relationship, with at least 2 columns defining the two nodes part of the relationship (a left-hand side node key, and right-hand side node key), and optionally other columns that can be used as attributes
    * Use the "Export Relationships" functionality to load the data into Neo4j, specifying how these Relationships match with existing nodes, and which "verb" defines the relationship
  * Or load Nodes and Relationships together
    * Create a DSS Dataset of Relationships that can contain both Nodes and Relationships attributes.
    * Use the "Quick export" recipe to create both the Nodes and Relationships at once in Neo4j
* The Recipes require an output Folder that is stored in the Neo4j import directory:
  * Use a Server's filesystem connection if the Neo4j instance is local and specify the Neo4j import directory as "root path"
  * Use a SCP/SFTP connection if Neo4j runs on a separate server than DSS, and specify the Neo4j import directory as "path from"

## Plugin limitations and improvements

* Only simple relationships are handled, in the form of (a)-(rel:REL)->(b)
* Only URI's in the form of "bolt://" have been tested, with a username / password based authentication


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.
