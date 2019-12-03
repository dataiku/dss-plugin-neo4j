# Neo4j Plugin

The purpose of this Dataiku Plugin is to allow DSS users to read from the [Neo4j](https://neo4j.com/) graph platform.
Neo4j offers a popular database and language (Cypher), which lets users query and analyze graph data structures.


## Plugin Components

The Plugin is made of the following components:

* a **Dataset** type to get data out of Neo4j, either entire groups of nodes, or using a custom Cypher query
* a **Macro** to run arbitrary Cypher statements against the Neo4j database, to be used when there is no need to output a Dataiku dataset

## Using the Plugin

### Prerequisites

- The main prerequisite to use this Plugin is to know the credentials to connect to the Neo4j database (URI, username, password) These parameters need to be entered in the Plugin global settings (available from the *Plugins > Neo4j > Settings* pane), possibly by a DSS admin.

### Usage scenario
The different components allow for different use cases:

* the Macro can be used when a user needs to simply **interact with the database** and when no output Dataset is required.  
It could be used for instance to perform maintenance tasks on the database, create indices, test Cypher queries, delete nodes...
* the Dataset can be used when a user needs to **get data outside of Neo4j and into DSS**. The connector allows to retrieve all nodes with a given label,  
or to perform an arbitrary [Cypher](https://neo4j.com/docs/cypher-manual/current/) query. The resulting DSS Dataset can be used in a larger Flow and  
blended with other data sources as required, and could serve as an input for a ML model (specifically, one could use Cypher to create graph-related features for a model)

## Plugin limitations and improvements

* Only simple relationships are handled, in the form of (a)-(rel:REL)->(b)
* Only URI's in the form of "bolt://" have been tested, with a username / password based authentication


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.
