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

- The main prerequisite to use this Plugin is to know the credentials to connect to the Neo4j database (URI, username, password) These parameters need to be entered in the Plugin global settings (available from the *Plugins > Neo4j > Settings* pane), possibly by a DSS admin.

- You must know where the Neo4j "import directory" is located** (cf. [documentation](https://neo4j.com/docs/operations-manual/current/configuration/file-locations/)), otherwise check with your administrators. The **Linux account used by Dataiku must have write permissions on the Neo4j import directory**, owned by default by the Neo4j Linux service account.  
For instance, if *dataiku* is the DSS service account, and */var/lib/neo4j/import/* the Neo4j import directory, the following command can be used:  
```bash
setfacl -m u:dataiku:rwx /var/lib/neo4j/import/
```

* If Neo4j runs on a separate server than DSS, the servers need to be able to communicate through SSH. Dataiku linux user should have password-less SSH access to the machine hosting Neo4j.

### Usage scenario
The different components allow for different use cases:

* the Macro can be used when a user needs to simply **interact with the database** and when no output Dataset is required.  
It could be used for instance to perform maintenance tasks on the database, create indices, test Cypher queries, delete nodes...
* the Dataset can be used when a user needs to **get data outside of Neo4j and into DSS**. The connector allows to retrieve all nodes with a given label,  
or to perform an arbitrary [Cypher](https://neo4j.com/docs/cypher-manual/current/) query. The resulting DSS Dataset can be used in a larger Flow and  
blended with other data sources as required, and could serve as an input for a ML model (specifically, one could use Cypher to create graph-related features for a model)
* the Recipes can be used when a users needs to **load DSS data into Neo4j**. A typical use case would be to use Neo4j specific features to perform analytics  
and investigations on the resulting graph. These recipes need to be used as follows:
  * Load Nodes first
    * Create a DSS Dataset by type of Nodes, with a node key and a set of node attributes (columns)
    * Use the "Export Node" functionality for each of these Datasets to create the Nodes in Neo4j
  * Then load Relationships
    * Create a DSS Dataset by type of Relationship, with at least 2 columns defining the two nodes part of the relationship (a left-hand side node key, and right-hand side node key), and optionally other columns that can be used as attributes
    * Use the "Export Relationships" functionality to load the data into Neo4j, specifying how these Relationships match with existing nodes, and which "verb" defines the relationship


## Plugin limitations and improvements

* Dates are exported to Neo4j as strings
* Only simple relationships are handled, in the form of (a)-(rel:REL)->(b)
* Only URI's in the form of "bolt://" have been tested, with a username / password based authentication


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.
