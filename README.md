# Neo4j Plugin


## Overview

The purpose of this Dataiku Plugin is to allow DSS users to interact with the [Neo4j](https://neo4j.com/) graph platform. 
Neo4j offers a popular database and language (Cypher), which lets users query and analyse graph data structures. 
The current version of the Plugin has different options to both read and write from/to Neo4j.


## Plugin Components

The Plugin is made of the following components:

* a **Macro** to run arbitrary Cypher statements against the Neo4j database, to be used when there is no need to output a Dataiku Dataset
* a custom **Dataset** to get data out of Neo4j, either entire groups of nodes, or using a custom Cypher query
* four custom **Recipes** to export Dataiku Datasets into Neo4j
* a dedicated **Code Environment** that pulls the required Python libraries


## Using the Plugin

### Prerequisites
The main prerequisite to use this Plugin is to **know the credentials to connect to the Neo4j database**:

* Neo4j database URI
* Neo4j database username
* Neo4j database password

These parameters need to be entered in the Plugin global settings (available from the *Administration > Plugins* pane), 
possibly by a DSS admin. 

Additionally, if Neo4j runs on a separate server than DSS, the **two servers will need to be able to communicate** to exchange data, 
so the proper networking and security configuration has to be set up (to be checked with your administrators).

### Specific Notes
The Macro and Dataset components do not require specific actions or knowledge to be used, beyond the prerequisites above. 

As for the custom Recipes to load data into Neo4j:
* there are two types of Recipes:
  * "Export Nodes", that takes a DSS Dataset as input and creates Neo4j nodes
  * "Export Relationships", that takes a DSS Dataset as input and creates Neo4j relationships between existing nodes
* each type of Recipe comes in two flavors:
  * "Local", to be used when DSS and Neo4j run on the same host machine
  * "Remote", to be used when DSS and Neo4j run on separate machines
  
#### Technical notes and requirements

* The Plugin assumes that **you know where the Neo4j "import directory" is located** (cf. [documentation](https://neo4j.com/docs/operations-manual/current/configuration/file-locations/)), otherwise check with your administrators.
* The **Linux account used by Dataiku must have write permissions on the Neo4j import directory**, owned by default by the Neo4j Linux service account.  
For instance, if *dataiku* is the DSS service account, and */var/lib/neo4j/import/* the Neo4j import directory, the following command can be used:  
```bash
setfacl -m u:dataiku:rwx /var/lib/neo4j/import/
``` 
* **When the "Remote" option is used** (meaning that Neo4j and DSS do not run on the same server), some additional steps are required:
  * the two servers must be able to communicate (check network configuration)
  * the Linux account specified in the Plugin must have password-less SSH access to the machine hosting Neo4j

### Usage scenario
The different components allow for different use cases:

* the Macro can be used when a user needs to simply **interact with the database** and when no output Dataset is required.  
It could be used for instance to perform maintenance tasks on the database, create indices, test Cypher queries, delete nodes...
* the Dataset can be used when a user needs to **get data outside of Neo4j and into DSS**. The connector allows to retrieve all nodes with a given label,  
or to perform an arbitratry [Cypher](https://neo4j.com/docs/cypher-manual/current/) query. The resulting DSS Dataset can be used in larger Flow and  
blended with other data sources as required, and could serve as an input for a ML model (specifically, one could use Cypher to create graph-related features for a model)
* the Recipes can be used when a users needs to **load DSS data into Neo4j**. 


1. Create one Dataset per node label/type with the required attributes in columns, and no duplicates, then use the "Export Nodes" recipe to load these nodes into Neo4j
2. Create one Dataset stroring the relationships (2 columns) as well as their attributes - and no duplicates, then use the "Export Relationships" recipe to load them into Neo4j


## Plugins limitations and potential improvements

* The Plugins use LOAD CSV to load data into Neo4j. It assumes that DSS and Neo4j are running on 2 separate servers, and thus requires ssh access from the DSS host to the Neo4j host to copy input files.
* Nodes and relationships attributes are all created as string - the Plugin does not handle data types
* Only simple relationships are handled, in the form of (a)-(rel:REL)->(b)
* Only URI's in the form of "bolt://" have been tested, with a username / password based authentication


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.