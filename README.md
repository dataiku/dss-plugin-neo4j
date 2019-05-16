# Neo4j Plugin


## Overview

The purpose of this Dataiku Plugin is to allow DSS users to interact with the [Neo4j](https://neo4j.com/) graph platform. Neo4j offers a popular database and language (Cypher), which lets users query and analyse graph data structures. 
The current version of the Plugin has different options to both read and write from/to Neo4j.

## Plugin Components

The Plugin is made of the following components:

* a **Macro** to run arbitrary Cypher statements against the Neo4j database, to be used when there is no need to output a Dataiku Dataset
* a custom **Dataset** to get data out of Neo4j, either entire groups of nodes, or using a custom Cypher query
* 

## Using the Plugin

### Prerequisites
In order to use this Plugin, you will need:

* network connectivity open between the Neo4j host server and the DSS host server 
* password-less ssh access from the DSS host server (for the user running DSS) to the Neo4j server (for the user running Neo4j) in order to copy files over scp 

### Plugin components
The Plugin has the following components:

* an "Export Nodes" custom recipe that takes a DSS Dataset as input and creates Neo4j nodes
* an "Export Relationships" custom recipe that takes a DSS Dataset as input and creates Neo4j relationships between existing nodes

### Using the Plugin
The Plugin needs to be used in 2 steps:

1. Create one Dataset per node label/type with the required attributes in columns, and no duplicates, then use the "Export Nodes" recipe to load these nodes into Neo4j
2. Create one Dataset stroring the relationships (2 columns) as well as their attributes - and no duplicates, then use the "Export Relationships" recipe to load them into Neo4j


### Plugins limitations and potential improvements

* The Plugins use LOAD CSV to load data into Neo4j. It assumes that DSS and Neo4j are running on 2 separate servers, and thus requires ssh access from the DSS host to the Neo4j host to copy input files.
* Nodes and relationships attributes are all created as string - the Plugin does not handle data types
* Only simple relationships are handled, in the form of (a)-(rel:REL)->(b)
* Only URI's in the form of "bolt://" have been tested, with a username / password based authentication


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.