# Neo4j Plugin


## Plugin information

This Plugin will let you interact with the Neo4j graph database directly from Dataiku DSS. 
The current version of the Plugin offers the ability to load Dataiku data directly into Neo4j. In particular, it will let create new nodes and attributes from Dataiku Datasets, as well as setting their properties. 

## Using the Plugin

### Prerequisites
In order to use this Plugin, you will need:

* network connectivity open between the Neo4j host server and the DSS host server 
* password-less ssh access from the DSS host server (for the user running DSS) to the Neo4j server (for the user running Neo4j) in order to copy files over scp 

### Plugin components
The Plugin has the following components:

* an "Export Nodes" custom recipe that takes a DSS Dataset as input and creates Neo4j nodes
* an "Export Relationships" custom recipe that takes a DSS Dataset as input and creates Neo4j relationships between existing nodes

### Using the Plugin in Dataiku

### Plugins limitations and potential improvements

* The Plugins uses LOAD CSV to load data into Neo4j. It thus requires ssh access from the DSS host to the Neo4j host to copy input files. It may not offer the best performances for very large files.
* Nodes and relationships attributes are all created as string - the Plugin does not handle data types


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests.