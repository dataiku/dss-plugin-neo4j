# Neo4j Plugin


## Plugin information

This Plugin will let you interact with the Neo4j graph database directly from Dataiku DSS. 

## Using the Plugin

### Prerequisites
In order to use the Plugin, you will need:

* network connectivity open between the Neo4j host server and the DSS host server 
* password-less ssh access from the DSS host server (for the user running DSS) to the Neo4j server (for the user running Neo4j) in order to copy files over scp 

### Plugin components
The Plugin has the following components:

* an "Export Nodes" custom recipe that takes a DSS Dataset as input and creates Neo4j nodes
* an "Export Relationships" custom recipe that takes a DSS Dataset as input and creates Neo4j relationships between existing nodes

### Using the Plugin in Dataiku


## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests. Among potential improvements, you may want to add:

* support for other methods in the Text Analytics or Computer Vision services (ex: OCR)
* add support for other services