PLUGIN_VERSION=1.1.2
PLUGIN_ID=neo4j

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env custom-recipes js plugin.json python-connectors python-lib python-runnables resource
