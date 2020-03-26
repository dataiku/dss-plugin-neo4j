from IPython.display import IFrame
import json
import uuid
from string import Template


def neovis_draw_query(query):

    html = """
    <html>
        <head>
            <script src="https://rawgit.com/neo4j-contrib/neovis.js/master/dist/neovis.js"></script>
        </head>   
        <script>
            
            function draw() {
                var config = {
                    container_id: "viz",
                    server_url: "bolt://localhost:7687",
                    server_user: "neo4j",
                    server_password: "graph_test",
                    initial_cypher: "$query"
                }

                var viz = new NeoVis.default(config);
                viz.render();
            }
        </script>
        <body onload="draw()">
            <div id="viz"></div>
        </body>
    </html>"""

    unique_id = str(uuid.uuid4())
    # html = html.format("test")    #, nodes=json.dumps(nodes), edges=json.dumps(edges), physics=json.dumps(physics))

    variables_dict = {
        "query":query
    }

    html = Template(html).substitute(variables_dict)

    # print(Template(html_template))

    filename = "graph-{}.html".format(unique_id)

    file = open(filename, "w")
    file.write(html)
    file.close()

    return IFrame(filename, width="100%", height="400")
