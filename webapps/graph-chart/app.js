let allRows;
let webAppConfig = dataiku.getWebAppConfig()['webAppConfig'];
let webAppDesc = dataiku.getWebAppDesc()['chart']


function draw(relation_caption) {
    var config = {
        container_id: "graph-chart",
        server_url: "bolt://localhost:7687",
        server_user: "neo4j",
        server_password: "graph_test",
        labels: {
                Label02: {
                      caption: "Source",
                      size: "Age"
                    }
                },
        relationships: {
                INTERACTS: {
                    caption: relation_caption,
                    thickness: "Interactions"
                }
            },
        initial_cypher: "MATCH (src:Label02)-[r]->(tgt) WHERE r.Interactions > 50 RETURN * LIMIT 200"
    }

    var viz = new NeoVis.default(config);
    viz.render();
}

// function draw() {
//     // let data = new google.visualization.arrayToDataTable(allRows, opt_firstRowIsData=true);
//     let data = new google.visualization.DataTable();
//     data.addColumn('string', 'label');
//     data.addColumn('number', 'min_threshold');
//     data.addColumn('number', 'min_value');
//     data.addColumn('number', 'max_value');
//     data.addColumn('number', 'max_threshold');
//     data.addColumn({type: 'string', role: 'style'});

//     for (var i = 0; i < allRows.length; i++) {
//         arr = allRows[i];
//         data.addRow([String(arr[0]), Number(arr[1]), Number(arr[1]), Number(arr[2]), Number(arr[2]), '']);
//     }

//     data.addRow(["Total", 0, 0, Number(arr[2]), Number(arr[2]), 'color: grey;']);
    
//     let options = {
//       legend: 'none',
//       bar: { groupWidth: '90%' }, // Remove space between bars.
//       candlestick: {
//         fallingColor: { strokeWidth: 0, fill: '#a52714' }, // red
//         risingColor: { strokeWidth: 0, fill: '#0f9d58' }   // green
//       }
//     };
//     let chart = new google.visualization.CandlestickChart(document.getElementById('waterfall-chart'));
    
//     chart.draw(data, options);
// }

window.parent.postMessage("sendConfig", "*")

window.addEventListener('message', function(event) {
    if (event.data) {
        console.warn("message 01")
        draw(true);

        webAppConfig = JSON.parse(event.data)['webAppConfig'];
        console.warn("webAppConfig: ", webAppConfig)
        // console.warn("message 22")
        // console.warn("event.data is: ", event.data)
        console.warn("filters: ", JSON.parse(event.data)['filters'])

        // try {
        //     checkWebAppParameters(webAppConfig, webAppDesc);
        // } catch (e) {
        //     dataiku.webappMessages.displayFatalError(e.message);
        //     return;
        // }

        let relation_caption = webAppConfig['relation_caption'];


        // dataiku.webappBackend.get('reformat_data', {"relation_caption": relation_caption})
        //             .then(
        //                 function(data){
        //                     console.warn("call of backend")
        //                     // bool_rel_capt = data['result'];
        //                     // console.warn("bool_rel_capt: ", bool_rel_capt)
        //                     draw(true); 
        //                 }
        //         ).catch(error => {
        //             console.warn("error of backend")
        //             dataiku.webappMessages.displayFatalError(error);
        //         });

        // $.getJSON(getWebAppBackendUrl('reformat_data'), {"relation_caption": relation_caption})
        //     .done(
        //         function(data){
        //             bool_rel_capt = data['results'];
        //             console.warn("bool_rel_capt: ", bool_rel_capt)
        //             draw(bool_rel_capt); 
        //         }
        //     ).error(function(data){
        //         console.warn("error backend call")
        //         dataiku.webappMessages.displayFatalError('Internal Server Error: ' + data.responseText);
        //     });


        // let dataset_name = webAppConfig['dataset'];
        // let category_column = webAppConfig['categories'];
        // let value_column = webAppConfig['values'];
        // let max_displayed_values = webAppConfig['max_displayed_values'];
        // let group_others = webAppConfig['group_others'];

        // try {
        //     if (category_column == value_column) {
        //         throw Error("Columns must be different")
        //     }
        //     if (max_displayed_values > 100) {
        //         throw Error("Max displayed values too high (maximum 100)")
        //     }
        // } catch (e) {
        //     dataiku.webappMessages.displayFatalError(e.message);
        //     return;
        // }
        
        // if (!window.google) {
        //     dataiku.webappMessages.displayFatalError('Failed to load Google Charts library. Check your connection.');
        // } else {

        //     google.charts.load('current', {'packages':['corechart']});
        //     google.charts.setOnLoadCallback(function() {
        //         dataiku.webappBackend.get('reformat_data', {'dataset_name': dataset_name, 'category_column': category_column, 'value_column': value_column, 'max_displayed_values': max_displayed_values, 'group_others': group_others})
        //             .then(
        //                 function(data){
        //                     allRows = data['result'];
        //                     $('#waterfall-chart').html('');
        //                     draw();
        //                 }
        //         ).catch(error => {
        //             dataiku.webappMessages.displayFatalError(error);
        //         });
        //     });
        // };
    }
});




