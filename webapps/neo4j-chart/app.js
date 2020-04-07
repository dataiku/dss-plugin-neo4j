let allRows;
let webAppConfig = dataiku.getWebAppConfig()['webAppConfig'];
let webAppDesc = dataiku.getWebAppDesc()['chart']

function draw(viz_config) {
    var config = viz_config;
    console.warn("config in draw: ", config)
    var viz = new NeoVis.default(config);
    viz.render();
}

window.parent.postMessage("sendConfig", "*")

window.addEventListener('message', function(event) {
    if (event.data) {

        event_data = JSON.parse(event.data);
        webAppConfig = event_data['webAppConfig']
        filters = event_data['filters']

        try {
            console.warn("webAppDesc: ", webAppDesc)
            checkWebAppParameters(webAppConfig, webAppDesc);
        } catch (e) {
            dataiku.webappMessages.displayFatalError(e.message);
            return;
        }

        console.warn("webAppConfig: ", webAppConfig)
        
        dataiku.webappBackend.get('reformat_data', {
                            "webAppConfig": JSON.stringify(webAppConfig),
                            "filters": JSON.stringify(filters)
                            })
                    .then(
                        function(data){
                            config_dump_js = data;
                            console.warn("config_dump_js: ", config_dump_js)
                            draw(config_dump_js); 
                        }
                ).catch(error => {
                    console.warn("error of backend")
                    dataiku.webappMessages.displayFatalError(error);
                });
    }
});


