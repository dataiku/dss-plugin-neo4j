/*
Helper function to query webapp backend with a default implementation for error handling
v 1.0.1
*/
function checkWebAppParameters(webAppConfig, webAppDesc) {
    if (webAppDesc.topBarParams) {
        webAppDesc.topBarParams.forEach(p => {
            if (p.mandatory) {
                var val = webAppConfig[p.name];
                if (val == undefined || val == "") {
                    throw new Error("Mandatory parameter '" + p.label + "' not specified.");
                }
            }
        });
    }
    if (webAppDesc.leftBarParams) {
        webAppDesc.leftBarParams.forEach(p => {
            if (p.mandatory) {
                var val = webAppConfig[p.name];
                if (val == undefined || val == "") {
                    throw new Error("Mandatory parameter '" + p.label + "' not specified.");
                }
            }
        });
    }
};


dataiku.webappBackend = (function() {
    function getUrl(path) {
        return dataiku.getWebAppBackendUrl(path);
    }

    function dkuDisplayError(error) {
        console.warn("backend error: ", error);
    }

    function get(path, args={}, displayErrors=true) {
        return fetch(getUrl(path) + '?' + $.param(args), {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        })
        .then(response => {
            if (response.status == 502) {
                throw Error("Webapp backend not started");
            } else if (response.status == 505) {
                response.text().then(text => dataiku.webappMessages.displayFatalError(`Graph query error:\n${text}.`))
                throw Error("Response not ok!")
            } else if (!response.ok) {
                response.text().then(text => dataiku.webappMessages.displayFatalError(`Backend error:\n${text}.\nCheck backend log for more information.`))
                throw Error("Response not ok!")
            } 
            try {
                return response.json();
            } catch {
                throw Error('The backend response is not JSON: '+ response.text());
            }
        })
        .catch(function(error) {
            if (displayErrors && error.message && !error.message.includes('not started')) { // little hack, backend not started should be handled elsewhere
                dataiku.webappMessages.displayFatalError(error)
            }
            throw error;
        });
    }

    return Object.freeze({getUrl, get});
})();


dataiku.webappMessages = (function() {
    function displayFatalError(err) {
        const errElt = $('<div class="fatal-error" style="margin: 100px auto; text-align: center; color: var(--error-red)"></div>')
        errElt.text(err);
        $('#graph-chart').html(errElt);
    }
    return  Object.freeze({displayFatalError})
})();

