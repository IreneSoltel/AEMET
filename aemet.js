(function () {
    // Añadir más logs de depuración
    console.log("Inicializando conector AEMET");

    var myConnector = tableau.makeConnector();
    
    // Registro de log al inicio de cada función importante
    myConnector.init = function(initCallback) {
        console.log("Inicializando conector");
        tableau.authType = tableau.authTypeEnum.custom;
        initCallback();
    };

    myConnector.getSchema = function (schemaCallback) {
        console.log("Obteniendo esquema");
        
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var cols = [];
        var tableSchema = {};
        
        // [Resto del código de getSchema igual]
        
        schemaCallback([tableSchema]);
    };

    myConnector.getData = function(table, doneCallback) {
        console.log("Obteniendo datos");
        
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        
        console.log("Datos de conexión:", {
            dataType: dataType,
            codigoMunicipio: codigoMunicipio
        });
        
        // [Resto del código de getData igual]
    };

    // Registrar conector
    tableau.registerConnector(myConnector);
})();
