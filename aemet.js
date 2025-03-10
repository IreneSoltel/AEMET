(function() {
    // Inicializar el conector de Tableau
    var myConnector = tableau.makeConnector();

    // Definir el esquema de datos
    myConnector.getSchema = function(schemaCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var cols = [];
        var tableSchema = {};
        
        // Esquemas para diferentes tipos de datos
        switch(dataType) {
            case "estaciones":
                cols = [
                    { id: "indicativo", dataType: tableau.dataTypeEnum.string },
                    { id: "nombre", dataType: tableau.dataTypeEnum.string },
                    { id: "provincia", dataType: tableau.dataTypeEnum.string },
                    { id: "altitud", dataType: tableau.dataTypeEnum.int },
                    { id: "longitud", dataType: tableau.dataTypeEnum.float },
                    { id: "latitud", dataType: tableau.dataTypeEnum.float },
                    { id: "indsinop", dataType: tableau.dataTypeEnum.string }
                ];
                tableSchema = {
                    id: "estacionesAEMET",
                    alias: "Estaciones meteorológicas de AEMET",
                    columns: cols
                };
                break;
            
            case "prediccion":
                cols = [
                    { id: "municipio", dataType: tableau.dataTypeEnum.string },
                    { id: "provincia", dataType: tableau.dataTypeEnum.string },
                    { id: "fecha", dataType: tableau.dataTypeEnum.date },
                    { id: "temperatura_maxima", dataType: tableau.dataTypeEnum.float },
                    { id: "temperatura_minima", dataType: tableau.dataTypeEnum.float },
                    { id: "estado_cielo", dataType: tableau.dataTypeEnum.string },
                    { id: "probabilidad_precipitacion", dataType: tableau.dataTypeEnum.float }
                ];
                tableSchema = {
                    id: "prediccionAEMET",
                    alias: "Predicción meteorológica diaria AEMET",
                    columns: cols
                };
                break;
            
            case "observacion":
                cols = [
                    { id: "idema", dataType: tableau.dataTypeEnum.string },
                    { id: "estacion", dataType: tableau.dataTypeEnum.string },
                    { id: "fecha", dataType: tableau.dataTypeEnum.datetime },
                    { id: "temperatura", dataType: tableau.dataTypeEnum.float },
                    { id: "precipitacion", dataType: tableau.dataTypeEnum.float },
                    { id: "humedad_relativa", dataType: tableau.dataTypeEnum.float },
                    { id: "velocidad_viento", dataType: tableau.dataTypeEnum.float },
                    { id: "direccion_viento", dataType: tableau.dataTypeEnum.float }
                ];
                tableSchema = {
                    id: "observacionAEMET",
                    alias: "Datos de observación AEMET",
                    columns: cols
                };
                break;
        }
        
        schemaCallback([tableSchema]);
    };

    // Obtener los datos
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        var tableData = [];
        
        // URLs base para diferentes endpoints
        var baseUrl = "https://opendata.aemet.es/opendata/api";
        var apiUrl = "";
        
        switch(dataType) {
            case "estaciones":
                apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
                break;
            case "prediccion":
                apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
                break;
            case "observacion":
                apiUrl = baseUrl + "/observacion/convencional/todas";
                break;
        }
        
        // Proxy CORS (sustituir con tu propio proxy)
        var corsProxyUrl = 'https://cors-anywhere.herokuapp.com/';
        var proxyUrl = corsProxyUrl + apiUrl;
        
        // Petición AJAX
        $.ajax({
            url: proxyUrl,
            type: "GET",
            dataType: "json",
            headers: {
                "api_key": apiKey,
                "X-Requested-With": "XMLHttpRequest"
            },
            success: function(resp) {
                if (resp.estado === 200 && resp.datos) {
                    var datosUrl = resp.datos;
                    var proxyDatosUrl = corsProxyUrl + datosUrl;
                    
                    $.getJSON(proxyDatosUrl, function(data) {
                        // Lógica de procesamiento de datos
                        switch(dataType) {
                            case "estaciones":
                                tableData = data.map(function(item) {
                                    return {
                                        "indicativo": item.indicativo || "",
                                        "nombre": item.nombre || "",
                                        "provincia": item.provincia || "",
                                        "altitud": parseInt(item.altitud) || 0,
                                        "longitud": parseFloat(item.longitud) || 0,
                                        "latitud": parseFloat(item.latitud) || 0,
                                        "indsinop": item.indsinop || ""
                                    };
                                });
                                break;
                            
                            case "prediccion":
                                if (Array.isArray(data) && data.length > 0) {
                                    var prediccionData = data[0];
                                    if (prediccionData && prediccionData.prediccion && prediccionData.prediccion.dia) {
                                        tableData = prediccionData.prediccion.dia.map(function(dia) {
                                            return {
                                                "municipio": prediccionData.nombre || "",
                                                "provincia": prediccionData.provincia || "",
                                                "fecha": dia.fecha || "",
                                                "temperatura_maxima": dia.temperatura && dia.temperatura.maxima ? parseFloat(dia.temperatura.maxima) : null,
                                                "temperatura_minima": dia.temperatura && dia.temperatura.minima ? parseFloat(dia.temperatura.minima) : null,
                                                "estado_cielo": dia.estadoCielo && dia.estadoCielo.length > 0 ? dia.estadoCielo[0].descripcion : "",
                                                "probabilidad_precipitacion": dia.probP
