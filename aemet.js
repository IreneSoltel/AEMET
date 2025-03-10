(function() {
    // Conector de Tableau para AEMET
    var myConnector = tableau.makeConnector();

    // Definición del esquema de datos
    myConnector.getSchema = function(schemaCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var tableSchema;

        // Esquemas para diferentes tipos de datos
        switch(dataType) {
            case "estaciones":
                tableSchema = {
                    id: "estacionesAEMET",
                    alias: "Estaciones meteorológicas de AEMET",
                    columns: [
                        { id: "indicativo", dataType: tableau.dataTypeEnum.string },
                        { id: "nombre", dataType: tableau.dataTypeEnum.string },
                        { id: "provincia", dataType: tableau.dataTypeEnum.string },
                        { id: "altitud", dataType: tableau.dataTypeEnum.int },
                        { id: "longitud", dataType: tableau.dataTypeEnum.float },
                        { id: "latitud", dataType: tableau.dataTypeEnum.float },
                        { id: "indsinop", dataType: tableau.dataTypeEnum.string }
                    ]
                };
                break;

            case "prediccion":
                tableSchema = {
                    id: "prediccionAEMET",
                    alias: "Predicción meteorológica diaria AEMET",
                    columns: [
                        { id: "municipio", dataType: tableau.dataTypeEnum.string },
                        { id: "provincia", dataType: tableau.dataTypeEnum.string },
                        { id: "fecha", dataType: tableau.dataTypeEnum.date },
                        { id: "temperatura_maxima", dataType: tableau.dataTypeEnum.float },
                        { id: "temperatura_minima", dataType: tableau.dataTypeEnum.float },
                        { id: "estado_cielo", dataType: tableau.dataTypeEnum.string },
                        { id: "probabilidad_precipitacion", dataType: tableau.dataTypeEnum.float }
                    ]
                };
                break;

            case "observacion":
                tableSchema = {
                    id: "observacionAEMET",
                    alias: "Datos de observación AEMET",
                    columns: [
                        { id: "idema", dataType: tableau.dataTypeEnum.string },
                        { id: "estacion", dataType: tableau.dataTypeEnum.string },
                        { id: "fecha", dataType: tableau.dataTypeEnum.datetime },
                        { id: "temperatura", dataType: tableau.dataTypeEnum.float },
                        { id: "precipitacion", dataType: tableau.dataTypeEnum.float },
                        { id: "humedad_relativa", dataType: tableau.dataTypeEnum.float },
                        { id: "velocidad_viento", dataType: tableau.dataTypeEnum.float },
                        { id: "direccion_viento", dataType: tableau.dataTypeEnum.float }
                    ]
                };
                break;
        }

        schemaCallback([tableSchema]);
    };

    // Obtención de datos
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        
        // Construcción de URLs
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
        
        // Proxy CORS (sustituir con un proxy real)
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
                        var tableData = [];
                        
                        // Procesamiento de datos según tipo
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
                                                "probabilidad_precipitacion": dia.probPrecipitacion && dia.probPrecipitacion.length > 0 ? parseFloat(dia.probPrecipitacion[0].value) : 0
                                            };
                                        });
                                    }
                                }
                                break;
                            
                            case "observacion":
                                tableData = data.map(function(item) {
                                    return {
                                        "idema": item.idema || "",
                                        "estacion": item.ubi || "",
                                        "fecha": item.fint || "",
                                        "temperatura": parseFloat(item.ta) || null,
                                        "precipitacion": parseFloat(item.prec) || 0,
                                        "humedad_relativa": parseFloat(item.hr) || null,
                                        "velocidad_viento": parseFloat(item.vv) || null,
                                        "direccion_viento": parseFloat(item.dv) || null
                                    };
                                });
                                break;
                        }
                        
                        table.appendRows(tableData);
                        doneCallback();
                    }).fail(function(jqXHR, textStatus, errorThrown) {
                        tableau.abortWithError("Error al obtener datos de AEMET: " + textStatus);
                    });
                } else {
                    tableau.abortWithError("Error en la respuesta de la API de AEMET");
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                tableau.abortWithError("Error de conexión con la API de AEMET: " + textStatus);
            }
        });
    };

    // Función shutdown obligatoria
    myConnector.shutdown = function() {
        console.log("Conector AEMET cerrado.");
    };

    // Registro del conector
    tableau.registerConnector(myConnector);

    // Inicialización de la interfaz
    $(document).ready(function() {
        // Inicializar Tableau
        tableau.init();

        // Mostrar/ocultar campo de municipio
        $('#dataType').change(function() {
            $('#municipioGroup').toggle($(this).val() === 'prediccion');
        });
        
        // Manejar envío del formulario
        $("#submitButton").click(function() {
            var apiKey = $('#apiKey').val().trim();
            var dataType = $('#dataType').val();
            var codigoMunicipio = $('#codigoMunicipio').val().trim();
            
            // Validaciones
            if (!apiKey) {
                alert("Por favor, introduce una API Key válida de AEMET");
                return;
            }
            
            if (dataType === 'prediccion' && !codigoMunicipio) {
                alert("Para predicciones, debes introducir un código de municipio");
                return;
            }
            
            // Guardar datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Nombre de la conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar conexión a Tableau
            tableau.submit();
        });
    });
})();
