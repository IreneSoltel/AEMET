(function () {
    // Crear el conector de Tableau
    var myConnector = tableau.makeConnector();
    
    // Definir el esquema de datos
    myConnector.getSchema = function (schemaCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var cols = [];
        var tableSchema = {};
        
        // Esquema para estaciones meteorológicas
        if (dataType === "estaciones") {
            cols = [
                { id: "indicativo", dataType: tableau.dataTypeEnum.string },
                { id: "nombre", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", dataType: tableau.dataTypeEnum.string },
                { id: "altitud", dataType: tableau.dataTypeEnum.int },
                { id: "longitud", dataType: tableau.dataTypeEnum.float },
                { id: "latitud", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "estacionesAEMET",
                alias: "Estaciones meteorológicas",
                columns: cols
            };
        }
        
        // Esquema para predicción meteorológica
        else if (dataType === "prediccion") {
            cols = [
                { id: "municipio", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", dataType: tableau.dataTypeEnum.date },
                { id: "temperatura_maxima", dataType: tableau.dataTypeEnum.float },
                { id: "temperatura_minima", dataType: tableau.dataTypeEnum.float },
                { id: "estado_cielo", dataType: tableau.dataTypeEnum.string }
            ];
            tableSchema = {
                id: "prediccionAEMET",
                alias: "Predicción meteorológica",
                columns: cols
            };
        }
        
        // Esquema para observaciones
        else if (dataType === "observacion") {
            cols = [
                { id: "estacion", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", dataType: tableau.dataTypeEnum.datetime },
                { id: "temperatura", dataType: tableau.dataTypeEnum.float },
                { id: "humedad", dataType: tableau.dataTypeEnum.float },
                { id: "precipitacion", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "observacionAEMET",
                alias: "Observaciones meteorológicas",
                columns: cols
            };
        }
        
        schemaCallback([tableSchema]);
    };

    // Obtener los datos
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        
        var baseUrl = "https://opendata.aemet.es/opendata/api";
        var apiUrl = "";
        
        // Seleccionar URL según el tipo de datos
        if (dataType === "estaciones") {
            apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
        } else if (dataType === "prediccion") {
            apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
        } else if (dataType === "observacion") {
            apiUrl = baseUrl + "/observacion/convencional/todas";
        }
        
        // Proxy para manejar CORS
        var proxyUrl = "https://api.allorigins.win/raw?url=" + encodeURIComponent(apiUrl);
        
        // Realizar la solicitud
        $.ajax({
            url: proxyUrl,
            type: "GET",
            headers: {
                "api_key": apiKey
            },
            dataType: "json",
            success: function(resp) {
                var tableData = [];
                
                // Procesar datos de estaciones
                if (dataType === "estaciones") {
                    resp.forEach(function(estacion) {
                        tableData.push({
                            "indicativo": estacion.indicativo,
                            "nombre": estacion.nombre,
                            "provincia": estacion.provincia,
                            "altitud": parseInt(estacion.altitud) || 0,
                            "longitud": parseFloat(estacion.longitud) || 0,
                            "latitud": parseFloat(estacion.latitud) || 0
                        });
                    });
                }
                
                // Procesar predicción
                else if (dataType === "prediccion") {
                    var prediccion = resp[0].prediccion.dia;
                    prediccion.forEach(function(dia) {
                        tableData.push({
                            "municipio": resp[0].nombre,
                            "provincia": resp[0].provincia,
                            "fecha": dia.fecha,
                            "temperatura_maxima": parseFloat(dia.temperatura.maxima) || 0,
                            "temperatura_minima": parseFloat(dia.temperatura.minima) || 0,
                            "estado_cielo": dia.estadoCielo[0].descripcion || ""
                        });
                    });
                }
                
                // Procesar observaciones
                else if (dataType === "observacion") {
                    resp.forEach(function(observacion) {
                        tableData.push({
                            "estacion": observacion.ubi || "",
                            "fecha": observacion.fint || "",
                            "temperatura": parseFloat(observacion.ta) || 0,
                            "humedad": parseFloat(observacion.hr) || 0,
                            "precipitacion": parseFloat(observacion.prec) || 0
                        });
                    });
                }
                
                table.appendRows(tableData);
                doneCallback();
            },
            error: function(xhr, status, error) {
                tableau.abortWithError("Error al obtener datos: " + error);
            }
        });
    };

    // Inicializar conector
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.custom;
        initCallback();
    };

    // Registrar conector
    tableau.registerConnector(myConnector);

    // Manejar evento de submit
    $(document).ready(function() {
        $("#submitButton").click(function() {
            var apiKey = $('#apiKey').val().trim();
            var dataType = $('#dataType').val();
            var codigoMunicipio = $('#codigoMunicipio').val().trim() || "28079";
            
            if (!apiKey) {
                alert("Por favor, introduce una API Key válida de AEMET");
                return;
            }
            
            // Guardar datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Establecer nombre de conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar conexión a Tableau
            tableau.submit();
        });

        // Mostrar/ocultar campo de municipio
        $('#dataType').change(function() {
            if ($(this).val() === 'prediccion') {
                $('#municipioGroup').show();
            } else {
                $('#municipioGroup').hide();
            }
        });
    });
})();