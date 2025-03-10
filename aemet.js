(function () {
    var myConnector = tableau.makeConnector();
    
    // Definir el esquema según el tipo de datos seleccionado
    myConnector.getSchema = function (schemaCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var cols = [];
        var tableSchema = {};
        
        if (dataType === "estaciones") {
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
        } else if (dataType === "prediccion") {
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
        } else if (dataType === "observacion") {
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
        }
        
        schemaCallback([tableSchema]);
    };

    // Obtener los datos a través del proxy Vercel
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        
        // URL del proxy Vercel
        var proxyUrl = 'https://aemet-proxy.vercel.app/api/aemet-proxy';
        
        // Construir URL con parámetros
        var fullUrl = `${proxyUrl}?dataType=${dataType}&apiKey=${apiKey}`;
        
        // Añadir código de municipio si es necesario
        if (dataType === 'prediccion') {
            fullUrl += `&codigoMunicipio=${codigoMunicipio}`;
        }

        // Solicitud fetch
        fetch(fullUrl)
            .then(response => response.json())
            .then(data => {
                var tableData = [];
                
                // Procesar datos según tipo
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
            })
            .catch(error => {
                console.error("Error:", error);
                tableau.abortWithError(error.toString());
            });
    };

    // Inicializar y registrar el conector
    tableau.registerConnector(myConnector);
    
    // Mostrar/ocultar campo de municipio según el tipo de datos
    $(document).ready(function() {
        $('#dataType').change(function() {
            if ($(this).val() === 'prediccion') {
                $('#municipioGroup').show();
            } else {
                $('#municipioGroup').hide();
            }
        });
        
        $("#submitButton").click(function () {
            var apiKey = $('#apiKey').val().trim();
            var dataType = $('#dataType').val();
            var codigoMunicipio = $('#codigoMunicipio').val().trim();
            
            if (!apiKey) {
                alert("Por favor, introduce una API Key válida de AEMET");
                return;
            }
            
            if (dataType === 'prediccion' && !codigoMunicipio) {
                alert("Para predicciones, debes introducir un código de municipio");
                return;
            }
            
            // Guardar los datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Establecer el nombre de la conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar la conexión a Tableau
            tableau.submit();
        });
    });
})();
