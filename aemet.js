(function () {
    'use strict';

    // Crear el conector
    var myConnector = tableau.makeConnector();

    // El método init es llamado cuando se carga el conector web
    myConnector.init = function(initCallback) {
        // Configurar autenticación - No se necesita autenticación de usuario, solo la API key
        tableau.authType = tableau.authTypeEnum.none;
        
        console.log("Fase del conector:", tableau.phase);
        
        // Si estamos en la fase interactiva, podemos configurar la interfaz de usuario
        if (tableau.phase === tableau.phaseEnum.interactivePhase) {
            // Esto es solo para debugging
            console.log("En fase interactiva");
        } else if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            // Esto es solo para debugging
            console.log("En fase de recolección de datos");
        }
        
        initCallback();
    };

    // Define el esquema que tu conector proporcionará
    myConnector.getSchema = function(schemaCallback) {
        console.log("Obteniendo esquema...");
        
        // Obtener datos de conexión que se pasaron al enviar el formulario
        var connectionData;
        try {
            connectionData = JSON.parse(tableau.connectionData || '{}');
            console.log("Datos de conexión:", connectionData);
        } catch (e) {
            console.error("Error al analizar los datos de conexión:", e);
            connectionData = {};
        }
        
        var dataType = connectionData.dataType || 'estaciones';
        var cols = [];
        var tableSchema = {};
        
        // Definir esquemas para diferentes tipos de datos
        if (dataType === "estaciones") {
            cols = [
                { id: "indicativo", alias: "Indicativo", dataType: tableau.dataTypeEnum.string },
                { id: "nombre", alias: "Nombre", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", alias: "Provincia", dataType: tableau.dataTypeEnum.string },
                { id: "altitud", alias: "Altitud", dataType: tableau.dataTypeEnum.int },
                { id: "longitud", alias: "Longitud", dataType: tableau.dataTypeEnum.float },
                { id: "latitud", alias: "Latitud", dataType: tableau.dataTypeEnum.float },
                { id: "indsinop", alias: "Índice SINOP", dataType: tableau.dataTypeEnum.string }
            ];
            tableSchema = {
                id: "estacionesAEMET",
                alias: "Estaciones meteorológicas de AEMET",
                columns: cols
            };
        } else if (dataType === "prediccion") {
            cols = [
                { id: "municipio", alias: "Municipio", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", alias: "Provincia", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", alias: "Fecha", dataType: tableau.dataTypeEnum.string },
                { id: "temperatura_maxima", alias: "Temperatura Máxima", dataType: tableau.dataTypeEnum.float },
                { id: "temperatura_minima", alias: "Temperatura Mínima", dataType: tableau.dataTypeEnum.float },
                { id: "estado_cielo", alias: "Estado del Cielo", dataType: tableau.dataTypeEnum.string },
                { id: "probabilidad_precipitacion", alias: "Prob. Precipitación", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "prediccionAEMET",
                alias: "Predicción meteorológica diaria AEMET",
                columns: cols
            };
        } else if (dataType === "observacion") {
            cols = [
                { id: "idema", alias: "Identificador de Estación", dataType: tableau.dataTypeEnum.string },
                { id: "estacion", alias: "Estación", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", alias: "Fecha y Hora", dataType: tableau.dataTypeEnum.string },
                { id: "temperatura", alias: "Temperatura", dataType: tableau.dataTypeEnum.float },
                { id: "precipitacion", alias: "Precipitación", dataType: tableau.dataTypeEnum.float },
                { id: "humedad_relativa", alias: "Humedad Relativa", dataType: tableau.dataTypeEnum.float },
                { id: "velocidad_viento", alias: "Velocidad Viento", dataType: tableau.dataTypeEnum.float },
                { id: "direccion_viento", alias: "Dirección Viento", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "observacionAEMET",
                alias: "Datos de observación AEMET",
                columns: cols
            };
        }
        
        console.log("Esquema generado:", tableSchema);
        schemaCallback([tableSchema]);
    };

    // Método para obtener los datos - NO lo modificamos para evitar problemas CORS
    // Si modificamos cómo se realiza la petición, podríamos tener problemas de CORS
    myConnector.getData = function(table, doneCallback) {
        console.log("Obteniendo datos...");
        
        // Obtener los datos de conexión
        var connectionData;
        try {
            connectionData = JSON.parse(tableau.connectionData || '{}');
        } catch (e) {
            console.error("Error al analizar los datos de conexión:", e);
            tableau.abortWithError("Error al analizar los datos de conexión: " + e.message);
            return;
        }
        
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType || 'estaciones';
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
        var tableData = [];
        
        if (!apiKey) {
            console.error("No se ha proporcionado una API key");
            tableau.abortWithError("No se ha proporcionado una API key");
            return;
        }
        
        // Construir la URL según el tipo de datos
        var baseUrl = "https://opendata.aemet.es/opendata/api";
        var apiUrl = "";
        
        if (dataType === "estaciones") {
            apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
        } else if (dataType === "prediccion") {
            apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
        } else if (dataType === "observacion") {
            apiUrl = baseUrl + "/observacion/convencional/todas";
        }
        
        console.log("URL de la API:", apiUrl);
        
        // Primera petición para obtener la URL de los datos
        $.ajax({
            url: apiUrl,
            method: "GET",
            headers: {
                "api_key": apiKey,
                "Accept": "application/json"
            },
            dataType: "json",
            success: function(resp) {
                console.log("Respuesta inicial:", resp);
                
                if (resp.estado === 200 && resp.datos) {
                    // URL de los datos reales
                    var datosUrl = resp.datos;
                    
                    // Segunda petición para obtener los datos
                    $.ajax({
                        url: datosUrl,
                        method: "GET",
                        headers: {
                            "api_key": apiKey,
                            "Accept": "application/json"
                        },
                        dataType: "json",
                        success: function(data) {
                            console.log("Datos recibidos - primeros elementos:", 
                                Array.isArray(data) && data.length > 2 ? data.slice(0, 2) : data);
                            
                            if (dataType === "estaciones") {
                                // Procesar datos de estaciones
                                for (var i = 0; i < data.length; i++) {
                                    var item = data[i];
                                    tableData.push({
                                        "indicativo": (item.indicativo || "").toString(),
                                        "nombre": (item.nombre || "").toString(),
                                        "provincia": (item.provincia || "").toString(),
                                        "altitud": parseInt(item.altitud) || 0,
                                        "longitud": parseFloat(item.longitud) || 0,
                                        "latitud": parseFloat(item.latitud) || 0,
                                        "indsinop": (item.indsinop || "").toString()
                                    });
                                }
                            } else if (dataType === "prediccion") {
                                // Procesar datos de predicción
                                if (Array.isArray(data) && data.length > 0) {
                                    var prediccionData = data[0];
                                    
                                    if (prediccionData && prediccionData.prediccion && prediccionData.prediccion.dia) {
                                        var dias = prediccionData.prediccion.dia;
                                        for (var i = 0; i < dias.length; i++) {
                                            var dia = dias[i];
                                            tableData.push({
                                                "municipio": (prediccionData.nombre || "").toString(),
                                                "provincia": (prediccionData.provincia || "").toString(),
                                                "fecha": (dia.fecha || "").toString(),
                                                "temperatura_maxima": dia.temperatura && dia.temperatura.maxima ? parseFloat(dia.temperatura.maxima) : 0,
                                                "temperatura_minima": dia.temperatura && dia.temperatura.minima ? parseFloat(dia.temperatura.minima) : 0,
                                                "estado_cielo": dia.estadoCielo && dia.estadoCielo.length > 0 ? (dia.estadoCielo[0].descripcion || "").toString() : "",
                                                "probabilidad_precipitacion": dia.probPrecipitacion && dia.probPrecipitacion.length > 0 ? parseFloat(dia.probPrecipitacion[0].value) || 0 : 0
                                            });
                                        }
                                    }
                                }
                            } else if (dataType === "observacion") {
                                // Procesar datos de observación
                                for (var i = 0; i < data.length; i++) {
                                    var item = data[i];
                                    tableData.push({
                                        "idema": (item.idema || "").toString(),
                                        "estacion": (item.ubi || "").toString(),
                                        "fecha": (item.fint || "").toString(),
                                        "temperatura": parseFloat(item.ta) || 0,
                                        "precipitacion": parseFloat(item.prec) || 0,
                                        "humedad_relativa": parseFloat(item.hr) || 0,
                                        "velocidad_viento": parseFloat(item.vv) || 0,
                                        "direccion_viento": parseFloat(item.dv) || 0
                                    });
                                }
                            }
                            
                            console.log("Total de filas procesadas:", tableData.length);
                            
                            if (tableData.length > 0) {
                                console.log("Primera fila como ejemplo:", tableData[0]);
                            } else {
                                console.warn("No se encontraron datos para procesar");
                            }
                            
                            table.appendRows(tableData);
                            doneCallback();
                        },
                        error: function(jqXHR, textStatus, errorThrown) {
                            console.error("Error al obtener datos:", textStatus, errorThrown);
                            console.error("Respuesta:", jqXHR.responseText);
                            tableau.abortWithError("Error al obtener datos: " + textStatus);
                        }
                    });
                } else {
                    console.error("Respuesta de API inválida:", resp);
                    tableau.abortWithError("Respuesta de API inválida: " + (resp.descripcion || "Error desconocido"));
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.error("Error en petición inicial:", textStatus, errorThrown);
                console.error("Estado:", jqXHR.status);
                console.error("Respuesta:", jqXHR.responseText);
                
                // Mostrar un mensaje más descriptivo
                var errorMsg = "Error al conectar con la API AEMET (" + jqXHR.status + "): " + textStatus;
                if (jqXHR.status === 0) {
                    errorMsg += ". Posible problema CORS - verifica que estés usando el simulador WDC o que el servidor permita CORS.";
                } else if (jqXHR.status === 401 || jqXHR.status === 403) {
                    errorMsg += ". Problema de autenticación - verifica tu API key.";
                }
                
                tableau.abortWithError(errorMsg);
            }
        });
    };

    // Registrar el conector
    tableau.registerConnector(myConnector);
    
    // Cuando el documento esté listo
    $(document).ready(function() {
        // Mostrar/ocultar campo de municipio
        $('#dataType').change(function() {
            $('#municipioGroup').toggle($(this).val() === 'prediccion');
        });
        
        // Inicializar la visibilidad del campo de municipio
        $('#municipioGroup').toggle($('#dataType').val() === 'prediccion');
        
        // Manejar el botón de envío
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
            
            // Guardar los datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Nombre de la conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar
            tableau.submit();
        });
    });
})();
