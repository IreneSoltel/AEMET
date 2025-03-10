(function () {
    'use strict';

    // Crear el conector de Tableau
    var myConnector = tableau.makeConnector();

    // Método de inicialización requerido
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.none;
        initCallback();
    };
    
    // Definir el esquema según el tipo de datos seleccionado
    myConnector.getSchema = function (schemaCallback) {
        try {
            var connectionData = JSON.parse(tableau.connectionData || '{}');
            var dataType = connectionData.dataType || 'estaciones';
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
                    { id: "fecha", dataType: tableau.dataTypeEnum.string }, // Cambiado a string para evitar problemas de formato
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
                    { id: "fecha", dataType: tableau.dataTypeEnum.string }, // Cambiado a string para evitar problemas de formato
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
        } catch (e) {
            console.error("Error en getSchema:", e);
            schemaCallback([], e);
        }
    };

    // Función para hacer solicitudes a través de un proxy CORS confiable
    function fetchWithProxy(url, apiKey, callback, errorCallback) {
        // Usar un proxy CORS público confiable (AllOrigins)
        var proxyUrl = "https://api.allorigins.win/raw?url=" + encodeURIComponent(url);
        
        $.ajax({
            url: proxyUrl,
            type: "GET",
            dataType: "json",
            headers: {
                "api_key": apiKey
            },
            success: callback,
            error: errorCallback
        });
    }

    // Obtener los datos de la API de AEMET
    myConnector.getData = function(table, doneCallback) {
        try {
            var connectionData = JSON.parse(tableau.connectionData || '{}');
            var apiKey = connectionData.apiKey;
            var dataType = connectionData.dataType || 'estaciones';
            var codigoMunicipio = connectionData.codigoMunicipio || "28079"; // Default a Madrid
            var tableData = [];
            
            // URLs base para diferentes endpoints de la API AEMET
            var baseUrl = "https://opendata.aemet.es/opendata/api";
            var apiUrl = "";
            
            if (dataType === "estaciones") {
                apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
            } else if (dataType === "prediccion") {
                apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
            } else if (dataType === "observacion") {
                apiUrl = baseUrl + "/observacion/convencional/todas";
            }
            
            console.log("Conectando a URL:", apiUrl);
            
            // Primera petición a través del proxy
            fetchWithProxy(
                apiUrl,
                apiKey,
                function(resp) {
                    console.log("Respuesta inicial:", resp);
                    
                    if (resp && resp.estado === 200 && resp.datos) {
                        // Segunda petición para obtener los datos reales
                        fetchWithProxy(
                            resp.datos,
                            apiKey,
                            function(data) {
                                console.log("Datos obtenidos:", data);
                                
                                try {
                                    if (dataType === "estaciones") {
                                        // Procesar datos de estaciones
                                        tableData = data.map(function(item) {
                                            return {
                                                "indicativo": (item.indicativo || "").toString(),
                                                "nombre": (item.nombre || "").toString(),
                                                "provincia": (item.provincia || "").toString(),
                                                "altitud": parseInt(item.altitud) || 0,
                                                "longitud": parseFloat(item.longitud) || 0,
                                                "latitud": parseFloat(item.latitud) || 0,
                                                "indsinop": (item.indsinop || "").toString()
                                            };
                                        });
                                    } else if (dataType === "prediccion") {
                                        // Procesar datos de predicción
                                        if (Array.isArray(data) && data.length > 0) {
                                            var prediccionData = data[0];
                                            
                                            if (prediccionData && prediccionData.prediccion && prediccionData.prediccion.dia) {
                                                tableData = prediccionData.prediccion.dia.map(function(dia) {
                                                    return {
                                                        "municipio": (prediccionData.nombre || "").toString(),
                                                        "provincia": (prediccionData.provincia || "").toString(),
                                                        "fecha": (dia.fecha || "").toString(), // Convertir a string para evitar problemas
                                                        "temperatura_maxima": dia.temperatura && dia.temperatura.maxima ? parseFloat(dia.temperatura.maxima) : 0,
                                                        "temperatura_minima": dia.temperatura && dia.temperatura.minima ? parseFloat(dia.temperatura.minima) : 0,
                                                        "estado_cielo": dia.estadoCielo && dia.estadoCielo.length > 0 ? (dia.estadoCielo[0].descripcion || "").toString() : "",
                                                        "probabilidad_precipitacion": dia.probPrecipitacion && dia.probPrecipitacion.length > 0 ? parseFloat(dia.probPrecipitacion[0].value) || 0 : 0
                                                    };
                                                });
                                            }
                                        }
                                    } else if (dataType === "observacion") {
                                        // Procesar datos de observación
                                        tableData = data.map(function(item) {
                                            return {
                                                "idema": (item.idema || "").toString(),
                                                "estacion": (item.ubi || "").toString(),
                                                "fecha": (item.fint || "").toString(), // Convertir a string para evitar problemas
                                                "temperatura": parseFloat(item.ta) || 0,
                                                "precipitacion": parseFloat(item.prec) || 0,
                                                "humedad_relativa": parseFloat(item.hr) || 0,
                                                "velocidad_viento": parseFloat(item.vv) || 0,
                                                "direccion_viento": parseFloat(item.dv) || 0
                                            };
                                        });
                                    }
                                    
                                    // Asegurarte de que tableData es un array
                                    if (!Array.isArray(tableData)) {
                                        console.warn("tableData no es un array, inicializando array vacío");
                                        tableData = [];
                                    }
                                    
                                    console.log("Filas procesadas:", tableData.length);
                                    
                                    // Verificar la primera fila como ejemplo
                                    if (tableData.length > 0) {
                                        console.log("Ejemplo de primera fila:", tableData[0]);
                                    }
                                    
                                    table.appendRows(tableData);
                                    doneCallback();
                                } catch (e) {
                                    console.error("Error procesando datos:", e);
                                    tableau.abortWithError("Error procesando datos: " + e.message);
                                }
                            },
                            function(jqXHR, textStatus, errorThrown) {
                                console.error("Error en segunda petición:", textStatus, errorThrown);
                                tableau.abortWithError("Error obteniendo datos: " + textStatus);
                            }
                        );
                    } else {
                        console.error("Respuesta inicial inválida:", resp);
                        tableau.abortWithError("Respuesta inválida de la API AEMET");
                    }
                },
                function(jqXHR, textStatus, errorThrown) {
                    console.error("Error en primera petición:", textStatus, errorThrown);
                    tableau.abortWithError("Error conectando con la API AEMET: " + textStatus);
                }
            );
        } catch (e) {
            console.error("Error general en getData:", e);
            tableau.abortWithError("Error general: " + e.message);
        }
    };

    // Registrar el conector
    tableau.registerConnector(myConnector);
    
    // Cuando el documento esté listo
    $(document).ready(function() {
        // Mostrar/ocultar campo de municipio según el tipo de datos
        $('#dataType').change(function() {
            $('#municipioGroup').toggle($(this).val() === 'prediccion');
        });
        
        // Inicializar el estado del campo de municipio
        $('#municipioGroup').toggle($('#dataType').val() === 'prediccion');
        
        // Manejar el envío del formulario
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
