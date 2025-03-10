(function () {
    'use strict';

    // Función de reintentos para AJAX con manejo de despertares del servidor
    function ajaxWithRetry(options, maxRetries = 3, delay = 5000) {
        return new Promise((resolve, reject) => {
            let retries = 0;
            
            function attempt() {
                console.log(`Intento ${retries + 1} de ${maxRetries}`);
                
                if (retries > 0) {
                    // Actualizar mensaje de carga para mostrar reintento
                    $('#loading-status').text(`Reintentando conectar (intento ${retries + 1} de ${maxRetries})...`);
                }
                
                $.ajax({
                    ...options,
                    success: function(data) {
                        resolve(data);
                    },
                    error: function(jqXHR, textStatus, errorThrown) {
                        console.error(`Error (intento ${retries + 1}):`, textStatus, errorThrown);
                        
                        // Si es un timeout o error 503, reintentamos
                        if ((textStatus === 'timeout' || jqXHR.status === 503 || jqXHR.status === 0) && retries < maxRetries - 1) {
                            retries++;
                            console.log(`Reintentando en ${delay/1000} segundos...`);
                            $('#loading-status').text(`El servidor está despertando, reintentando en ${delay/1000} segundos... (${retries}/${maxRetries-1})`);
                            setTimeout(attempt, delay);
                        } else {
                            reject({jqXHR, textStatus, errorThrown});
                        }
                    }
                });
            }
            
            attempt();
        });
    }

    // Crear el conector de Tableau
    var myConnector = tableau.makeConnector();

    // Método de inicialización
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.none;
        initCallback();
    };

    // Definir el esquema según el tipo de datos seleccionado
    myConnector.getSchema = function (schemaCallback) {
        var connectionData;
        try {
            connectionData = JSON.parse(tableau.connectionData || '{}');
        } catch (e) {
            console.error("Error parsing connection data:", e);
            connectionData = {};
        }
        
        var dataType = connectionData.dataType || 'estaciones';
        var cols = [];
        var tableSchema = {};
        
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
        
        schemaCallback([tableSchema]);
    };

    // Obtener los datos de AEMET a través del proxy
    myConnector.getData = function(table, doneCallback) {
        var connectionData;
        try {
            connectionData = JSON.parse(tableau.connectionData || '{}');
        } catch (e) {
            console.error("Error parsing connection data:", e);
            tableau.abortWithError("Error en los datos de conexión");
            return;
        }
        
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType || 'estaciones';
        var codigoMunicipio = connectionData.codigoMunicipio || "28079";
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
        
        console.log("URL de la API:", apiUrl);
        
        // URL del proxy con los parámetros necesarios
        var proxyUrl = "https://tidy-forested-berry.glitch.me/aemet-proxy?url=" + encodeURIComponent(apiUrl) + "&apiKey=" + encodeURIComponent(apiKey);
        
        // Primera petición para obtener la URL de los datos a través del proxy
        ajaxWithRetry({
            url: proxyUrl,
            type: "GET",
            dataType: "json",
            timeout: 30000 // 30 segundos timeout
        })
        .then(function(resp) {
            console.log("Respuesta inicial a través del proxy:", resp);
            
            if (resp.estado === 200 && resp.datos) {
                // URL de los datos reales
                var datosUrl = resp.datos;
                
                // Segunda petición para obtener los datos a través del proxy
                var proxyDatosUrl = "https://tidy-forested-berry.glitch.me/aemet-proxy?url=" + encodeURIComponent(datosUrl) + "&apiKey=" + encodeURIComponent(apiKey);
                
                return ajaxWithRetry({
                    url: proxyDatosUrl,
                    type: "GET",
                    dataType: "json",
                    timeout: 60000 // 60 segundos timeout para la segunda petición
                });
            } else {
                throw {
                    textStatus: "error", 
                    message: "Respuesta de API inválida: " + (resp.descripcion || "Error desconocido")
                };
            }
        })
        .then(function(data) {
            console.log("Datos recibidos a través del proxy (muestra):", 
                Array.isArray(data) && data.length > 2 ? data.slice(0, 2) : data);
            
            if (dataType === "estaciones") {
                // Procesar datos de estaciones
                for (var i = 0, len = data.length; i < len; i++) {
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
                        for (var i = 0, len = prediccionData.prediccion.dia.length; i < len; i++) {
                            var dia = prediccionData.prediccion.dia[i];
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
                for (var i = 0, len = data.length; i < len; i++) {
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
        })
        .catch(function(error) {
            console.error("Error en petición:", error);
            tableau.abortWithError("Error al conectar con el proxy: " + (error.message || error.textStatus));
        });
    };

    // Registrar el conector con Tableau
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
            
            // Mostrar mensaje de carga
            $("#loading-message").show();
            
            // Guardar datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Establecer nombre de conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar
            tableau.submit();
        });
    });
})();
