(function() {
    'use strict';

    // Log para depuración
    console.log('Iniciando conector AEMET');

    // Crear el conector de Tableau con funciones de depuración
    var myConnector = {
        // Método de inicialización
        init: function(initCallback) {
            console.log('Método init() llamado');
            // Configurar tipo de autenticación
            tableau.authType = tableau.authTypeEnum.none;
            
            // Llamar al callback de inicialización
            initCallback();
        },

        // Definir el esquema de datos
        getSchema: function(schemaCallback) {
            console.log('Método getSchema() llamado');
            
            // Parsear datos de conexión de forma segura
            var connectionData;
            try {
                connectionData = JSON.parse(tableau.connectionData || '{}');
            } catch (error) {
                console.error('Error parsing connection data:', error);
                schemaCallback([], error);
                return;
            }

            var dataType = connectionData.dataType || 'estaciones';
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

            console.log('Esquema generado:', tableSchema);
            schemaCallback([tableSchema]);
        },

        // Obtener datos
        getData: function(table, doneCallback) {
            console.log('Método getData() llamado');
            
            // Parsear datos de conexión de forma segura
            var connectionData;
            try {
                connectionData = JSON.parse(tableau.connectionData || '{}');
            } catch (error) {
                console.error('Error parsing connection data:', error);
                tableau.abortWithError('Error en datos de conexión');
                return;
            }

            var apiKey = connectionData.apiKey;
            var dataType = connectionData.dataType || 'estaciones';
            var codigoMunicipio = connectionData.codigoMunicipio || "28079";
            
            console.log('Datos de conexión:', {
                dataType: dataType,
                codigoMunicipio: codigoMunicipio
            });
            
            // URLs base de AEMET
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

            console.log('URL de la API:', apiUrl);

            // Primera petición para obtener URL de datos
            $.ajax({
                url: apiUrl,
                type: "GET",
                dataType: "json",
                headers: {
                    "api_key": apiKey
                },
                success: function(resp) {
                    console.log('Respuesta inicial:', resp);
                    
                    if (resp.estado === 200 && resp.datos) {
                        // Segunda petición para obtener datos reales
                        $.ajax({
                            url: resp.datos,
                            type: "GET",
                            dataType: "json",
                            headers: {
                                "api_key": apiKey
                            },
                            success: function(data) {
                                console.log('Datos recibidos:', data);
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

                                console.log('Filas procesadas:', tableData.length);
                                table.appendRows(tableData);
                                doneCallback();
                            },
                            error: function(jqXHR, textStatus, errorThrown) {
                                console.error('Error en segunda petición:', textStatus, errorThrown);
                                tableau.abortWithError("Error al obtener datos: " + textStatus);
                            }
                        });
                    } else {
                        console.error('Error en respuesta inicial:', resp);
                        tableau.abortWithError("Error en la respuesta de la API: " + (resp.descripcion || "Desconocido"));
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.error('Error en primera petición:', textStatus, errorThrown);
                    tableau.abortWithError("Error de conexión: " + textStatus);
                }
            });
        }
    };

    // Registrar el conector
    if (typeof tableau !== 'undefined' && tableau.makeConnector) {
        console.log('Registrando conector');
        tableau.registerConnector(myConnector);
    } else {
        console.error('Tableau no está definido o no tiene el método makeConnector');
    }

    // Eventos de la interfaz de usuario
    $(document).ready(function() {
        console.log('Documento listo');

        // Mostrar/ocultar campo de municipio
        $('#dataType').change(function() {
            $('#municipioGroup').toggle($(this).val() === 'prediccion');
        });
        
        // Manejar el envío del formulario
        $("#submitButton").click(function() {
            console.log('Botón de envío clickeado');

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
            try {
                tableau.connectionData = JSON.stringify({
                    "apiKey": apiKey,
                    "dataType": dataType,
                    "codigoMunicipio": codigoMunicipio
                });
                
                // Establecer el nombre de la conexión
                tableau.connectionName = "Datos AEMET - " + dataType;
                
                // Enviar la conexión a Tableau
                tableau.submit();
            } catch (error) {
                console.error('Error al enviar datos:', error);
                alert('Error al procesar la solicitud');
            }
        });
    });
})();
