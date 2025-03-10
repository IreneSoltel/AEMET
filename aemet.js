(function() {
    'use strict';
    
    // Variable global para almacenar datos en caché
    window.cachedTableData = null;
    
    // Crear el conector
    var connector = tableau.makeConnector();
    
    // Definir el esquema
    connector.getSchema = function(schemaCallback) {
        console.log("Obteniendo esquema...");
        
        // Obtener los datos y el esquema
        _getAEMETData(function(data) {
            schemaCallback([{
                id: data.tableId,
                alias: data.tableAlias,
                columns: data.columns
            }]);
        });
    };
    
    // Obtener datos
    connector.getData = function(table, doneCallback) {
        console.log("Obteniendo datos...");
        
        // Obtener los datos del caché o de la API
        _getAEMETData(function(data) {
            // Añadir las filas a la tabla
            table.appendRows(data.rows);
            doneCallback();
        });
    };
    
    // Registrar el conector
    tableau.registerConnector(connector);
    
    // Función auxiliar para obtener datos de AEMET
    function _getAEMETData(callback) {
        // Si ya tenemos los datos en caché, los devolvemos directamente
        if (window.cachedTableData) {
            console.log("Utilizando datos en caché");
            callback(window.cachedTableData);
            return;
        }
        
        // Obtener los datos de conexión
        var connectionData = JSON.parse(tableau.connectionData || '{}');
        
        console.log("Solicitando datos al servidor intermediario...");
        
        // Hacer la solicitud a un servidor PHP intermedio (similar al wdc.php del ejemplo)
        // IMPORTANTE: Debes crear este script PHP en tu servidor
        $.ajax({
            url: 'aemet_proxy.php',  // Ruta a tu script PHP
            method: 'GET',
            data: {
                apiKey: connectionData.apiKey,
                dataType: connectionData.dataType,
                codigoMunicipio: connectionData.codigoMunicipio
            },
            dataType: 'json',
            success: function(response) {
                console.log("Datos recibidos correctamente");
                
                // Guardar en caché para futuros usos
                window.cachedTableData = response;
                
                // Devolver los datos
                callback(response);
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.error("Error obteniendo datos:", textStatus, errorThrown);
                tableau.abortWithError("Error obteniendo datos: " + textStatus);
            }
        });
    }
    
    // Cuando el documento está listo
    $(document).ready(function() {
        // Mostrar/ocultar campo de municipio
        $('#dataType').change(function() {
            $('#municipioGroup').toggle($(this).val() === 'prediccion');
        });
        
        // Inicializar visibilidad del campo de municipio
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
            
            // Configurar los datos de conexión
            tableau.connectionData = JSON.stringify({
                apiKey: apiKey,
                dataType: dataType,
                codigoMunicipio: codigoMunicipio
            });
            
            // Establecer el nombre de la conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar los datos a Tableau
            tableau.submit();
        });
    });
})();
