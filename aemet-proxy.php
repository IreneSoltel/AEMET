<?php
/**
 * Script proxy para obtener datos de la API de AEMET
 * Este script debe alojarse en un servidor PHP.
 */

// Permitir CORS desde cualquier origen (para desarrollo)
header("Access-Control-Allow-Origin: *");
header("Access-Control-Allow-Headers: Content-Type, api_key");
header("Content-Type: application/json");

// Obtener parámetros
$apiKey = $_GET['apiKey'] ?? '';
$dataType = $_GET['dataType'] ?? 'estaciones';
$codigoMunicipio = $_GET['codigoMunicipio'] ?? '28079';

// Validar la clave API
if (empty($apiKey)) {
    http_response_code(400);
    echo json_encode([
        'error' => 'Se requiere una clave API'
    ]);
    exit;
}

// Configurar la URL de la API según el tipo de datos
$baseUrl = "https://opendata.aemet.es/opendata/api";
$apiUrl = "";

switch ($dataType) {
    case 'estaciones':
        $apiUrl = $baseUrl . "/valores/climatologicos/inventarioestaciones/todasestaciones";
        break;
    case 'prediccion':
        $apiUrl = $baseUrl . "/prediccion/especifica/municipio/diaria/" . $codigoMunicipio;
        break;
    case 'observacion':
        $apiUrl = $baseUrl . "/observacion/convencional/todas";
        break;
    default:
        http_response_code(400);
        echo json_encode([
            'error' => 'Tipo de datos no válido'
        ]);
        exit;
}

// Realizar la primera petición para obtener la URL de los datos
$ch = curl_init($apiUrl);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_HTTPHEADER, [
    'api_key: ' . $apiKey,
    'Accept: application/json'
]);

$response = curl_exec($ch);
$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);

// Comprobar si la petición fue exitosa
if ($status !== 200) {
    http_response_code($status);
    echo json_encode([
        'error' => 'Error al conectar con la API de AEMET',
        'status' => $status,
        'response' => $response
    ]);
    exit;
}

// Decodificar la respuesta
$responseData = json_decode($response, true);

// Comprobar si la respuesta contiene la URL de los datos
if (!isset($responseData['datos']) || $responseData['estado'] !== 200) {
    http_response_code(500);
    echo json_encode([
        'error' => 'Respuesta inválida de la API de AEMET',
        'response' => $responseData
    ]);
    exit;
}

// Realizar la segunda petición para obtener los datos
$ch = curl_init($responseData['datos']);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_HTTPHEADER, [
    'api_key: ' . $apiKey,
    'Accept: application/json'
]);

$datosResponse = curl_exec($ch);
$datosStatus = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);

// Comprobar si la petición fue exitosa
if ($datosStatus !== 200) {
    http_response_code($datosStatus);
    echo json_encode([
        'error' => 'Error al obtener los datos de AEMET',
        'status' => $datosStatus,
        'response' => $datosResponse
    ]);
    exit;
}

// Decodificar los datos
$datos = json_decode($datosResponse, true);

// Preparar la respuesta para el conector WDC
$result = [
    'tableId' => '',
    'tableAlias' => '',
    'columns' => [],
    'rows' => []
];

// Configurar el esquema y procesar los datos según el tipo
switch ($dataType) {
    case 'estaciones':
        $result['tableId'] = 'estacionesAEMET';
        $result['tableAlias'] = 'Estaciones meteorológicas de AEMET';
        $result['columns'] = [
            ['id' => 'indicativo', 'dataType' => 'string', 'alias' => 'Indicativo'],
            ['id' => 'nombre', 'dataType' => 'string', 'alias' => 'Nombre'],
            ['id' => 'provincia', 'dataType' => 'string', 'alias' => 'Provincia'],
            ['id' => 'altitud', 'dataType' => 'int', 'alias' => 'Altitud'],
            ['id' => 'longitud', 'dataType' => 'float', 'alias' => 'Longitud'],
            ['id' => 'latitud', 'dataType' => 'float', 'alias' => 'Latitud'],
            ['id' => 'indsinop', 'dataType' => 'string', 'alias' => 'Índice SINOP']
        ];
        
        foreach ($datos as $item) {
            $result['rows'][] = [
                'indicativo' => (string)($item['indicativo'] ?? ''),
                'nombre' => (string)($item['nombre'] ?? ''),
                'provincia' => (string)($item['provincia'] ?? ''),
                'altitud' => (int)($item['altitud'] ?? 0),
                'longitud' => (float)($item['longitud'] ?? 0),
                'latitud' => (float)($item['latitud'] ?? 0),
                'indsinop' => (string)($item['indsinop'] ?? '')
            ];
        }
        break;
        
    case 'prediccion':
        $result['tableId'] = 'prediccionAEMET';
        $result['tableAlias'] = 'Predicción meteorológica diaria AEMET';
        $result['columns'] = [
            ['id' => 'municipio', 'dataType' => 'string', 'alias' => 'Municipio'],
            ['id' => 'provincia', 'dataType' => 'string', 'alias' => 'Provincia'],
            ['id' => 'fecha', 'dataType' => 'string', 'alias' => 'Fecha'],
            ['id' => 'temperatura_maxima', 'dataType' => 'float', 'alias' => 'Temperatura Máxima'],
            ['id' => 'temperatura_minima', 'dataType' => 'float', 'alias' => 'Temperatura Mínima'],
            ['id' => 'estado_cielo', 'dataType' => 'string', 'alias' => 'Estado del Cielo'],
            ['id' => 'probabilidad_precipitacion', 'dataType' => 'float', 'alias' => 'Prob. Precipitación']
        ];
        
        if (is_array($datos) && count($datos) > 0) {
            $prediccionData = $datos[0];
            if (isset($prediccionData['prediccion']['dia'])) {
                foreach ($prediccionData['prediccion']['dia'] as $dia) {
                    $result['rows'][] = [
                        'municipio' => (string)($prediccionData['nombre'] ?? ''),
                        'provincia' => (string)($prediccionData['provincia'] ?? ''),
                        'fecha' => (string)($dia['fecha'] ?? ''),
                        'temperatura_maxima' => (float)($dia['temperatura']['maxima'] ?? 0),
                        'temperatura_minima' => (float)($dia['temperatura']['minima'] ?? 0),
                        'estado_cielo' => (string)(isset($dia['estadoCielo'][0]) ? $dia['estadoCielo'][0]['descripcion'] : ''),
                        'probabilidad_precipitacion' => (float)(isset($dia['probPrecipitacion'][0]) ? $dia['probPrecipitacion'][0]['value'] : 0)
                    ];
                }
            }
        }
        break;
        
    case 'observacion':
        $result['tableId'] = 'observacionAEMET';
        $result['tableAlias'] = 'Datos de observación AEMET';
        $result['columns'] = [
            ['id' => 'idema', 'dataType' => 'string', 'alias' => 'Identificador de Estación'],
            ['id' => 'estacion', 'dataType' => 'string', 'alias' => 'Estación'],
            ['id' => 'fecha', 'dataType' => 'string', 'alias' => 'Fecha y Hora'],
            ['id' => 'temperatura', 'dataType' => 'float', 'alias' => 'Temperatura'],
            ['id' => 'precipitacion', 'dataType' => 'float', 'alias' => 'Precipitación'],
            ['id' => 'humedad_relativa', 'dataType' => 'float', 'alias' => 'Humedad Relativa'],
            ['id' => 'velocidad_viento', 'dataType' => 'float', 'alias' => 'Velocidad Viento'],
            ['id' => 'direccion_viento', 'dataType' => 'float', 'alias' => 'Dirección Viento']
        ];
        
        foreach ($datos as $item) {
            $result['rows'][] = [
                'idema' => (string)($item['idema'] ?? ''),
                'estacion' => (string)($item['ubi'] ?? ''),
                'fecha' => (string)($item['fint'] ?? ''),
                'temperatura' => (float)($item['ta'] ?? 0),
                'precipitacion' => (float)($item['prec'] ?? 0),
                'humedad_relativa' => (float)($item['hr'] ?? 0),
                'velocidad_viento' => (float)($item['vv'] ?? 0),
                'direccion_viento' => (float)($item['dv'] ?? 0)
            ];
        }
        break;
}

// Devolver el resultado
echo json_encode($result);
?>