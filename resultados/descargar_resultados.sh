#!/bin/bash

# Ruta base en HDFS donde se encuentran los archivos
HDFS_BASE_PATH="/user/hadoop2/output"

# Directorio local donde se descargarán los archivos
LOCAL_BASE_PATH="."

# Función para descargar directorios específicos de HDFS a local
download_from_hdfs() {
    local hdfs_path="$1"
    local local_path="$2"

    echo "Descargando desde HDFS: $hdfs_path a $local_path"
    hadoop fs -get "$hdfs_path" "$local_path"
}

# Descargar carpetas XX_proporciones
for country in US BR MX; do
    # Crear el directorio local para el país si no existe
    mkdir -p "${LOCAL_BASE_PATH}/${country}"
    
    download_from_hdfs "${HDFS_BASE_PATH}/${country}_proporciones" "${LOCAL_BASE_PATH}/${country}/${country}_proporciones"
done

# Descargar carpetas XX_pregunta_Y_resultado_Z
for country in US BR MX; do
    # Crear el directorio local para el país si no existe
    mkdir -p "${LOCAL_BASE_PATH}/${country}"
    
    for question in 1 4; do
        for result in 1 2; do
            download_from_hdfs "${HDFS_BASE_PATH}/${country}_pregunta_${question}_resultado_${result}" "${LOCAL_BASE_PATH}/${country}/${country}_pregunta_${question}_resultado_${result}"
        done
    done
done

echo "Descarga completada."