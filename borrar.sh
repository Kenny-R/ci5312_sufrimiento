#!/bin/bash

# Ruta base en HDFS donde se encuentran los archivos
HDFS_BASE_PATH="/user/hadoop2/output"

# Función para borrar directorios específicos en HDFS
delete_from_hdfs() {
    local hdfs_path="$1"

    echo "Borrando en HDFS: $hdfs_path"
    hadoop fs -rm -r "$hdfs_path"
}

# Borrar carpetas XX_pregunta_Y_resultado_Z
for country in US BR MX; do
    for question in 1 4; do
        for result in 1 2; do
            delete_from_hdfs "${HDFS_BASE_PATH}/${country}_pregunta_${question}_resultado_${result}"
        done
    done
done

echo "Borrado completado."