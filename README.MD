# Proyecto de Gestión de Grandes Volúmenes de Datos

Este repositorio contiene el código fuente para la entrega del segundo corte del proyecto de la materia de Gestión de Grandes Volúmenes de Datos.

## Pregunta 1 (Kenny)

La pregunta 1 la respondi usando MapReduce y Hive. Para la parte de MapReduce el codigo importante esta en la carpeta `codigo de maprReduce de kenny para la pregunta 1` en el cual esta un proyecto de maven. Los archivos mas importantes son:

- `/map_reduces/map_reduces/metricas_engagement/src/main/java/com/map/reduces/Main.java`: Este archivo contiene toda la configuración para ejecutar el varios trabajos que al final lo que hacen es calcular las proporciones de los videos de los dataSets. 

- `/map_reduces/map_reduces/metricas_engagement/src/main/java/com/map/reduces/TrendingDateAndIdVideoKey.java`: El codigo de una clave compuesta de solo la fecha en la que un video se hizo trending y el Id de dicho video, esto se hizo para poder ordenar (Por qué? Por que si lit)


- `/map_reduces/map_reduces/metricas_engagement/src/main/java/com/map/reduces/TrendingMetricsKey.java`: Clave compuesta por las proporciones calculadas para crear el archivo final, mejor explicación en la documentación del trabajo de esta parte en Main.

Para calcular estar proporciones hay 2 fases:
1) Transformar los datos de CSV a Sequence File, para esto se usa la primera key
2) Calcular y almacenar los datos de las proporciones, para esto se utiliza la segunda key


El resto de la pregunta 1 se hizo en hive por limitaciones de tiempo, se explicara mejor el codigo de hive en la carpeta `codigo de kenny para hive`.


## Pregunta 4 (Kenny)

Todo se hizo en hive, revise la carpeta de `codigo de kenny para hive`



