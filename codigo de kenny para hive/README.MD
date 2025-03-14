Aquí se almacena todo el codigo que use para hive, es bastante intuitivo de leer por lo que aquí solo mencionare muy por encima que hace cada cosa:


# `crear_tabla_ranked`

Crea una tabla con los datos que me paso luis, los cuales son los mismo que hay en el dataset menos la descripcion y agregando la posición de dicho video dentro del ranking de los 200 videos en trending en un dia en particular.


# `crear_tabla_trending_metrics`

Crea una tabla para poder utilizar las distintas proporciones que se calcular para los videos, para mas información revisa la carpeta de resultados.


# `pregunta 1\pregunta_1_resultado_1`

Toma los datos de la tabla de trending_metrics y los ordena por la proporcion likes/vistas. Tambien hace Join con la table de ranked para poder obtener el titulo y la posición del video. Para mas información revisa la carpeta de resultados.


# `pregunta 1\pregunta_1_resultado_2`

Toma los datos de la tabla de trending_metrics y los ordena por la cantidad de vistas. Tambien hace Join con la table de ranked para poder obtener el titulo y la posición del video. Para mas información revisa la carpeta de resultados.


# `pregunta 4\pregunta_4_resultados_1`

Toma los datos de la tabla de ranked para obtener la cantidad de categorias distintas que se volvieron trending de un canal, luego calcula la cantidad de videos de un canal que se volverion trending y entraron en los mejores 50 en una fecha en particular, junta ambos resultados y ordena por la cantidad de videos trending que entraron entre los mejores 50. Para mas información revisa la carpeta de resultados.

# `pregunta 4\pregunta_4_resultados_2`

Toma los datos de la tabla de ranked para obtener la cantidad de categorias distintas que se volvieron trending de un canal, luego calcula la cantidad de videos de un canal que se volverion trending y entraron en los mejores 50, junta ambos resultados y ordena por la cantidad de videos trending que entraron entre los mejores 50. Para mas información revisa la carpeta de resultados.