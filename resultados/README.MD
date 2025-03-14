En esta carpeta esta todos los resultados, que se obtuvieron, esta dividido por pais cada país se compone a su vez de las siguiente carpetas:

- XX_proporciones
- XX_pregunta_1_resultado_1
- XX_pregunta_1_resultado_2
- XX_pregunta_4_resultado_1
- XX_pregunta_4_resultado_2

# Detalles

## XX_proporciones

Contiene todas las partes de un sequence File con todos los videos y las proporciones calculadas. La clave de las filas contienen lo siguiente:
- videoId = El id del video
- trendingDate = La fecha en la que el video se hizo trending (un mismo video se puede hacer trending en varias fechas distintas)
- likesToViewsRatio = likes / vistas
- dislikesToViewsRatio = dislikes / vistas
- commentsToViewsRatio = comentarios / vistas
- likesToDislikesRatio = likes / dislikes
- views = vistas del video
- likes = me gustas del video
- dislikes = no me gustas del video
- comments = comentarios del video

y el valor de cada fila es la categoria del video


## XX_pregunta_1_resultado_1

Contiene los videos los videos ordenados por la proporcion de likes/vistas. cada fila del archivo (que es un archivo de texto donde cada valor esta separado por \t) esta conformado por las siguientes columnas:
- ranking = posición del video entre los mejores 200 de ese dia 
- title = Titulo del video
- videoId = El id del video
- trendingDate = La fecha en la que el video se hizo trending (un mismo video se puede hacer trending en varias fechas distintas)
- likesToViewsRatio = likes / vistas
- dislikesToViewsRatio = dislikes / vistas
- commentsToViewsRatio = comentarios / vistas
- likesToDislikesRatio = likes / dislikes
- views = vistas del video
- likes = me gustas del video
- dislikes = no me gustas del video
- comments = comentarios del video

Realmente este archivo esta ordenado siguiendo la siguiente jerarquia:

1) likesToViewsRatio
2) dislikesToViewsRatio
3) commentsToViewsRatio
3) likesToDislikesRatio


## XX_pregunta_1_resultado_2
Contiene los videos los videos ordenados por la cantidad de vistas. cada fila del archivo (que es un archivo de texto donde cada valor esta separado por \t) esta conformado por las siguientes columnas:
- ranking = posición del video entre los mejores 200 de ese dia 
- title = Titulo del video
- videoId = El id del video
- trendingDate = La fecha en la que el video se hizo trending (un mismo video se puede hacer trending en varias fechas distintas)
- likesToViewsRatio = likes / vistas
- dislikesToViewsRatio = dislikes / vistas
- commentsToViewsRatio = comentarios / vistas
- likesToDislikesRatio = likes / dislikes
- views = vistas del video
- likes = me gustas del video
- dislikes = no me gustas del video
- comments = comentarios del video

Realmente este archivo esta ordenado siguiendo la siguiente jerarquia:

1) views
2) likesToViewsRatio
3) dislikesToViewsRatio
4) commentsToViewsRatio
5) likesToDislikesRatio


## XX_pregunta_4_resultado_1

Contiene los canales del dataset del pais XX ordenados por la cantidad de videos que se volvieron tendencia y entraron entre los mejores 50 en una fecha en especifico. Al igual que antes es un archivo de texto donde los datos estan separados por "\t". Cada fila esta conformado por las siguientes columnas:

- trending_date= Fecha en la que el uno o mas videos se hicieron trending de ese canal
- channel_id= Id de canal
- channel_title = Nombre del canal
- distinct_category_count = La cantidad de categorias de videos distintas de este canal que se volvieron trending
- top_videos_count = La cantidad de videos del canal que se volvieron trending y entraron entre los mejores 50 en una fecha en especifico

Este archivo esta ordenado por top_videos_count.


## XX_pregunta_4_resultado_2

Muy similar al resultado anterior solo que ya no se toma en cuenta la fecha, es decir, que contamos el total de videos de una canal el cual se volvio trending y entro entre los mejores 50 en algun momento del tiempo. Nuevamente, es un archivo de texto separado por "\t" y cada fila contiene las siguientes columnas:

- channel_id= Id de canal
- channel_title = Nombre del canal
- distinct_category_count = La cantidad de categorias de videos distintas de este canal que se volvieron trending
- top_videos_count = La cantidad de videos del canal que se volvieron trending y entraron entre los mejores 50 en una fecha en especifico

Este archivo esta ordenado por top_videos_count.
