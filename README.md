Para ejecutar esto compilaba usando maven y luego ejecutaba el siguiente comando:

```sh
hadoop jar [Archivo compliado].jar [La dirección a la clase principal]  [Direccion del archivo de entrada] [Dirección donde se va a guardar el sequence file] [Dirección donde se va a almacenar el resultado]
```

Todas estas direcciones estan en el hdfs. Ejemplo de ejecución:

```sh
hadoop jar prueba_kenny.jar com.map.reduces.SortVideosByLikesAndViews /user/hadoop/input/BR.csv ouput/sequence_file_prueba ouput/kenny_prueba
```