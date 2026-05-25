#### Demograficos-mx

El siguiente proceso es una muestra de datos sinteticos de personas en mexico y su informacion personal, son datos dummy por lo que no se esta exponiendo datos  personales al publico en general


##### Herramientas necesarias para su ejecucion
- Apache spark (3.0) descargar los binarios
- Python 3.0


#### Pasos para ejecutar el script

- **1:** Descargar el siguiente paquete que contiene el framework de spark

```sh
#Descargar el framework de spark para poder ejecutar el codigo de manera local
arturo@debian$ wget https://downloads.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3-scala2.13.tgz 
# Descargar el proyecto 
arturo@debian$ git clone https://github.com/ArGonzalezBecerril/Demograficos-Mx/tree/main
```

- **2:** Una vez descargado el proyecto nos situamos dentro del folder Demograficos-mx

```sh
arturo@debian$ cd Demograficos-mx
arturo@debian$ python Inicio.py
```

>Nota Importante para su ejecucion

- Se debe usar el python que viene por defecto en el paquete de Apache Spark
- No es necesario configurar ninguna ruta, ya que todas las rutas son relativas y se encuentran en el mismo proyecto.
- Como sugerencia es recomendable tener un ide tipo pycharm para poder editar y ejecutar el proyecto, de esta manera solo se abre el proyecto en pycharm y en "Project Structure" solo se agregan dos librerias que vienen en el paquete de spark las cuales son pyspark.zip y py4j-0.10.9.7-src estas 2 librerias estan en "/spark-3.5.8-bin-hadoop3/python/lib" 
- Se sugirio crear un zip, pero el proyecto ya se encuentra en git y de ser necesario cuando se descarga solo se opta por descargarlo en formato zip.

Licence Apache.
