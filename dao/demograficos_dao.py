import os
from util.excepciones import ArchivoNoEncontradoException, DatosCorruptosException

'''
@Descripcion
# Clase que realiza la lectura de un archivo json
'''


class DemograficoJson:
    def __init__(self, ruta):
        self.ruta = ruta

    def obten(self, spark):
        if not os.path.exists(self.ruta):
            raise ArchivoNoEncontradoException(self.ruta)
        try:
            dataframe_json = spark.read \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .json(self.ruta)
            # dataframe_json = spark.read.json(self.ruta)
            return dataframe_json
        except Exception as e:
            raise DatosCorruptosException(str(e))


class DemograficoParquet:
    def __init__(self, ruta):
        self.ruta = ruta

    def obten(self, spark):
        if not os.path.exists(self.ruta):
            raise ArchivoNoEncontradoException(self.ruta)
        try:
            dataframe_parquet = spark.read \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .parquet(self.ruta)
            # dataframe_json = spark.read.json(self.ruta)
            return dataframe_parquet
        except Exception as e:
            raise DatosCorruptosException(str(e))
