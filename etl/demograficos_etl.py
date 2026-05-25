import logging
from dao.demograficos_dao import DemograficoJson, DemograficoParquet
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class RawDemograficosEtl:
    def __init__(self, spark, ruta_archivo):
        self.ruta_archivo = ruta_archivo
        self.df_demograficos = None
        self.df_demograficos_raw = None
        self.spark = spark
        self.ruta_destino = "datos/salida/bronce"

    def extraccion(self):
        lector = DemograficoJson(self.ruta_archivo)
        self.df_demograficos = lector.obten(self.spark)

    def transformacion(self):
        self.df_demograficos.createOrReplaceTempView("tbl_demograficos")
        self.df_demograficos_raw = self.spark.sql("""
            SELECT 
                apellido_materno,
                apellido_paterno,
                codigo_postal,
                contacto,
                email,
                estado_republica,
                fecha_nacimiento,
                genero,
                id_persona,
                nombre,
                notas,
                ubicacion,
                CURRENT_TIMESTAMP() AS ingestion_timestamp,
                INPUT_FILE_NAME() AS source_file
            FROM tbl_demograficos
        """)

    def carga(self):
        self.df_demograficos_raw.coalesce(1).write.mode("overwrite").parquet(self.ruta_destino)
        logging.info("[CARGA] Capa Bronce guardada exitosamente en datos/salida/bronce")


class CommonDemograficosEtl:
    def __init__(self, spark, ruta_archivo):
        self.ruta_archivo = ruta_archivo
        self.df_demograficos = None
        self.df_demograficos_common = None
        self.spark = spark
        self.ruta_destino = "datos/salida/plata"

    def extraccion(self):
        lector = DemograficoParquet(self.ruta_archivo)
        self.df_demograficos = lector.obten(self.spark)

    def transformacion(self):
        # Limpieza de nombres y apellidos
        df_demograf_common = self.df_demograficos \
            .withColumn("nombre_limpio", F.upper(F.trim(F.regexp_replace(F.col("nombre"), "@", "")))) \
            .withColumn("apellido_paterno_limpio",
                        F.upper(F.trim(F.regexp_replace(F.col("apellido_paterno"), "@", "")))) \
            .withColumn("apellido_materno_limpio",
                        F.upper(F.trim(F.regexp_replace(F.col("apellido_materno"), "@", ""))))
        # Normalizacion de generos
        df_demograf_common = df_demograf_common.withColumn("genero_limpio",
                                                           F.when(
                                                               F.upper(F.trim(F.col("genero"))).isin("M", "MASCULINO",
                                                                                                     "H", "HOMBRE"),
                                                               "MASCULINO")
                                                           .when(F.upper(F.trim(F.col("genero"))).isin("F", "FEMENINO",
                                                                                                       "MUJER"),
                                                                 "FEMENINO")
                                                           .otherwise("NO ESPECIFICADO"))
        # Formateo de fecha y calcular la edad
        df_demograf_common = df_demograf_common \
            .withColumn("fecha_nac_parsed", F.to_date(F.col("fecha_nacimiento"), "yyyy-MM-dd")) \
            .withColumn("edad_calculada", F.floor(F.months_between(F.current_date(), F.col("fecha_nac_parsed")) / 12))
        # Acentos
        df_demograf_common = df_demograf_common.withColumn("estado_sin_acentos",
                                                           F.translate(F.upper(F.trim(F.col("estado_republica"))),
                                                                       "ÁÉÍÓÚÜÑ", "AEIOUUN"))
        # Acentos para estados (Borrarlos)
        df_demograf_common = df_demograf_common.withColumn("estado_final",
                                                           F.when(F.col("estado_sin_acentos").isin("CDMX", "DF",
                                                                                                   "CIUDAD DE MEXICO"),
                                                                  "CIUDAD DE MEXICO")
                                                           .when(F.col("estado_sin_acentos").isin("EDOMEX",
                                                                                                  "ESTADO DE MEXICO"),
                                                                 "ESTADO DE MEXICO")
                                                           .otherwise(F.col("estado_sin_acentos"))).drop(
            "estado_sin_acentos")
        # Codigo Postal
        df_demograf_common = df_demograf_common \
            .withColumn("cp_origen", F.col("ubicacion.domicilio.referencia_cp")) \
            .withColumn("cp_limpio_previo", F.regexp_replace(F.col("cp_origen"), r"(?i)CP-", "")) \
            .withColumn("cp_limpio", F.trim(F.col("cp_limpio_previo"))) \
            .withColumn("is_cp_valido", F.when(F.col("cp_limpio").rlike(r"^\d{5}$"), True).otherwise(False)) \
            .drop("cp_limpio_previo")

        # Extraemos contacto
        df_demograf_common = df_demograf_common \
            .withColumn("telefono_movil", F.col("contacto.telefonos.movil")) \
            .withColumn("telefono_fijo", F.col("contacto.telefonos.fijo")) \
            .withColumn("email_principal", F.col("email")) \
            .withColumn("is_email_valido",
                        F.when(F.col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), True).otherwise(False)) \
            .withColumn("via_contacto_parsed", F.upper(F.trim(F.col("contacto.preferencias_contacto.canal_preferido")))) \
            .withColumn("direccion_parsed", F.upper(F.trim(F.col("ubicacion.domicilio.colonia"))))

        df_demograf_common = df_demograf_common.withColumn("is_registro_alterado",
                                                           F.when(F.col("notas").contains(
                                                               "registro marcado con campo extra no estándar"),
                                                                  True).otherwise(False))\
            .withColumn("fec_carga", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))

        self.df_demograficos_common = self.renombrado_de_campos(df_demograf_common)

    def carga(self):
        self.df_demograficos_common.coalesce(1).write.mode("overwrite").parquet(self.ruta_destino)
        logging.info("[CARGA] Capa Plata guardada exitosamente en datos/salida/plata")

    def renombrado_de_campos(self, df_demograficos):
        desc_campos = {
            "id_persona": "Identificador único de la persona",
            "nom_persona": "Nombre de la persona",
            "a_paterno": "Apellido paterno de la persona",
            "a_materno": "Apellido materno de la persona",
            "genero": "Género de la persona",
            "f_nacimiento": "Fecha de nacimiento",
            "edad": "Edad de la persona",
            "estado": "Estado de la república donde vive",
            "direccion": "Dirección o colonia de la persona",
            "codigo_postal": "Código postal",
            "es_cp_valido": "Indica si el código postal es válido",
            "num_tel_celular": "Número de teléfono celular",
            "num_tel_casa": "Número de teléfono fijo o de casa",
            "email_persona": "Correo electrónico de la persona",
            "es_email_valida": "Indica si el correo electrónico es válido",
            "via_contacto": "Medio de contacto preferido por la persona",
            "es_registro_alterado": "Indica si el registro venía con texto extraño en las notas"
        }
        # Campos que se publicaran en Capa Plata
        df_renombrado = df_demograficos.select(
            F.col("id_persona").alias("id_persona"),
            F.col("nombre_limpio").alias("nom_persona"),
            F.col("apellido_paterno_limpio").alias("a_paterno"),
            F.col("apellido_materno_limpio").alias("a_materno"),
            F.col("genero_limpio").alias("genero"),
            F.col("fecha_nac_parsed").alias("f_nacimiento"),
            F.col("edad_calculada").alias("edad"),
            F.col("estado_final").alias("estado"),
            F.col("direccion_parsed").alias("direccion"),
            F.col("cp_limpio").alias("codigo_postal"),
            F.col("is_cp_valido").alias("es_cp_valido"),
            F.col("telefono_movil").alias("num_tel_celular"),
            F.col("telefono_fijo").alias("num_tel_casa"),
            F.col("email_principal").alias("email_persona"),
            F.col("is_email_valido").alias("es_email_valida"),
            F.col("via_contacto_parsed").alias("via_contacto"),
            F.col("is_registro_alterado").alias("es_registro_alterado"),
            "ingestion_timestamp",
            "source_file",
            "fec_carga"
        )
        col_con_metadatos = []
        for campo in df_renombrado.schema.fields:
            if campo.name in desc_campos:
                nuevo_metadata = dict(campo.metadata)
                nuevo_metadata["comment"] = desc_campos[campo.name]
                campo = campo.__class__(campo.name, campo.dataType, campo.nullable, nuevo_metadata)
            col_con_metadatos.append(campo)
        logging.info("Transformacion de campos en capa plata, finalizado")
        return self.spark.createDataFrame(df_renombrado.rdd, StructType(col_con_metadatos))


class BussinesDemograficosEtl:
    def __init__(self, spark, ruta_archivo):
        self.ruta_archivo = ruta_archivo
        self.df_demograficos = None
        self.df_rangos_edad = None
        self.df_mtra_demografia = None
        self.df_demograficos_calidad = None
        self.spark = spark
        self.ruta_destino = "datos/salida/oro/"

    def extraccion(self):
        lector = DemograficoParquet(self.ruta_archivo)
        self.df_demograficos = lector.obten(self.spark)

    def transformacion(self):
        # 1 Rangos de Edad
        self.df_rangos_edad = self.df_demograficos.withColumn("rango_edad",
                                                        F.when(F.col("edad") < 18, "Menor_de_Edad")
                                                        .when((F.col("edad") >= 18) & (F.col("edad") <= 65), "Adulto")
                                                        .when(F.col("edad") > 65, "Adulto_Mayor")
                                                        .otherwise("No calculado")
                                                        ).withColumn("fyh_carga", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
        # 2 Agrupacion por estado, edad y genero
        self.df_mtra_demografia = self.df_rangos_edad \
            .groupBy("estado", "rango_edad", "genero") \
            .agg(
            F.count("id_persona").alias("total_personas"),
            F.round(F.avg("edad"), 1).alias("edad_promedio_grupo")
        ).orderBy("estado", "rango_edad").withColumn("fyh_carga", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))

        self.df_demograficos_calidad = self.df_rangos_edad \
            .groupBy("estado") \
            .agg(F.count("id_persona").alias("registros_totales"),
            F.round((F.sum(F.when(F.col("es_cp_valido") == True, 1).otherwise(0)) / F.count("id_persona")) * 100,2).alias("pct_cp_valido"),
            F.round((F.sum(F.when(F.col("es_email_valida") == True, 1).otherwise(0)) / F.count("id_persona")) * 100,2).alias("pct_email_valido"),
            F.sum(F.when(F.col("es_registro_alterado") == True, 1).otherwise(0)).alias("total_registros_alterados")
        ).orderBy(F.desc("total_registros_alterados")).withColumn("fyh_carga", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))

    def carga(self):
        self.df_rangos_edad.coalesce(1).write.mode("overwrite").parquet(f"{self.ruta_destino}mtra_demograficos")
        self.df_mtra_demografia.coalesce(1).write.mode("overwrite").parquet(f"{self.ruta_destino}mtra_grp_estados_demograficos")
        self.df_demograficos_calidad.coalesce(1).write.mode("overwrite").parquet(f"{self.ruta_destino}mtra_kpi_demograficos")

        logging.info("[CARGA] Capa Oro guardada exitosamente en datos/salida/oro")
