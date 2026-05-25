import logging

from dao.conexion import SparkConnector
from dao.demograficos_dao import *
from etl.demograficos_etl import RawDemograficosEtl, CommonDemograficosEtl, BussinesDemograficosEtl
import util.Utilerias as util

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

ruta_json = "/home/arturo/PycharmProjects/mx-demograficos/datos/entrada/personas_dummy.json"
ruta_demo_bronce = "datos/salida/bronce"
ruta_demo_plata = "datos/salida/plata"
try:
    # Ingesta de la Capa Bronce
    spark = SparkConnector().get_session()

    etl_demograf_mx = RawDemograficosEtl(spark, ruta_json)
    etl_demograf_mx.extraccion()
    etl_demograf_mx.transformacion()
    etl_demograf_mx.carga()

    # Ingesta de la capa Plata
    etl_demo_plata = CommonDemograficosEtl(spark, ruta_demo_bronce)
    etl_demo_plata.extraccion()
    etl_demo_plata.transformacion()
    etl_demo_plata.carga()

    # Ingesta de la capa Oro
    etl_demo_oro = BussinesDemograficosEtl(spark, ruta_demo_plata)
    etl_demo_oro.extraccion()
    etl_demo_oro.transformacion()
    etl_demo_oro.carga()

    util.generar_rep_eda_demograficos(spark)

except ArchivoNoEncontradoException as e:
    logging.info(e)
except DatosCorruptosException as e:
    logging.error(e)
