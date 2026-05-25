from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class SparkConnector:
    
    def get_session(self):
        app_name = "Demograficos_Mx"
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()
