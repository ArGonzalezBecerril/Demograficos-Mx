import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class DemograficoException(Exception):
    def __init__(self, mensaje):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.error(mensaje)
        super().__init__(mensaje)


class ArchivoNoEncontradoException(DemograficoException):
    def __init__(self, ruta):
        mensaje = f"Archivo no encontrado en la ruta especificada: '{ruta}'"
        super().__init__(mensaje)


class ConexionNoEstablecidaException(DemograficoException):
    def __init__(self, detalle=""):
        mensaje = f"No se pudo establecer conexión con Spark en modo local. Detalle: {detalle}"
        super().__init__(mensaje)


class DatosCorruptosException(DemograficoException):
    def __init__(self, detalle=""):
        mensaje = f"El archivo contiene datos inválidos o estructura rota. Detalle: {detalle}"
        super().__init__(mensaje)
