"""
Excepciones personalizadas del Fabric Canonical Model Framework.
"""


class CanonicalModelError(Exception):
    """
    Excepcion base del framework.
    Todas las excepciones personalizadas heredan de esta clase.
    """


class ConfigurationError(CanonicalModelError):
    """
    Error relacionado con la configuracion general del framework.
    """


class RegistryError(ConfigurationError):
    """
    Error durante la lectura o validacion del registry.
    """


class EntityConfigurationError(ConfigurationError):
    """
    Error en la configuracion de una entidad canonica.
    """


class SourceError(CanonicalModelError):
    """
    Error relacionado con la lectura o disponibilidad de fuentes.
    """


class WatermarkError(CanonicalModelError):
    """
    Error relacionado con la gestion de marcas de agua incrementales.
    """


class EntityResolutionError(CanonicalModelError):
    """
    Error durante la resolucion e integracion de entidades.
    """


class ValidationError(CanonicalModelError):
    """
    Error durante las validaciones de datos.
    """


class PersistenceError(CanonicalModelError):
    """
    Error durante la escritura o persistencia de entidades canonicas.
    """


class ExecutionError(CanonicalModelError):
    """
    Error general durante la ejecucion del framework.
    """


class PersistenceError(CanonicalModelError):
    """
    Error durante la persistencia de entidades canonicas.
    """