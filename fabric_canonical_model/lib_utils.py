"""
Utilidades generales del Fabric Canonical Model Framework.
"""

from datetime import datetime
from typing import Any, Dict

import yaml

from fabric_canonical_model.exceptions import ConfigurationError


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, yaml.MappingNode):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "expected a mapping node",
                node.start_mark
            )

        self.flatten_mapping(node)
        mapping = {}

        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)

            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found an unhashable key",
                    key_node.start_mark
                ) from exc

            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark
                )

            mapping[key] = self.construct_object(value_node, deep=deep)

        return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _UniqueKeySafeLoader.construct_mapping
)


def get_spark():
    """
    Obtiene la sesion activa de Spark o crea una si no existe.

    Returns:
        SparkSession: Sesion activa de Spark.
    """
    try:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    except Exception as exc:
        raise RuntimeError(
            f"No fue posible obtener la sesion de Spark: {str(exc)}"
        ) from exc


def read_yaml_file(file_path: str) -> Dict[str, Any]:
    """
    Lee un archivo YAML y devuelve su contenido como diccionario.

    Args:
        file_path: Ruta del archivo YAML.

    Returns:
        Dict[str, Any]: Contenido del archivo.

    Raises:
        ConfigurationError: Si el archivo no existe, no puede leerse
        o su contenido no es valido.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            content = yaml.load(file, Loader=_UniqueKeySafeLoader)

        if content is None:
            raise ConfigurationError(
                f"El archivo YAML esta vacio: {file_path}"
            )

        if not isinstance(content, dict):
            raise ConfigurationError(
                f"El contenido YAML debe ser un diccionario: {file_path}"
            )

        return content

    except ConfigurationError:
        raise

    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"No se encontro el archivo YAML: {file_path}"
        ) from exc

    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"El archivo YAML tiene una estructura invalida: {file_path}. "
            f"Detalle: {str(exc)}"
        ) from exc

    except OSError as exc:
        raise ConfigurationError(
            f"No fue posible leer el archivo YAML: {file_path}. "
            f"Detalle: {str(exc)}"
        ) from exc


def validate_required_fields(
    data: Dict[str, Any],
    required_fields: list,
    context: str
) -> None:
    """
    Valida que un diccionario contenga los campos obligatorios.

    Args:
        data: Diccionario que se va a validar.
        required_fields: Lista de campos obligatorios.
        context: Nombre del objeto o configuracion que se valida.

    Raises:
        ConfigurationError: Si faltan campos obligatorios.
    """
    if not isinstance(data, dict):
        raise ConfigurationError(
            f"La configuracion de {context} debe ser un diccionario."
        )

    missing_fields = [
        field
        for field in required_fields
        if field not in data or data[field] is None
    ]

    if missing_fields:
        raise ConfigurationError(
            f"Faltan campos obligatorios en {context}: "
            f"{', '.join(missing_fields)}"
        )


def is_empty(value: Any) -> bool:
    """
    Determina si un valor esta vacio o es nulo.

    Args:
        value: Valor que se desea evaluar.

    Returns:
        bool: True si el valor esta vacio; False en caso contrario.
    """
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    return False


def get_current_timestamp() -> datetime:
    """
    Obtiene la fecha y hora actual.

    Returns:
        datetime: Fecha y hora actual.
    """
    return datetime.now()


def get_current_timestamp_string(
    format_string: str = "%Y-%m-%d %H:%M:%S"
) -> str:
    """
    Obtiene la fecha y hora actual como texto.

    Args:
        format_string: Formato de salida.

    Returns:
        str: Fecha y hora formateada.
    """
    return get_current_timestamp().strftime(format_string)