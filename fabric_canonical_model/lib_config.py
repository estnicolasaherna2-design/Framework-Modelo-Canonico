
"""
Configuracion general del Fabric Canonical Model Framework.
"""

import os
from typing import Any, Dict

from fabric_canonical_model.exceptions import ConfigurationError
from fabric_canonical_model.lib_utils import read_yaml_file


class FrameworkConfig:
    """
    Administra la configuracion general del framework.
    """

    REQUIRED_FIELDS = [
        "environment",
        "lakehouse",
        "paths",
        "schemas"
    ]

    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa la configuracion del framework.

        Args:
            config: Diccionario con la configuracion general.
        """
        self._validate_config(config)

        self.config = config

        self.environment = config["environment"]
        self.lakehouse = config["lakehouse"]
        self.paths = config["paths"]
        self.schemas = config["schemas"]

    @classmethod
    def from_yaml(cls, file_path: str) -> "FrameworkConfig":
        """
        Construye la configuracion a partir de un archivo YAML.

        Args:
            file_path: Ruta del archivo YAML.

        Returns:
            FrameworkConfig: Instancia de configuracion.
        """
        config = read_yaml_file(file_path)

        return cls(config)

    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> None:
        """
        Valida la estructura general de la configuracion.
        """
        if not isinstance(config, dict):
            raise ConfigurationError(
                "La configuracion general debe ser un diccionario."
            )

        missing_fields = [
            field
            for field in FrameworkConfig.REQUIRED_FIELDS
            if field not in config or config[field] is None
        ]

        if missing_fields:
            raise ConfigurationError(
                "Faltan propiedades obligatorias en la configuracion "
                f"general: {', '.join(missing_fields)}"
            )

        if not isinstance(config["paths"], dict):
            raise ConfigurationError(
                "La propiedad 'paths' debe ser un diccionario."
            )

        if not isinstance(config["schemas"], dict):
            raise ConfigurationError(
                "La propiedad 'schemas' debe ser un diccionario."
            )

        if not isinstance(config["lakehouse"], str) or not config["lakehouse"].strip():
            raise ConfigurationError(
                "La propiedad 'lakehouse' debe contener un nombre valido."
            )

        if not isinstance(config["environment"], str) or not config["environment"].strip():
            raise ConfigurationError(
                "La propiedad 'environment' debe contener un valor valido."
            )

    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene una propiedad de primer nivel de la configuracion.
        """
        return self.config.get(key, default)

    def get_path(self, key: str) -> str:
        """
        Obtiene una ruta configurada.

        Args:
            key: Nombre de la ruta.

        Returns:
            str: Ruta configurada.
        """
        value = self.paths.get(key)

        if not isinstance(value, str) or not value.strip():
            raise ConfigurationError(
                f"No existe una ruta valida para la propiedad '{key}'."
            )

        return value

    def get_schema(self, key: str) -> str:
        """
        Obtiene un schema configurado.
        """
        value = self.schemas.get(key)

        if not isinstance(value, str) or not value.strip():
            raise ConfigurationError(
                f"No existe un schema valido para la propiedad '{key}'."
            )

        return value

    def get_lakehouse(self) -> str:
        """
        Obtiene el nombre del Lakehouse configurado.
        """
        return self.lakehouse

    def get_environment(self) -> str:
        """
        Obtiene el ambiente de ejecucion.
        """
        return self.environment

    def get_absolute_path(self, key: str, base_path: str) -> str:
        """
        Construye una ruta absoluta a partir de una ruta relativa.

        Args:
            key: Nombre de la ruta configurada.
            base_path: Directorio base.

        Returns:
            str: Ruta absoluta.
        """
        relative_path = self.get_path(key)

        return os.path.join(base_path, relative_path)
