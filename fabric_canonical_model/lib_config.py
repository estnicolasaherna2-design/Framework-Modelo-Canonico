
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
        "execution",
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
        self.lakehouse = config.get("lakehouse")
        self.execution = config["execution"]
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

        unknown_fields = sorted(
            set(config) - set(FrameworkConfig.REQUIRED_FIELDS)
        )

        if unknown_fields:
            raise ConfigurationError(
                "Propiedades desconocidas en la configuracion general: "
                f"{', '.join(unknown_fields)}"
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

        if not isinstance(config["environment"], str) or not config["environment"].strip():
            raise ConfigurationError(
                "La propiedad 'environment' debe contener un valor valido."
            )

        execution = config["execution"]
        if not isinstance(execution, dict):
            raise ConfigurationError(
                "La propiedad 'execution' debe ser un diccionario."
            )
        unknown_execution_fields = sorted(
            set(execution) - {"default_mode", "timezone"}
        )
        if unknown_execution_fields:
            raise ConfigurationError(
                "Propiedades desconocidas en 'execution': "
                f"{', '.join(unknown_execution_fields)}"
            )
        if set(execution) != {"default_mode", "timezone"}:
            raise ConfigurationError(
                "'execution' debe contener 'default_mode' y 'timezone'."
            )
        if execution["default_mode"] not in {
            "FULL_LOAD",
            "UPSERT",
            "SCD2"
        }:
            raise ConfigurationError(
                "'execution.default_mode' debe ser FULL_LOAD, UPSERT o SCD2."
            )
        if not isinstance(execution["timezone"], str) or not execution["timezone"].strip():
            raise ConfigurationError(
                "'execution.timezone' debe contener un valor valido."
            )

        for field, required_keys in (
            ("paths", {"config_root", "registry", "entities"}),
            ("schemas", {"canonical", "metadata"})
        ):
            value = config[field]
            if not isinstance(value, dict):
                raise ConfigurationError(
                    f"La propiedad '{field}' debe ser un diccionario."
                )
            unknown_keys = sorted(set(value) - required_keys)
            missing_keys = sorted(required_keys - set(value))
            if unknown_keys:
                raise ConfigurationError(
                    f"Propiedades desconocidas en '{field}': "
                    f"{', '.join(unknown_keys)}"
                )
            if missing_keys:
                raise ConfigurationError(
                    f"Faltan propiedades en '{field}': "
                    f"{', '.join(missing_keys)}"
                )
            for key, item in value.items():
                if not isinstance(item, str) or not item.strip():
                    raise ConfigurationError(
                        f"'{field}.{key}' debe contener un valor valido."
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
        Obtiene el nombre del Lakehouse si se proporciona de forma opcional.
        """
        if not self.lakehouse:
            raise ConfigurationError(
                "El Lakehouse debe obtenerse del contexto de ejecucion, "
                "no de framework.yml."
            )

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
