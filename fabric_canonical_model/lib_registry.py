
"""
Administracion del Registry del modelo canonico.
"""

import os
from typing import Any, Dict, List, Optional

from fabric_canonical_model.exceptions import RegistryError
from fabric_canonical_model.lib_utils import read_yaml_file


class CanonicalRegistry:
    """
    Administra el registro de entidades del modelo canonico.
    """

    def __init__(self, registry_path: str):
        """
        Inicializa el Registry.

        Args:
            registry_path: Ruta del archivo registry.yml.
        """
        self.registry_path = registry_path
        self.registry_directory = os.path.dirname(registry_path)

        self.registry = read_yaml_file(registry_path)

        self._validate_registry()

        self.canonical_model = self.registry["canonical_model"]
        self.entities = self.canonical_model["entities"]

    def _validate_registry(self) -> None:
        """
        Valida la estructura y el contenido del Registry.
        """

        if "canonical_model" not in self.registry:
            raise RegistryError(
                "El Registry debe contener la propiedad 'canonical_model'."
            )

        canonical_model = self.registry["canonical_model"]

        if not isinstance(canonical_model, dict):
            raise RegistryError(
                "La propiedad 'canonical_model' debe ser un diccionario."
            )

        required_fields = ["name", "version", "entities"]

        missing_fields = [
            field
            for field in required_fields
            if field not in canonical_model
            or canonical_model[field] is None
        ]

        if missing_fields:
            raise RegistryError(
                "Faltan propiedades obligatorias en canonical_model: "
                f"{', '.join(missing_fields)}"
            )

        if not isinstance(canonical_model["name"], str) or not canonical_model["name"].strip():
            raise RegistryError(
                "El nombre del modelo canonico debe ser valido."
            )

        if not isinstance(canonical_model["version"], str) or not canonical_model["version"].strip():
            raise RegistryError(
                "La version del modelo canonico debe ser valida."
            )

        if not isinstance(canonical_model["entities"], list):
            raise RegistryError(
                "La propiedad 'entities' debe ser una lista."
            )

        self._validate_entities(canonical_model["entities"])

    @staticmethod
    def _validate_entities(entities: List[Dict[str, Any]]) -> None:
        """
        Valida las definiciones de las entidades registradas.
        """

        entity_names = []

        for index, entity in enumerate(entities):

            if not isinstance(entity, dict):
                raise RegistryError(
                    f"La definicion de la entidad en la posicion {index} "
                    "debe ser un diccionario."
                )

            required_fields = ["name", "enabled", "config"]

            missing_fields = [
                field
                for field in required_fields
                if field not in entity or entity[field] is None
            ]

            if missing_fields:
                raise RegistryError(
                    f"Faltan propiedades en la entidad de la posicion "
                    f"{index}: {', '.join(missing_fields)}"
                )

            name = entity["name"]

            if not isinstance(name, str) or not name.strip():
                raise RegistryError(
                    f"El nombre de la entidad en la posicion {index} "
                    "no es valido."
                )

            if not isinstance(entity["enabled"], bool):
                raise RegistryError(
                    f"La propiedad 'enabled' de la entidad '{name}' "
                    "debe ser booleana."
                )

            if not isinstance(entity["config"], str) or not entity["config"].strip():
                raise RegistryError(
                    f"La ruta de configuracion de la entidad '{name}' "
                    "no es valida."
                )

            if name in entity_names:
                raise RegistryError(
                    f"Existe una entidad duplicada en el Registry: '{name}'."
                )

            entity_names.append(name)

    def get_model_name(self) -> str:
        """
        Obtiene el nombre del modelo canonico.
        """
        return self.canonical_model["name"]

    def get_model_version(self) -> str:
        """
        Obtiene la version del modelo canonico.
        """
        return self.canonical_model["version"]

    def get_all_entities(self) -> List[Dict[str, Any]]:
        """
        Obtiene todas las entidades registradas.
        """
        return self.entities

    def get_enabled_entities(self) -> List[Dict[str, Any]]:
        """
        Obtiene unicamente las entidades habilitadas.
        """
        return [
            entity
            for entity in self.entities
            if entity["enabled"] is True
        ]

    def get_disabled_entities(self) -> List[Dict[str, Any]]:
        """
        Obtiene las entidades deshabilitadas.
        """
        return [
            entity
            for entity in self.entities
            if entity["enabled"] is False
        ]

    def is_entity_enabled(self, entity_name: str) -> bool:
        """
        Consulta si una entidad esta habilitada.

        Args:
            entity_name: Nombre de la entidad.

        Returns:
            bool: True si esta habilitada.
        """
        entity = self.get_entity(entity_name)

        return entity["enabled"]

    def get_entity(
        self,
        entity_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene la definicion de una entidad por su nombre.

        Args:
            entity_name: Nombre de la entidad.

        Returns:
            Dict o None: Definicion de la entidad.
        """
        for entity in self.entities:
            if entity["name"] == entity_name:
                return entity

        return None

    def get_entity_config_path(self, entity_name: str) -> str:
        """
        Obtiene la ruta absoluta del YAML de una entidad.

        La ruta relativa definida en el Registry se resuelve
        respecto al directorio donde se encuentra registry.yml.

        Args:
            entity_name: Nombre de la entidad.

        Returns:
            str: Ruta del YAML de la entidad.

        Raises:
            RegistryError: Si la entidad no existe o esta deshabilitada.
        """
        entity = self.get_entity(entity_name)

        if entity is None:
            raise RegistryError(
                f"La entidad '{entity_name}' no existe en el Registry."
            )

        if not entity["enabled"]:
            raise RegistryError(
                f"La entidad '{entity_name}' esta deshabilitada."
            )

        return os.path.normpath(
            os.path.join(
                self.registry_directory,
                entity["config"]
            )
        )