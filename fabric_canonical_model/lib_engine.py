
"""
Motor principal del Fabric Canonical Model Framework.
"""

from typing import Any, Callable, Dict, List, Optional

from fabric_canonical_model.exceptions import (
    ExecutionError,
    RegistryError
)


class CanonicalModelEngine:
    """
    Motor principal para ejecutar entidades canonicas.

    Responsabilidades:
        - Consultar el Registry.
        - Identificar entidades habilitadas.
        - Coordinar la ejecucion de entidades.
        - Consolidar resultados.
    """

    SUPPORTED_MODES = {
        "FULL_LOAD",
        "UPSERT",
        "SCD2"
    }

    def __init__(
        self,
        registry,
        execution_manager
    ):
        self.registry = registry
        self.execution_manager = execution_manager

        self._validate_dependencies()

    def _validate_dependencies(self) -> None:
        """
        Verifica que los componentes requeridos esten disponibles.
        """

        if self.registry is None:
            raise RegistryError(
                "Se debe proporcionar una instancia del Registry."
            )

        if not callable(
            getattr(self.registry, "get_enabled_entities", None)
        ):
            raise RegistryError(
                "El Registry no implementa get_enabled_entities()."
            )

        if self.execution_manager is None:
            raise ExecutionError(
                "Se debe proporcionar un administrador de ejecuciones."
            )

        if not callable(
            getattr(self.execution_manager, "execute_entity", None)
        ):
            raise ExecutionError(
                "El administrador no implementa execute_entity()."
            )

    def get_enabled_entities(self) -> List[Dict[str, Any]]:
        """
        Obtiene las entidades habilitadas en el Registry.
        """

        try:
            entities = self.registry.get_enabled_entities()

        except Exception as exc:
            raise RegistryError(
                "No fue posible obtener las entidades habilitadas. "
                f"Detalle: {str(exc)}"
            ) from exc

        if not isinstance(entities, list):
            raise RegistryError(
                "El Registry debe devolver una lista de entidades."
            )

        for entity in entities:

            if not isinstance(entity, dict):
                raise RegistryError(
                    "Cada entidad del Registry debe ser un diccionario."
                )

            entity_name = entity.get("name")

            if not isinstance(entity_name, str) or not entity_name.strip():
                raise RegistryError(
                    "Existe una entidad habilitada sin nombre valido."
                )

        entity_names = [
            entity["name"]
            for entity in entities
        ]

        if len(entity_names) != len(set(entity_names)):
            raise RegistryError(
                "El Registry contiene nombres de entidades duplicados."
            )

        return entities

    def execute_entity(
        self,
        entity_name: str,
        execution_mode: str,
        source_names: List[str],
        process_function: Callable,
        process_name: str = "nb_create_canonical_model"
    ) -> Dict[str, Any]:
        """
        Ejecuta una entidad canonica especifica.

        La entidad debe estar habilitada en el Registry.
        """

        enabled_entities = self.get_enabled_entities()

        enabled_names = {
            entity["name"]
            for entity in enabled_entities
        }

        if entity_name not in enabled_names:
            raise ExecutionError(
                f"La entidad '{entity_name}' no esta habilitada "
                "en el Registry."
            )

        return self.execution_manager.execute_entity(
            entity_name=entity_name,
            execution_mode=execution_mode,
            source_names=source_names,
            process_function=process_function,
            process_name=process_name
        )

    def execute_enabled_entities(
        self,
        entity_runtime: Dict[str, Dict[str, Any]],
        stop_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Ejecuta todas las entidades habilitadas en el Registry.

        Args:
            entity_runtime:
                Configuracion de ejecucion por entidad.

            stop_on_error:
                Si es True, detiene el proceso ante el primer error.

        Cada entidad debe proporcionar:
            execution_mode
            source_names
            process_function

        Returns:
            Resultado consolidado de las ejecuciones.
        """

        if not isinstance(entity_runtime, dict):
            raise ExecutionError(
                "entity_runtime debe ser un diccionario."
            )

        enabled_entities = self.get_enabled_entities()

        if not enabled_entities:
            return {
                "total_entities": 0,
                "successful_entities": 0,
                "failed_entities": 0,
                "results": []
            }

        results = []

        for entity in enabled_entities:

            entity_name = entity["name"]

            runtime_config = entity_runtime.get(entity_name)

            if not isinstance(runtime_config, dict):

                error_message = (
                    f"No existe configuracion de ejecucion "
                    f"para la entidad '{entity_name}'."
                )

                results.append({
                    "entity_name": entity_name,
                    "execution_status": "FAILED",
                    "error_message": error_message
                })

                if stop_on_error:
                    break

                continue

            try:

                execution_mode = runtime_config.get(
                    "execution_mode",
                    "UPSERT"
                )

                source_names = runtime_config.get(
                    "source_names"
                )

                process_function = runtime_config.get(
                    "process_function"
                )

                process_name = runtime_config.get(
                    "process_name",
                    "nb_create_canonical_model"
                )

                if execution_mode not in self.SUPPORTED_MODES:
                    raise ExecutionError(
                        f"Modo no soportado para '{entity_name}': "
                        f"'{execution_mode}'."
                    )

                result = self.execute_entity(
                    entity_name=entity_name,
                    execution_mode=execution_mode,
                    source_names=source_names,
                    process_function=process_function,
                    process_name=process_name
                )

                results.append(result)

            except Exception as exc:

                results.append({
                    "entity_name": entity_name,
                    "execution_status": "FAILED",
                    "error_message": str(exc)
                })

                if stop_on_error:
                    break

        successful_entities = sum(
            1
            for result in results
            if result.get("execution_status") == "SUCCESS"
        )

        failed_entities = sum(
            1
            for result in results
            if result.get("execution_status") == "FAILED"
        )

        return {
            "total_entities": len(enabled_entities),
            "executed_entities": len(results),
            "successful_entities": successful_entities,
            "failed_entities": failed_entities,
            "results": results
        }
