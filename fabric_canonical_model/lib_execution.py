
"""
Coordinacion de ejecuciones de entidades canonicas.
"""

from typing import Any, Callable, Dict, List, Optional

from fabric_canonical_model.exceptions import ExecutionError


class EntityExecutionManager:
    """
    Coordina la ejecucion de entidades canonicas.

    Responsabilidades:
        - Iniciar auditoria.
        - Ejecutar el proceso de construccion y persistencia.
        - Confirmar watermarks despues de persistir.
        - Finalizar auditoria.
        - Registrar errores de ejecucion.
    """

    def __init__(
        self,
        audit_manager,
        watermark_manager
    ):
        self.audit = audit_manager
        self.watermark = watermark_manager

    def execute_entity(
        self,
        entity_name: str,
        execution_mode: str,
        source_names: List[str],
        process_function: Callable,
        process_name: str = "nb_create_canonical_model"
    ) -> Dict[str, Any]:
        """
        Ejecuta el ciclo completo de una entidad.

        Args:
            entity_name: Nombre de la entidad canonica.
            execution_mode: FULL_LOAD, UPSERT o SCD2.
            source_names: Fuentes participantes.
            process_function: Funcion que construye y persiste
                la entidad. Recibe execution_id como argumento.
            process_name: Nombre del proceso ejecutor.

        Returns:
            Dict con identificador, estado y metricas.

        La funcion process_function debe devolver:

            {
                "metrics": {
                    "records_read": 100,
                    "records_inserted": 10,
                    "records_updated": 20,
                    "records_rejected": 0
                },
                "candidate_watermarks": {
                    "AVM": "2026-11-23 10:00:00",
                    "BDP": "2026-11-23 10:15:00"
                }
            }
        """

        self._validate_execution_parameters(
            entity_name=entity_name,
            execution_mode=execution_mode,
            source_names=source_names,
            process_function=process_function
        )

        execution_id = self.audit.start_execution(
            entity_name=entity_name,
            execution_mode=execution_mode,
            source_names=source_names,
            process_name=process_name
        )

        try:
            # 1. Ejecutar construccion y persistencia.
            process_result = process_function(execution_id)

            if not isinstance(process_result, dict):
                raise ExecutionError(
                    "El proceso de construccion debe devolver "
                    "un diccionario de resultados."
                )

            metrics = process_result.get("metrics", {})
            candidate_watermarks = process_result.get(
                "candidate_watermarks",
                {}
            )

            if not isinstance(metrics, dict):
                raise ExecutionError(
                    "Las metricas de ejecucion deben ser un diccionario."
                )

            if not isinstance(candidate_watermarks, dict):
                raise ExecutionError(
                    "Los candidate_watermarks deben ser un diccionario."
                )

            # 2. Validar que los watermarks correspondan
            # a las fuentes participantes.
            unknown_sources = [
                source
                for source in candidate_watermarks
                if source not in source_names
            ]

            if unknown_sources:
                raise ExecutionError(
                    "Se recibieron watermarks para fuentes "
                    "no participantes: "
                    f"{', '.join(unknown_sources)}"
                )

            # 3. Confirmar watermarks despues de persistir.
            for source_name, candidate_watermark in (
                candidate_watermarks.items()
            ):

                if candidate_watermark is None:
                    continue

                self.watermark.commit_watermark(
                    entity_name=entity_name,
                    source_name=source_name,
                    candidate_watermark=candidate_watermark
                )

            # 4. Finalizar auditoria exitosamente.
            self.audit.finish_execution(
                execution_id=execution_id,
                execution_status="SUCCESS",
                metrics=metrics
            )

            return {
                "execution_id": execution_id,
                "entity_name": entity_name,
                "execution_status": "SUCCESS",
                "metrics": metrics,
                "candidate_watermarks": candidate_watermarks
            }

        except Exception as exc:

            error_message = str(exc)

            try:
                self.audit.finish_execution(
                    execution_id=execution_id,
                    execution_status="FAILED",
                    metrics={
                        "records_read": 0,
                        "records_inserted": 0,
                        "records_updated": 0,
                        "records_rejected": 0
                    },
                    error_message=error_message
                )

            except Exception as audit_exc:
                raise ExecutionError(
                    f"Fallo la ejecucion de '{entity_name}'. "
                    f"Error original: {error_message}. "
                    f"Adicionalmente fallo la auditoria: {str(audit_exc)}"
                ) from exc

            raise ExecutionError(
                f"Fallo la ejecucion de la entidad '{entity_name}'. "
                f"Execution ID: {execution_id}. "
                f"Detalle: {error_message}"
            ) from exc

    @staticmethod
    def _validate_execution_parameters(
        entity_name: str,
        execution_mode: str,
        source_names: List[str],
        process_function: Callable
    ) -> None:
        """
        Valida los parametros de ejecucion.
        """

        if not isinstance(entity_name, str) or not entity_name.strip():
            raise ExecutionError(
                "El nombre de la entidad no es valido."
            )

        supported_modes = [
            "FULL_LOAD",
            "UPSERT",
            "SCD2"
        ]

        if execution_mode not in supported_modes:
            raise ExecutionError(
                f"Modo de ejecucion no soportado: '{execution_mode}'."
            )

        if not isinstance(source_names, list) or not source_names:
            raise ExecutionError(
                "Se debe proporcionar al menos una fuente."
            )

        if len(source_names) != len(set(source_names)):
            raise ExecutionError(
                "La lista de fuentes contiene nombres duplicados."
            )

        if not callable(process_function):
            raise ExecutionError(
                "process_function debe ser una funcion ejecutable."
            )
