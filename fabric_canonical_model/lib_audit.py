
"""
Auditoria y trazabilidad de ejecuciones del framework canonico.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType
)

from delta.tables import DeltaTable

from fabric_canonical_model.exceptions import PersistenceError
from fabric_canonical_model.lib_utils import get_spark


class ExecutionAudit:
    """
    Administra la auditoria de ejecuciones del framework.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        schema_name: str = "canonical_metadata",
        table_name: str = "execution_audit"
    ):
        self.spark = spark or get_spark()

        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{schema_name}.{table_name}"

        self._ensure_table()

    def _ensure_table(self) -> None:
        """
        Crea el schema y la tabla de auditoria si no existen.
        """

        try:
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS `{self.schema_name}`"
            )

            schema = StructType([
                StructField("execution_id", StringType(), False),
                StructField("entity_name", StringType(), True),
                StructField("execution_mode", StringType(), True),
                StructField("execution_status", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("records_read", LongType(), True),
                StructField("records_inserted", LongType(), True),
                StructField("records_updated", LongType(), True),
                StructField("records_rejected", LongType(), True),
                StructField("source_names", StringType(), True),
                StructField("process_name", StringType(), True),
                StructField("error_message", StringType(), True)
            ])

            if not self.spark.catalog.tableExists(self.full_table_name):

                empty_dataframe = self.spark.createDataFrame(
                    [],
                    schema
                )

                empty_dataframe.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(self.full_table_name)

        except Exception as exc:
            raise PersistenceError(
                "No fue posible inicializar la tabla de auditoria. "
                f"Detalle: {str(exc)}"
            ) from exc

    @staticmethod
    def _current_timestamp() -> datetime:
        """
        Obtiene la fecha y hora UTC actual.
        """

        return datetime.now(timezone.utc).replace(tzinfo=None)

    def start_execution(
        self,
        entity_name: str,
        execution_mode: str,
        source_names: Optional[list] = None,
        process_name: str = "nb_create_canonical_model"
    ) -> str:
        """
        Registra el inicio de una ejecucion.

        Returns:
            str: Identificador de ejecucion.
        """

        execution_id = str(uuid.uuid4())
        start_time = self._current_timestamp()

        record = [(
            execution_id,
            entity_name,
            execution_mode,
            "RUNNING",
            start_time,
            None,
            None,
            0,
            0,
            0,
            0,
            ",".join(source_names or []),
            process_name,
            None
        )]

        try:
            dataframe = self.spark.createDataFrame(
                record,
                self._get_schema()
            )

            dataframe.write.format("delta") \
                .mode("append") \
                .saveAsTable(self.full_table_name)

            return execution_id

        except Exception as exc:
            raise PersistenceError(
                "No fue posible registrar el inicio de la ejecucion. "
                f"Detalle: {str(exc)}"
            ) from exc

    def finish_execution(
        self,
        execution_id: str,
        execution_status: str,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """
        Actualiza el registro de auditoria al finalizar.

        execution_status:
            SUCCESS
            FAILED
        """

        if execution_status not in ["SUCCESS", "FAILED"]:
            raise PersistenceError(
                "El estado final debe ser SUCCESS o FAILED."
            )

        metrics = metrics or {}

        end_time = self._current_timestamp()

        try:
            delta_table = DeltaTable.forName(
                self.spark,
                self.full_table_name
            )

            execution = (
                self.spark.table(self.full_table_name)
                .filter(
                    F.col("execution_id") == execution_id
                )
                .select("start_time")
                .first()
            )

            if execution is None:
                raise PersistenceError(
                    f"No existe la ejecucion '{execution_id}'."
                )

            start_time = execution["start_time"]

            duration = (
                end_time - start_time
            ).total_seconds()

            update_values = {
                "execution_status": F.lit(execution_status),
                "end_time": F.lit(end_time).cast("timestamp"),
                "duration_seconds": F.lit(duration),
                "records_read": F.lit(
                    int(metrics.get("records_read", 0))
                ),
                "records_inserted": F.lit(
                    int(metrics.get("records_inserted", 0))
                ),
                "records_updated": F.lit(
                    int(metrics.get("records_updated", 0))
                ),
                "records_rejected": F.lit(
                    int(metrics.get("records_rejected", 0))
                ),
                "error_message": F.lit(error_message)
            }

            delta_table.update(
                condition=F.col("execution_id") == execution_id,
                set=update_values
            )

        except Exception as exc:
            raise PersistenceError(
                f"No fue posible finalizar la auditoria "
                f"'{execution_id}'. Detalle: {str(exc)}"
            ) from exc

    def get_execution(self, execution_id: str):
        """
        Consulta una ejecucion por su identificador.
        """

        return (
            self.spark.table(self.full_table_name)
            .filter(
                F.col("execution_id") == execution_id
            )
        )

    def get_entity_history(
        self,
        entity_name: str,
        limit: int = 100
    ):
        """
        Consulta el historial de ejecuciones de una entidad.
        """

        if limit < 1:
            raise PersistenceError(
                "El limite de registros debe ser mayor que cero."
            )

        return (
            self.spark.table(self.full_table_name)
            .filter(
                F.col("entity_name") == entity_name
            )
            .orderBy(
                F.col("start_time").desc()
            )
            .limit(limit)
        )

    def _get_schema(self) -> StructType:
        """
        Devuelve el esquema de la tabla de auditoria.
        """

        return StructType([
            StructField("execution_id", StringType(), False),
            StructField("entity_name", StringType(), True),
            StructField("execution_mode", StringType(), True),
            StructField("execution_status", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("records_read", LongType(), True),
            StructField("records_inserted", LongType(), True),
            StructField("records_updated", LongType(), True),
            StructField("records_rejected", LongType(), True),
            StructField("source_names", StringType(), True),
            StructField("process_name", StringType(), True),
            StructField("error_message", StringType(), True)
        ])
