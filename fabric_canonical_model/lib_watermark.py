
"""
Administracion de watermarks para el procesamiento incremental
del modelo canonico.
"""

from datetime import datetime, timedelta
from typing import Optional, Union

from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

from fabric_canonical_model.exceptions import WatermarkError
from fabric_canonical_model.lib_utils import get_spark


class WatermarkManager:
    """
    Administra los watermarks de cada entidad y fuente.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        table_name: str = "canonical_metadata.entity_source_watermarks"
    ):
        """
        Inicializa el administrador de watermarks.

        Args:
            spark: Sesion Spark opcional.
            table_name: Tabla Delta de control.
        """
        self.spark = spark or get_spark()
        self.table_name = table_name

        self._ensure_table()

    def _ensure_table(self) -> None:
        """
        Crea el schema y la tabla de control si no existen.
        """

        try:
            self.spark.sql(
                "CREATE SCHEMA IF NOT EXISTS canonical_metadata"
            )

            schema = StructType([
                StructField("entity_name", StringType(), False),
                StructField("source_name", StringType(), False),
                StructField("watermark_value", TimestampType(), True),
                StructField("updated_at", TimestampType(), True)
            ])

            empty_dataframe = self.spark.createDataFrame(
                [],
                schema
            )

            (
                empty_dataframe.write
                .format("delta")
                .mode("ignore")
                .saveAsTable(self.table_name)
            )

        except Exception as exc:
            raise WatermarkError(
                f"No fue posible inicializar la tabla de watermarks "
                f"'{self.table_name}'. Detalle: {str(exc)}"
            ) from exc

    @staticmethod
    def _validate_identifiers(
        entity_name: str,
        source_name: str
    ) -> None:
        """
        Valida los identificadores de entidad y fuente.
        """

        if not isinstance(entity_name, str) or not entity_name.strip():
            raise WatermarkError(
                "El nombre de la entidad no es valido."
            )

        if not isinstance(source_name, str) or not source_name.strip():
            raise WatermarkError(
                "El nombre de la fuente no es valido."
            )

    @staticmethod
    def _normalize_watermark(
        value: Optional[Union[datetime, str]]
    ) -> Optional[datetime]:
        """
        Convierte un watermark de texto a datetime.
        """

        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            try:
                return datetime.fromisoformat(
                    value.replace("Z", "+00:00")
                )

            except ValueError as exc:
                raise WatermarkError(
                    f"El watermark '{value}' no tiene un formato valido."
                ) from exc

        raise WatermarkError(
            "El watermark debe ser datetime, texto ISO 8601 o None."
        )

    def get_watermark(
        self,
        entity_name: str,
        source_name: str,
        initial_watermark: Optional[Union[datetime, str]] = None
    ) -> Optional[datetime]:
        """
        Consulta el ultimo watermark confirmado.

        Si no existe un registro, devuelve el watermark inicial
        proporcionado. Si tampoco existe, devuelve None.
        """

        self._validate_identifiers(entity_name, source_name)

        initial_value = self._normalize_watermark(
            initial_watermark
        )

        try:
            result = (
                self.spark.table(self.table_name)
                .filter(
                    (F.col("entity_name") == entity_name)
                    & (F.col("source_name") == source_name)
                )
                .select("watermark_value")
                .first()
            )

            if result is None:
                return initial_value

            return result["watermark_value"]

        except Exception as exc:
            raise WatermarkError(
                f"No fue posible consultar el watermark de "
                f"'{entity_name}' para la fuente '{source_name}'. "
                f"Detalle: {str(exc)}"
            ) from exc

    def get_changed_records(
        self,
        dataframe: DataFrame,
        entity_name: str,
        source_name: str,
        watermark_column: str,
        initial_watermark: Optional[Union[datetime, str]] = None,
        safety_lookback_minutes: int = 0
    ) -> DataFrame:
        """
        Filtra los registros modificados desde el ultimo watermark.

        Si no existe watermark confirmado ni inicial, devuelve
        el DataFrame completo.

        El lookback permite releer una ventana anterior para
        cubrir retrasos en la disponibilidad de los datos.
        """

        self._validate_identifiers(entity_name, source_name)

        if not isinstance(dataframe, DataFrame):
            raise WatermarkError(
                "El objeto proporcionado no es un DataFrame."
            )

        if watermark_column not in dataframe.columns:
            raise WatermarkError(
                f"La columna watermark '{watermark_column}' "
                "no existe en el DataFrame."
            )

        if (
            not isinstance(safety_lookback_minutes, int)
            or isinstance(safety_lookback_minutes, bool)
            or safety_lookback_minutes < 0
        ):
            raise WatermarkError(
                "safety_lookback_minutes debe ser un entero mayor o igual a cero."
            )

        watermark = self.get_watermark(
            entity_name,
            source_name,
            initial_watermark
        )

        if watermark is None:
            return dataframe

        cutoff = watermark - timedelta(
            minutes=safety_lookback_minutes
        )

        return dataframe.filter(
            F.col(watermark_column) >= F.lit(cutoff)
        )

    def get_candidate_watermark(
        self,
        dataframe: DataFrame,
        watermark_column: str
    ) -> Optional[datetime]:
        """
        Calcula el maximo valor de la columna watermark
        disponible en el DataFrame.

        Este valor es candidato y todavia no queda confirmado.
        """

        if watermark_column not in dataframe.columns:
            raise WatermarkError(
                f"La columna watermark '{watermark_column}' "
                "no existe en el DataFrame."
            )

        try:
            result = (
                dataframe
                .agg(F.max(F.col(watermark_column)).alias("candidate"))
                .first()
            )

            return result["candidate"]

        except Exception as exc:
            raise WatermarkError(
                f"No fue posible calcular el watermark candidato. "
                f"Detalle: {str(exc)}"
            ) from exc

    def commit_watermark(
        self,
        entity_name: str,
        source_name: str,
        candidate_watermark: Union[datetime, str]
    ) -> None:
        """
        Confirma el watermark despues de una persistencia exitosa.

        La confirmacion utiliza MERGE para actualizar el registro
        existente o insertar uno nuevo.
        """

        self._validate_identifiers(entity_name, source_name)

        candidate = self._normalize_watermark(
            candidate_watermark
        )

        if candidate is None:
            raise WatermarkError(
                "No se puede confirmar un watermark nulo."
            )

        try:
            source_dataframe = self.spark.createDataFrame(
                [
                    (
                        entity_name,
                        source_name,
                        candidate
                    )
                ],
                schema=StructType([
                    StructField("entity_name", StringType(), False),
                    StructField("source_name", StringType(), False),
                    StructField("watermark_value", TimestampType(), False)
                ])
            ).withColumn(
                "updated_at",
                F.current_timestamp()
            )

            target = DeltaTable.forName(
                self.spark,
                self.table_name
            )

            (
                target.alias("target")
                .merge(
                    source_dataframe.alias("source"),
                    """
                    target.entity_name = source.entity_name
                    AND target.source_name = source.source_name
                    """
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        except Exception as exc:
            raise WatermarkError(
                f"No fue posible confirmar el watermark de "
                f"'{entity_name}' para '{source_name}'. "
                f"Detalle: {str(exc)}"
            ) from exc