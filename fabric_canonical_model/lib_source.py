
"""
Lectura y acceso a fuentes Curated del modelo canonico.
"""

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from fabric_canonical_model.exceptions import SourceError
from fabric_canonical_model.lib_utils import get_spark


class SourceReader:
    """
    Administra la lectura de las fuentes Curated
    del modelo canonico desde un Lakehouse.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None
    ):
        """
        Inicializa el lector de fuentes.

        Args:
            spark: Sesion Spark opcional.
        """
        self.spark = spark or get_spark()

    @staticmethod
    def _validate_source_config(
        source_config: Dict[str, Any]
    ) -> None:
        """
        Valida la configuracion minima de una fuente.
        """

        if not isinstance(source_config, dict):
            raise SourceError(
                "La configuracion de la fuente debe ser un diccionario."
            )

        required_fields = ["name", "schema", "table"]

        missing_fields = [
            field
            for field in required_fields
            if field not in source_config
            or not isinstance(source_config[field], str)
            or not source_config[field].strip()
        ]

        if missing_fields:
            raise SourceError(
                "Faltan propiedades obligatorias en la fuente: "
                f"{', '.join(missing_fields)}"
            )

    @staticmethod
    def _build_table_name(
        source_config: Dict[str, Any]
    ) -> str:
        """
        Construye el nombre completamente cualificado de la tabla.
        """

        schema = source_config["schema"]
        table = source_config["table"]

        return f"`{schema}`.`{table}`"

    def read_table(
        self,
        source_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Lee una tabla Curated.

        Args:
            source_config: Configuracion de la fuente.

        Returns:
            DataFrame: Datos de la fuente.
        """

        self._validate_source_config(source_config)

        source_name = source_config["name"]
        table_name = self._build_table_name(source_config)

        try:
            return self.spark.read.table(table_name)

        except AnalysisException as exc:
            raise SourceError(
                f"No fue posible acceder a la tabla '{table_name}' "
                f"de la fuente '{source_name}'. "
                f"Detalle: {str(exc)}"
            ) from exc

        except Exception as exc:
            raise SourceError(
                f"Error inesperado leyendo la fuente '{source_name}'. "
                f"Tabla: '{table_name}'. "
                f"Detalle: {str(exc)}"
            ) from exc

    def read_query(
        self,
        query: str,
        source_name: str = "custom_query"
    ) -> DataFrame:
        """
        Ejecuta una consulta SQL en Spark.
        """

        if not isinstance(query, str) or not query.strip():
            raise SourceError(
                "La consulta SQL no puede estar vacia."
            )

        try:
            return self.spark.sql(query)

        except Exception as exc:
            raise SourceError(
                f"No fue posible ejecutar la consulta de "
                f"'{source_name}'. Detalle: {str(exc)}"
            ) from exc

    def get_table_columns(
        self,
        source_config: Dict[str, Any]
    ) -> List[str]:
        """
        Obtiene los nombres de las columnas de una fuente.
        """

        return self.read_table(source_config).columns

    def get_table_schema(
        self,
        source_config: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Obtiene los nombres y tipos de las columnas.
        """

        dataframe = self.read_table(source_config)

        return {
            field.name: field.dataType.simpleString()
            for field in dataframe.schema.fields
        }

    def get_table_metadata(
        self,
        source_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Obtiene metadatos basicos de una fuente.
        No ejecuta un count() sobre la tabla.
        """

        dataframe = self.read_table(source_config)

        return {
            "source": source_config["name"],
            "schema": source_config["schema"],
            "table": source_config["table"],
            "columns": dataframe.columns,
            "column_count": len(dataframe.columns)
        }

    def read_sources(
        self,
        sources_config: List[Dict[str, Any]]
    ) -> Dict[str, DataFrame]:
        """
        Lee multiples fuentes y devuelve sus DataFrames
        identificados por nombre.
        """

        if not isinstance(sources_config, list):
            raise SourceError(
                "La configuracion de fuentes debe ser una lista."
            )

        dataframes = {}

        for source_config in sources_config:

            self._validate_source_config(source_config)

            source_name = source_config["name"]

            if source_name in dataframes:
                raise SourceError(
                    f"Existe una fuente duplicada: '{source_name}'."
                )

            dataframes[source_name] = self.read_table(
                source_config
            )

        return dataframes