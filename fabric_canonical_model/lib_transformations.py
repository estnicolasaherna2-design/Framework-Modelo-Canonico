
"""
Motor de transformaciones para entidades canonicas.
"""

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_canonical_model.exceptions import CanonicalModelError


class TransformationEngine:
    """
    Ejecuta transformaciones sobre DataFrames mediante
    operaciones SQL y PySpark.
    """

    SUPPORTED_OPERATIONS = {
        "TRIM",
        "UPPER",
        "LOWER",
        "INITCAP",
        "CAST",
        "RENAME",
        "DROP",
        "FILL_NULL",
        "FILTER",
        "DERIVE_SQL"
    }

    def apply(
        self,
        dataframe: DataFrame,
        transformations: Optional[List[Dict[str, Any]]] = None
    ) -> DataFrame:
        """
        Ejecuta una lista ordenada de transformaciones.

        Args:
            dataframe: DataFrame de entrada.
            transformations: Lista de transformaciones.

        Returns:
            DataFrame: DataFrame transformado.
        """

        if not isinstance(dataframe, DataFrame):
            raise CanonicalModelError(
                "El objeto proporcionado no es un DataFrame valido."
            )

        if not transformations:
            return dataframe

        if not isinstance(transformations, list):
            raise CanonicalModelError(
                "Las transformaciones deben estar definidas como una lista."
            )

        result = dataframe

        for index, transformation in enumerate(transformations):

            if not isinstance(transformation, dict):
                raise CanonicalModelError(
                    f"La transformacion en la posicion {index} "
                    "debe ser un diccionario."
                )

            operation = transformation.get("operation")

            if operation not in self.SUPPORTED_OPERATIONS:
                raise CanonicalModelError(
                    f"Operacion no soportada: '{operation}'."
                )

            try:
                result = self._execute_operation(
                    result,
                    transformation
                )

            except Exception as exc:
                raise CanonicalModelError(
                    f"Error ejecutando la transformacion "
                    f"'{operation}' en la posicion {index}. "
                    f"Detalle: {str(exc)}"
                ) from exc

        return result

    def _execute_operation(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        """
        Ejecuta una operacion individual.
        """

        operation = transformation["operation"]

        if operation == "TRIM":
            return self._trim(dataframe, transformation)

        if operation == "UPPER":
            return self._upper(dataframe, transformation)

        if operation == "LOWER":
            return self._lower(dataframe, transformation)

        if operation == "INITCAP":
            return self._initcap(dataframe, transformation)

        if operation == "CAST":
            return self._cast(dataframe, transformation)

        if operation == "RENAME":
            return self._rename(dataframe, transformation)

        if operation == "DROP":
            return self._drop(dataframe, transformation)

        if operation == "FILL_NULL":
            return self._fill_null(dataframe, transformation)

        if operation == "FILTER":
            return self._filter(dataframe, transformation)

        if operation == "DERIVE_SQL":
            return self._derive_sql(dataframe, transformation)

        raise CanonicalModelError(
            f"Operacion no implementada: '{operation}'."
        )

    @staticmethod
    def _validate_column(
        dataframe: DataFrame,
        column: str
    ) -> None:
        """
        Valida que una columna exista.
        """

        if not isinstance(column, str) or column not in dataframe.columns:
            raise CanonicalModelError(
                f"La columna '{column}' no existe en el DataFrame."
            )

    def _trim(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.trim(F.col(column))
        )

    def _upper(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.upper(F.col(column))
        )

    def _lower(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.lower(F.col(column))
        )

    def _initcap(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.initcap(F.col(column))
        )

    def _cast(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]
        data_type = transformation["data_type"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.col(column).cast(data_type)
        )

    def _rename(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        source_column = transformation["source_column"]
        target_column = transformation["target_column"]

        self._validate_column(dataframe, source_column)

        if target_column in dataframe.columns:
            raise CanonicalModelError(
                f"La columna destino '{target_column}' ya existe."
            )

        return dataframe.withColumnRenamed(
            source_column,
            target_column
        )

    def _drop(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        columns = transformation["columns"]

        if not isinstance(columns, list):
            columns = [columns]

        missing_columns = [
            column
            for column in columns
            if column not in dataframe.columns
        ]

        if missing_columns:
            raise CanonicalModelError(
                "No existen las siguientes columnas para eliminar: "
                f"{', '.join(missing_columns)}"
            )

        return dataframe.drop(*columns)

    def _fill_null(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]
        value = transformation["value"]

        self._validate_column(dataframe, column)

        return dataframe.withColumn(
            column,
            F.coalesce(
                F.col(column),
                F.lit(value)
            )
        )

    def _filter(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        condition = transformation["condition"]

        if not isinstance(condition, str) or not condition.strip():
            raise CanonicalModelError(
                "La condicion del filtro SQL no puede estar vacia."
            )

        return dataframe.filter(
            F.expr(condition)
        )

    def _derive_sql(
        self,
        dataframe: DataFrame,
        transformation: Dict[str, Any]
    ) -> DataFrame:
        column = transformation["column"]
        expression = transformation["expression"]

        if not isinstance(column, str) or not column.strip():
            raise CanonicalModelError(
                "El nombre de la columna derivada no es valido."
            )

        if not isinstance(expression, str) or not expression.strip():
            raise CanonicalModelError(
                "La expresion SQL no puede estar vacia."
            )

        return dataframe.withColumn(
            column,
            F.expr(expression)
        )

    def apply_sql_query(
        self,
        query: str,
        views: Dict[str, DataFrame]
    ) -> DataFrame:
        """
        Ejecuta una consulta SQL sobre DataFrames registrados
        temporalmente como vistas.

        Args:
            query: Consulta SQL.
            views: Diccionario de vistas y DataFrames.

        Returns:
            DataFrame: Resultado de la consulta.
        """

        if not isinstance(query, str) or not query.strip():
            raise CanonicalModelError(
                "La consulta SQL no puede estar vacia."
            )

        if not isinstance(views, dict) or not views:
            raise CanonicalModelError(
                "Se debe proporcionar al menos una vista SQL."
            )

        registered_views = []

        try:
            for view_name, dataframe in views.items():

                if (
                    not isinstance(view_name, str)
                    or not view_name.isidentifier()
                ):
                    raise CanonicalModelError(
                        f"Nombre de vista SQL no valido: '{view_name}'."
                    )

                if not isinstance(dataframe, DataFrame):
                    raise CanonicalModelError(
                        f"La vista '{view_name}' no contiene "
                        "un DataFrame valido."
                    )

                dataframe.createOrReplaceTempView(view_name)
                registered_views.append(view_name)

            return next(iter(views.values())).sparkSession.sql(query)

        except Exception as exc:
            raise CanonicalModelError(
                f"Error ejecutando la consulta SQL. "
                f"Detalle: {str(exc)}"
            ) from exc

        finally:
            for view_name in registered_views:
                self._drop_temp_view(
                    next(iter(views.values())).sparkSession,
                    view_name
                )

    @staticmethod
    def _drop_temp_view(
        spark,
        view_name: str
    ) -> None:
        """
        Elimina una vista temporal registrada.
        """

        try:
            spark.catalog.dropTempView(view_name)

        except Exception:
            pass
