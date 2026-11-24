
"""
Validaciones de calidad e integridad para entidades canonicas.
"""

from functools import reduce
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_canonical_model.exceptions import ValidationError


class DataValidator:
    """
    Ejecuta validaciones sobre fuentes y entidades canonicas.
    """

    # ---------------------------------------------------------
    # VALIDACIONES GENERALES
    # ---------------------------------------------------------

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        """
        Verifica que el objeto recibido sea un DataFrame valido.
        """

        if not isinstance(dataframe, DataFrame):
            raise ValidationError(
                "El objeto recibido no es un DataFrame valido."
            )

        normalized_columns = [
            column.lower()
            for column in dataframe.columns
        ]

        if len(normalized_columns) != len(set(normalized_columns)):
            raise ValidationError(
                "El DataFrame contiene columnas duplicadas "
                "o ambiguas."
            )

    @staticmethod
    def _validate_column_list(
        columns: List[str],
        parameter_name: str,
        allow_empty: bool = False
    ) -> None:
        """
        Valida una lista de nombres de columnas.
        """

        if not isinstance(columns, list):
            raise ValidationError(
                f"'{parameter_name}' debe ser una lista."
            )

        if not columns and not allow_empty:
            raise ValidationError(
                f"'{parameter_name}' no puede estar vacia."
            )

        if any(
            not isinstance(column, str) or not column.strip()
            for column in columns
        ):
            raise ValidationError(
                f"'{parameter_name}' contiene nombres invalidos."
            )

        normalized_columns = [
            column.lower()
            for column in columns
        ]

        if len(normalized_columns) != len(set(normalized_columns)):
            raise ValidationError(
                f"'{parameter_name}' contiene columnas duplicadas."
            )

    @staticmethod
    def _column(column_name: str):
        """
        Construye una referencia segura a una columna Spark.
        """

        escaped_name = column_name.replace("`", "``")

        return F.col(f"`{escaped_name}`")

    @staticmethod
    def _validate_existing_columns(
        dataframe: DataFrame,
        columns: List[str]
    ) -> None:
        """
        Comprueba que todas las columnas existan en el DataFrame.
        """

        missing_columns = [
            column
            for column in columns
            if column not in dataframe.columns
        ]

        if missing_columns:
            raise ValidationError(
                "No existen las siguientes columnas: "
                f"{', '.join(missing_columns)}"
            )

    # ---------------------------------------------------------
    # COLUMNAS OBLIGATORIAS
    # ---------------------------------------------------------

    def validate_required_columns(
        self,
        dataframe: DataFrame,
        required_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Valida la existencia de columnas obligatorias.
        """

        self._validate_dataframe(dataframe)

        self._validate_column_list(
            required_columns,
            "required_columns",
            allow_empty=True
        )

        missing_columns = [
            column
            for column in required_columns
            if column not in dataframe.columns
        ]

        return {
            "validation": "required_columns",
            "passed": len(missing_columns) == 0,
            "missing_columns": missing_columns
        }

    # ---------------------------------------------------------
    # VALORES NULOS
    # ---------------------------------------------------------

    def validate_not_null(
        self,
        dataframe: DataFrame,
        columns: List[str],
        treat_empty_strings: bool = True
    ) -> Dict[str, Any]:
        """
        Identifica valores nulos en columnas obligatorias.

        Cuando treat_empty_strings=True, tambien considera
        invalidas las cadenas vacias o con espacios.
        """

        self._validate_dataframe(dataframe)

        self._validate_column_list(
            columns,
            "columns"
        )

        self._validate_existing_columns(
            dataframe,
            columns
        )

        null_counts = {}

        for column in columns:

            expression = self._column(column).isNull()

            if treat_empty_strings:

                expression = (
                    expression
                    | (
                        F.trim(
                            self._column(column).cast("string")
                        ) == ""
                    )
                )

            count = (
                dataframe
                .filter(expression)
                .count()
            )

            null_counts[column] = count

        return {
            "validation": "not_null",
            "passed": all(
                value == 0
                for value in null_counts.values()
            ),
            "null_counts": null_counts,
            "invalid_records_by_column": null_counts
        }

    # ---------------------------------------------------------
    # CLAVES DE NEGOCIO
    # ---------------------------------------------------------

    def validate_business_keys(
        self,
        dataframe: DataFrame,
        business_keys: List[str]
    ) -> Dict[str, Any]:
        """
        Valida que las claves de negocio no sean nulas
        ni contengan valores vacios.
        """

        self._validate_dataframe(dataframe)

        self._validate_column_list(
            business_keys,
            "business_keys"
        )

        self._validate_existing_columns(
            dataframe,
            business_keys
        )

        invalid_conditions = []

        for column in business_keys:

            invalid_conditions.append(
                self._column(column).isNull()
                | (
                    F.trim(
                        self._column(column).cast("string")
                    ) == ""
                )
            )

        invalid_condition = reduce(
            lambda left, right: left | right,
            invalid_conditions
        )

        invalid_count = (
            dataframe
            .filter(invalid_condition)
            .count()
        )

        total_count = dataframe.count()

        return {
            "validation": "business_keys",
            "passed": invalid_count == 0,
            "total_records": total_count,
            "invalid_records": invalid_count,
            "valid_records": total_count - invalid_count,
            "business_keys": business_keys
        }

    # ---------------------------------------------------------
    # DUPLICADOS
    # ---------------------------------------------------------

    def validate_duplicates(
        self,
        dataframe: DataFrame,
        business_keys: List[str]
    ) -> Dict[str, Any]:
        """
        Detecta claves de negocio duplicadas.

        Devuelve:
            duplicate_groups:
                Cantidad de combinaciones de claves duplicadas.

            duplicate_records:
                Total de registros pertenecientes a grupos duplicados.

            excess_duplicate_records:
                Cantidad de registros adicionales que exceden
                una fila por clave.
        """

        self._validate_dataframe(dataframe)

        self._validate_column_list(
            business_keys,
            "business_keys"
        )

        self._validate_existing_columns(
            dataframe,
            business_keys
        )

        duplicate_groups = (
            dataframe
            .groupBy(
                *[
                    self._column(column)
                    for column in business_keys
                ]
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_metrics = duplicate_groups.agg(
            F.count("*").alias("duplicate_groups"),
            F.coalesce(
                F.sum("count"),
                F.lit(0)
            ).alias("duplicate_records"),
            F.coalesce(
                F.sum(F.col("count") - 1),
                F.lit(0)
            ).alias("excess_duplicate_records")
        ).first()

        group_count = int(
            duplicate_metrics["duplicate_groups"]
        )

        record_count = int(
            duplicate_metrics["duplicate_records"]
        )

        excess_count = int(
            duplicate_metrics["excess_duplicate_records"]
        )

        return {
            "validation": "duplicates",
            "passed": group_count == 0,
            "business_keys": business_keys,
            "duplicate_groups": group_count,
            "duplicate_records": record_count,
            "excess_duplicate_records": excess_count
        }

    # ---------------------------------------------------------
    # CONSISTENCIA ENTRE FUENTES
    # ---------------------------------------------------------

    def validate_consistency(
        self,
        dataframes: Dict[str, DataFrame],
        business_key: str,
        attribute: str
    ) -> Dict[str, Any]:
        """
        Identifica diferencias de un atributo entre fuentes
        para una misma clave de negocio.

        Las fuentes deben tener previamente los mismos
        nombres de columnas.

        Los valores nulos no participan en la comparacion.
        """

        if (
            not isinstance(dataframes, dict)
            or len(dataframes) < 2
        ):
            raise ValidationError(
                "Se requieren al menos dos fuentes "
                "para validar consistencia."
            )

        if (
            not isinstance(business_key, str)
            or not business_key.strip()
        ):
            raise ValidationError(
                "Se debe proporcionar una clave de negocio valida."
            )

        if (
            not isinstance(attribute, str)
            or not attribute.strip()
        ):
            raise ValidationError(
                "Se debe proporcionar un atributo valido."
            )

        records = []

        for source_name, dataframe in dataframes.items():

            if (
                not isinstance(source_name, str)
                or not source_name.strip()
            ):
                raise ValidationError(
                    "Existe una fuente con nombre invalido."
                )

            self._validate_dataframe(dataframe)

            required_columns = [
                business_key,
                attribute
            ]

            self._validate_existing_columns(
                dataframe,
                required_columns
            )

            duplicate_result = self.validate_duplicates(
                dataframe,
                [business_key]
            )

            if not duplicate_result["passed"]:
                raise ValidationError(
                    f"La fuente '{source_name}' contiene "
                    "claves de negocio duplicadas. "
                    "No es posible validar la consistencia "
                    "de forma no ambigua."
                )

            source_records = dataframe.select(
                self._column(business_key),
                self._column(attribute).cast(
                    "string"
                ).alias("attribute_value"),
                F.lit(source_name).alias("source_name")
            )

            records.append(source_records)

        combined = reduce(
            lambda left, right: left.unionByName(right),
            records
        )

        conflicts = (
            combined
            .filter(
                F.col(business_key).isNotNull()
                & F.col("attribute_value").isNotNull()
            )
            .groupBy(business_key)
            .agg(
                F.countDistinct(
                    "attribute_value"
                ).alias("distinct_values"),

                F.collect_set(
                    F.struct(
                        "source_name",
                        "attribute_value"
                    )
                ).alias("source_values")
            )
            .filter(
                F.col("distinct_values") > 1
            )
        )

        conflict_count = conflicts.count()

        return {
            "validation": "consistency",
            "passed": conflict_count == 0,
            "business_key": business_key,
            "attribute": attribute,
            "conflict_count": conflict_count,
            "conflicts": conflicts
        }

    # ---------------------------------------------------------
    # REGLAS PERSONALIZADAS
    # ---------------------------------------------------------

    def validate_custom_rules(
        self,
        dataframe: DataFrame,
        rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta reglas de validacion mediante expresiones SQL.

        Cada regla debe contener:
            name
            condition

        La condicion debe devolver TRUE para registros validos.
        """

        self._validate_dataframe(dataframe)

        if not isinstance(rules, list):
            raise ValidationError(
                "La configuracion de reglas debe ser una lista."
            )

        results = []

        rule_names = set()

        for rule in rules:

            if not isinstance(rule, dict):
                raise ValidationError(
                    "Cada regla personalizada debe ser un diccionario."
                )

            rule_name = rule.get("name")
            condition = rule.get("condition")

            if (
                not isinstance(rule_name, str)
                or not rule_name.strip()
            ):
                raise ValidationError(
                    "Cada regla debe contener un nombre valido."
                )

            if (
                not isinstance(condition, str)
                or not condition.strip()
            ):
                raise ValidationError(
                    f"La regla '{rule_name}' debe contener "
                    "una condicion SQL valida."
                )

            if rule_name in rule_names:
                raise ValidationError(
                    f"El nombre de regla '{rule_name}' esta duplicado."
                )

            rule_names.add(rule_name)

            try:

                invalid_condition = ~F.coalesce(
                    F.expr(condition),
                    F.lit(False)
                )

                invalid_records = (
                    dataframe
                    .filter(invalid_condition)
                    .count()
                )

                results.append({
                    "validation": "custom_rule",
                    "rule": rule_name,
                    "passed": invalid_records == 0,
                    "invalid_records": invalid_records
                })

            except Exception as exc:

                raise ValidationError(
                    f"Error ejecutando la regla '{rule_name}'. "
                    f"Condicion: '{condition}'. "
                    f"Detalle: {str(exc)}"
                ) from exc

        return results

    # ---------------------------------------------------------
    # VALIDACION INTEGRAL
    # ---------------------------------------------------------

    def validate_dataframe(
        self,
        dataframe: DataFrame,
        business_keys: List[str],
        required_columns: Optional[List[str]] = None,
        check_duplicates: bool = True
    ) -> Dict[str, Any]:
        """
        Ejecuta un conjunto de validaciones generales.

        Comprueba:
            1. Existencia de columnas obligatorias.
            2. Valores nulos en columnas obligatorias.
            3. Integridad de claves de negocio.
            4. Duplicados de claves de negocio.

        Si faltan columnas, no ejecuta validaciones posteriores
        que dependan de ellas.
        """

        self._validate_dataframe(dataframe)

        self._validate_column_list(
            business_keys,
            "business_keys"
        )

        required_columns = required_columns or []

        self._validate_column_list(
            required_columns,
            "required_columns",
            allow_empty=True
        )

        if not isinstance(check_duplicates, bool):
            raise ValidationError(
                "check_duplicates debe ser un valor booleano."
            )

        columns_to_check = list(
            dict.fromkeys(
                required_columns + business_keys
            )
        )

        results = []

        required_result = self.validate_required_columns(
            dataframe,
            columns_to_check
        )

        results.append(required_result)

        missing_columns = required_result["missing_columns"]

        if missing_columns:
            return {
                "passed": False,
                "results": results,
                "missing_columns": missing_columns
            }

        # Las claves de negocio tienen su propia validacion,
        # que tambien detecta cadenas vacias.
        non_key_required_columns = [
            column
            for column in required_columns
            if column not in business_keys
        ]

        if non_key_required_columns:

            results.append(
                self.validate_not_null(
                    dataframe,
                    non_key_required_columns
                )
            )

        results.append(
            self.validate_business_keys(
                dataframe,
                business_keys
            )
        )

        if check_duplicates:

            results.append(
                self.validate_duplicates(
                    dataframe,
                    business_keys
                )
            )

        return {
            "passed": all(
                result["passed"]
                for result in results
            ),
            "results": results
        }