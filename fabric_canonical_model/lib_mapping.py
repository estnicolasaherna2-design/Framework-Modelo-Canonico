
"""
Mapeo de atributos de fuentes hacia estructuras canonicas.
"""

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_canonical_model.exceptions import ValidationError


class AttributeMapper:
    """
    Administra el mapeo de columnas de origen hacia
    los atributos de una entidad canonica.
    """

    def __init__(self):
        pass

    # ---------------------------------------------------------
    # VALIDACION DE CONFIGURACION
    # ---------------------------------------------------------

    @staticmethod
    def _validate_attributes_config(
        attributes_config: List[Dict[str, Any]]
    ) -> None:
        """
        Valida la estructura general de la configuracion
        de atributos canonicos.
        """

        if not isinstance(attributes_config, list):
            raise ValidationError(
                "La configuracion de atributos debe ser una lista."
            )

        if not attributes_config:
            raise ValidationError(
                "La configuracion de atributos esta vacia."
            )

        canonical_names = []

        for attribute in attributes_config:

            if not isinstance(attribute, dict):
                raise ValidationError(
                    "Cada atributo debe estar definido "
                    "como un diccionario."
                )

            canonical_name = attribute.get("name")

            if (
                not isinstance(canonical_name, str)
                or not canonical_name.strip()
            ):
                raise ValidationError(
                    "Existe un atributo sin nombre canonico valido."
                )

            mappings = attribute.get("mapping", {})

            if not isinstance(mappings, dict):
                raise ValidationError(
                    f"El mapping del atributo '{canonical_name}' "
                    "debe ser un diccionario."
                )

            for source_name, source_column in mappings.items():

                if (
                    not isinstance(source_name, str)
                    or not source_name.strip()
                ):
                    raise ValidationError(
                        f"El atributo '{canonical_name}' "
                        "contiene una fuente con nombre invalido."
                    )

                if (
                    source_column is not None
                    and (
                        not isinstance(source_column, str)
                        or not source_column.strip()
                    )
                ):
                    raise ValidationError(
                        f"El mapping del atributo '{canonical_name}' "
                        f"en la fuente '{source_name}' "
                        "contiene una columna invalida."
                    )

            data_type = attribute.get("type")

            if data_type is not None:

                if (
                    not isinstance(data_type, str)
                    or not data_type.strip()
                ):
                    raise ValidationError(
                        f"El tipo del atributo '{canonical_name}' "
                        "no es valido."
                    )

            canonical_names.append(canonical_name)

        normalized_names = [
            name.lower()
            for name in canonical_names
        ]

        if len(normalized_names) != len(set(normalized_names)):
            raise ValidationError(
                "Existen nombres canonicos duplicados "
                "o ambiguos en la configuracion."
            )

    @staticmethod
    def _validate_mapping(
        dataframe: DataFrame,
        source_name: str,
        attributes_config: List[Dict[str, Any]]
    ) -> None:
        """
        Valida que las columnas configuradas existan
        en la fuente correspondiente.
        """

        if not isinstance(dataframe, DataFrame):
            raise ValidationError(
                f"La fuente '{source_name}' "
                "no contiene un DataFrame valido."
            )

        if (
            not isinstance(source_name, str)
            or not source_name.strip()
        ):
            raise ValidationError(
                "El nombre de la fuente no es valido."
            )

        AttributeMapper._validate_attributes_config(
            attributes_config
        )

        normalized_columns = [
            column.lower()
            for column in dataframe.columns
        ]

        if len(normalized_columns) != len(set(normalized_columns)):
            raise ValidationError(
                f"La fuente '{source_name}' contiene "
                "columnas duplicadas o ambiguas."
            )

        for attribute in attributes_config:

            canonical_name = attribute["name"]
            mappings = attribute.get("mapping", {})

            source_column = mappings.get(source_name)

            if source_column is None:
                continue

            if source_column not in dataframe.columns:
                raise ValidationError(
                    f"La columna '{source_column}' del atributo "
                    f"'{canonical_name}' no existe en "
                    f"la fuente '{source_name}'."
                )

    @staticmethod
    def _validate_source_column_collisions(
        source_name: str,
        attributes_config: List[Dict[str, Any]]
    ) -> None:
        """
        Evita que una misma columna de origen se utilice
        para construir varios atributos canonicos.
        """

        mapped_columns = {}

        for attribute in attributes_config:

            canonical_name = attribute["name"]
            source_column = attribute.get(
                "mapping",
                {}
            ).get(source_name)

            if source_column is None:
                continue

            normalized_column = source_column.lower()

            if normalized_column in mapped_columns:

                previous_attribute = mapped_columns[
                    normalized_column
                ]

                raise ValidationError(
                    f"La columna '{source_column}' de la fuente "
                    f"'{source_name}' esta asignada a los atributos "
                    f"'{previous_attribute}' y '{canonical_name}'."
                )

            mapped_columns[normalized_column] = canonical_name

    # ---------------------------------------------------------
    # MAPEO DE UNA FUENTE
    # ---------------------------------------------------------

    def map_source(
        self,
        dataframe: DataFrame,
        source_name: str,
        attributes_config: List[Dict[str, Any]],
        include_unmapped: bool = False
    ) -> DataFrame:
        """
        Mapea las columnas de una fuente a nombres canonicos.

        Args:
            dataframe:
                DataFrame de origen.

            source_name:
                Identificador de la fuente.

            attributes_config:
                Configuracion de atributos canonicos.

            include_unmapped:
                Conserva columnas que no participan en el mapeo.

        Returns:
            DataFrame con nombres canonicos.
        """

        if not isinstance(include_unmapped, bool):
            raise ValidationError(
                "include_unmapped debe ser un valor booleano."
            )

        self._validate_mapping(
            dataframe,
            source_name,
            attributes_config
        )

        self._validate_source_column_collisions(
            source_name,
            attributes_config
        )

        select_expressions = []

        mapped_columns = set()

        canonical_columns = set()

        for attribute in attributes_config:

            canonical_name = attribute["name"]

            mappings = attribute.get("mapping", {})

            source_column = mappings.get(source_name)

            if source_column is None:
                continue

            if canonical_name.lower() in canonical_columns:
                raise ValidationError(
                    f"El atributo canonico '{canonical_name}' "
                    "esta duplicado."
                )

            expression = F.col(
                f"`{source_column}`"
            )

            data_type = attribute.get("type")

            if data_type:

                try:
                    expression = expression.cast(
                        data_type.strip()
                    )

                except Exception as exc:
                    raise ValidationError(
                        f"No fue posible configurar la conversion "
                        f"del atributo '{canonical_name}' "
                        f"al tipo '{data_type}'. "
                        f"Detalle: {str(exc)}"
                    ) from exc

            select_expressions.append(
                expression.alias(canonical_name)
            )

            mapped_columns.add(
                source_column.lower()
            )

            canonical_columns.add(
                canonical_name.lower()
            )

        if include_unmapped:

            for column_name in dataframe.columns:

                normalized_column = column_name.lower()

                if normalized_column in mapped_columns:
                    continue

                if normalized_column in canonical_columns:
                    raise ValidationError(
                        f"La columna no mapeada '{column_name}' "
                        "genera una colision con un atributo canonico."
                    )

                select_expressions.append(
                    F.col(f"`{column_name}`")
                )

                canonical_columns.add(
                    normalized_column
                )

        if not select_expressions:
            raise ValidationError(
                f"No se encontraron atributos para mapear "
                f"en la fuente '{source_name}'."
            )

        return dataframe.select(
            *select_expressions
        )

    # ---------------------------------------------------------
    # MAPEO DE MULTIPLES FUENTES
    # ---------------------------------------------------------

    def map_sources(
        self,
        dataframes: Dict[str, DataFrame],
        attributes_config: List[Dict[str, Any]],
        include_unmapped: bool = False
    ) -> Dict[str, DataFrame]:
        """
        Aplica el mapeo a multiples fuentes.

        Returns:
            Diccionario con los DataFrames mapeados.
        """

        if not isinstance(dataframes, dict) or not dataframes:
            raise ValidationError(
                "Se debe proporcionar un diccionario de fuentes."
            )

        if not isinstance(include_unmapped, bool):
            raise ValidationError(
                "include_unmapped debe ser un valor booleano."
            )

        self._validate_attributes_config(
            attributes_config
        )

        mapped_dataframes = {}

        for source_name, dataframe in dataframes.items():

            mapped_dataframes[source_name] = self.map_source(
                dataframe=dataframe,
                source_name=source_name,
                attributes_config=attributes_config,
                include_unmapped=include_unmapped
            )

        return mapped_dataframes

    # ---------------------------------------------------------
    # METADATOS DE ATRIBUTOS CANONICOS
    # ---------------------------------------------------------

    @staticmethod
    def get_canonical_columns(
        attributes_config: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Obtiene los nombres de los atributos canonicos.
        """

        AttributeMapper._validate_attributes_config(
            attributes_config
        )

        return [
            attribute["name"]
            for attribute in attributes_config
        ]

    def validate_canonical_columns(
        self,
        dataframe: DataFrame,
        attributes_config: List[Dict[str, Any]]
    ) -> None:
        """
        Valida que un DataFrame contenga los atributos canonicos.
        """

        if not isinstance(dataframe, DataFrame):
            raise ValidationError(
                "El objeto recibido no es un DataFrame valido."
            )

        expected_columns = self.get_canonical_columns(
            attributes_config
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

        missing_columns = [
            column
            for column in expected_columns
            if column not in dataframe.columns
        ]

        if missing_columns:
            raise ValidationError(
                "Faltan atributos canonicos en el DataFrame: "
                f"{', '.join(missing_columns)}"
            )
