
"""
Resolucion de identidad e integracion de entidades canonicas.
"""

from functools import reduce
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabric_canonical_model.exceptions import EntityResolutionError


class EntityResolver:
    """
    Resuelve la identidad y consolida atributos de entidades
    provenientes de diferentes fuentes.
    """

    SUPPORTED_STRATEGIES = {
        "SOURCE_PRIORITY",
        "FIRST_NON_NULL",
        "CONSISTENT"
    }

    def __init__(self):
        pass

    # ---------------------------------------------------------
    # VALIDACIONES
    # ---------------------------------------------------------

    @staticmethod
    def _validate_sources(
        dataframes: Dict[str, DataFrame]
    ) -> None:
        """
        Valida los DataFrames recibidos.
        """

        if not isinstance(dataframes, dict) or not dataframes:
            raise EntityResolutionError(
                "Se debe proporcionar un diccionario de fuentes."
            )

        for source_name, dataframe in dataframes.items():

            if (
                not isinstance(source_name, str)
                or not source_name.strip()
            ):
                raise EntityResolutionError(
                    "Existe una fuente con nombre invalido."
                )

            if not isinstance(dataframe, DataFrame):
                raise EntityResolutionError(
                    f"La fuente '{source_name}' "
                    "no contiene un DataFrame valido."
                )

            normalized_columns = [
                column.lower()
                for column in dataframe.columns
            ]

            if len(normalized_columns) != len(set(normalized_columns)):
                raise EntityResolutionError(
                    f"La fuente '{source_name}' contiene "
                    "columnas duplicadas o ambiguas."
                )

    @staticmethod
    def _validate_business_keys(
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> None:
        """
        Valida que cada fuente contenga las claves de negocio.
        """

        if (
            not isinstance(business_keys, list)
            or not business_keys
        ):
            raise EntityResolutionError(
                "Se debe configurar al menos una clave de negocio."
            )

        if len(business_keys) != len(set(business_keys)):
            raise EntityResolutionError(
                "La configuracion contiene claves de negocio duplicadas."
            )

        if any(
            not isinstance(key, str) or not key.strip()
            for key in business_keys
        ):
            raise EntityResolutionError(
                "Las claves de negocio deben ser nombres validos."
            )

        source_mapping = source_mapping or {}

        for source_name, dataframe in dataframes.items():

            mapping = source_mapping.get(source_name, {})

            if not isinstance(mapping, dict):
                raise EntityResolutionError(
                    f"El mapping de la fuente '{source_name}' "
                    "debe ser un diccionario."
                )

            for business_key in business_keys:

                source_column = mapping.get(
                    business_key,
                    business_key
                )

                if source_column not in dataframe.columns:
                    raise EntityResolutionError(
                        f"La clave de negocio '{source_column}' "
                        f"no existe en la fuente '{source_name}'."
                    )

    @staticmethod
    def validate_source_deduplication_config(
        source_names: List[str],
        source_deduplication: Optional[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Valida las fuentes configuradas para DISTINCT por clave."""

        if source_deduplication is None:
            return {}

        if (
            not isinstance(source_names, list)
            or any(
                not isinstance(name, str) or not name.strip()
                for name in source_names
            )
        ):
            raise EntityResolutionError(
                "source_names debe ser una lista de nombres válidos."
            )

        if not isinstance(source_deduplication, dict):
            raise EntityResolutionError(
                "source_deduplication debe ser un diccionario."
            )

        if any(
            not isinstance(name, str) or not name.strip()
            for name in source_deduplication
        ):
            raise EntityResolutionError(
                "source_deduplication contiene un nombre de fuente inválido."
            )

        unknown_sources = set(source_deduplication) - set(source_names)
        if unknown_sources:
            raise EntityResolutionError(
                "source_deduplication referencia fuentes no declaradas: "
                f"{sorted(unknown_sources)}."
            )

        normalized = {}
        for source_name, config in source_deduplication.items():
            if (
                not isinstance(config, dict)
                or set(config) != {"strategy"}
                or config["strategy"] != "DISTINCT"
            ):
                raise EntityResolutionError(
                    f"source_deduplication.{source_name} debe contener "
                    "strategy: DISTINCT."
                )
            normalized[source_name] = "DISTINCT"

        return normalized

    @staticmethod
    def deduplicate_sources(
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        source_deduplication: Optional[Dict[str, Any]] = None,
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, DataFrame]:
        """Consolida una fila por clave y conserva valores no nulos."""

        rules = EntityResolver.validate_source_deduplication_config(
            list(dataframes),
            source_deduplication
        )
        if not rules:
            return dataframes

        EntityResolver._validate_sources(dataframes)
        EntityResolver._validate_business_keys(
            dataframes,
            business_keys,
            source_mapping
        )
        source_mapping = source_mapping or {}
        result = dict(dataframes)

        for source_name in rules:
            dataframe = dataframes[source_name]
            key_columns = [
                source_mapping.get(source_name, {}).get(key, key)
                for key in business_keys
            ]
            if len(key_columns) != len(set(key_columns)):
                raise EntityResolutionError(
                    f"La fuente '{source_name}' mapea varias claves de "
                    "negocio a la misma columna física."
                )
            value_columns = [
                column
                for column in dataframe.columns
                if column not in key_columns
            ]
            if value_columns:
                result[source_name] = dataframe.groupBy(
                    *key_columns
                ).agg(
                    *[
                        F.first(
                            F.col(f"`{column}`"),
                            ignorenulls=True
                        ).alias(column)
                        for column in value_columns
                    ]
                )
            else:
                result[source_name] = dataframe.select(
                    *key_columns
                ).dropDuplicates(key_columns)

        return result

    @staticmethod
    def _validate_source_uniqueness(
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> None:
        """
        Verifica que cada fuente tenga una sola fila por
        combinacion de claves de negocio.

        Los registros con claves nulas se excluyen porque
        no pueden participar en la identificacion de entidades.
        """

        source_mapping = source_mapping or {}

        for source_name, dataframe in dataframes.items():

            mapping = source_mapping.get(source_name, {})

            key_expressions = [
                F.col(
                    f"`{mapping.get(key, key)}`"
                ).alias(key)
                for key in business_keys
            ]

            keys_dataframe = dataframe.select(
                *key_expressions
            )

            valid_condition = reduce(
                lambda left, right: left & right,
                [
                    F.col(key).isNotNull()
                    for key in business_keys
                ]
            )

            valid_keys = keys_dataframe.filter(
                valid_condition
            )

            duplicates = (
                valid_keys
                .groupBy(*business_keys)
                .count()
                .filter(F.col("count") > 1)
            )

            if duplicates.limit(1).count() > 0:

                sample = [
                    row.asDict()
                    for row in duplicates.limit(5).collect()
                ]

                raise EntityResolutionError(
                    f"La fuente '{source_name}' contiene "
                    "multiples registros para una misma clave "
                    f"de negocio. Ejemplos: {sample}"
                )

    # ---------------------------------------------------------
    # PREPARACION DE FUENTES
    # ---------------------------------------------------------

    def _prepare_sources(
        self,
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, DataFrame]:
        """
        Normaliza las claves de negocio.

        Los atributos mantienen sus nombres originales.
        Su correspondencia se consulta mediante source_mapping.
        """

        self._validate_sources(dataframes)

        self._validate_business_keys(
            dataframes,
            business_keys,
            source_mapping
        )

        source_mapping = source_mapping or {}

        prepared = {}

        for source_name, dataframe in dataframes.items():

            mapping = source_mapping.get(source_name, {})

            source_key_columns = {
                mapping.get(key, key)
                for key in business_keys
            }

            source_key_columns.update(business_keys)

            select_expressions = [
                F.col(
                    f"`{mapping.get(key, key)}`"
                ).alias(key)
                for key in business_keys
            ]

            for column_name in dataframe.columns:

                if column_name not in source_key_columns:

                    select_expressions.append(
                        F.col(f"`{column_name}`")
                    )

            prepared[source_name] = dataframe.select(
                *select_expressions
            )

        return prepared

    @staticmethod
    def _get_source_column(
        source_name: str,
        canonical_attribute: str,
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> str:
        """
        Obtiene el nombre fisico de una columna en una fuente.
        """

        mapping = (
            source_mapping or {}
        ).get(source_name, {})

        return mapping.get(
            canonical_attribute,
            canonical_attribute
        )

    def _validate_attribute_configuration(
        self,
        attributes_config: List[Dict[str, Any]],
        business_keys: List[str]
    ) -> None:
        """
        Valida la configuracion de atributos.
        """

        if not isinstance(attributes_config, list):
            raise EntityResolutionError(
                "La configuracion de atributos debe ser una lista."
            )

        attribute_names = []

        for config in attributes_config:

            if not isinstance(config, dict):
                raise EntityResolutionError(
                    "Cada atributo debe estar definido como diccionario."
                )

            attribute = config.get("name")
            strategy = config.get("strategy")

            if (
                not isinstance(attribute, str)
                or not attribute.strip()
            ):
                raise EntityResolutionError(
                    "Existe un atributo sin nombre valido."
                )

            if attribute in business_keys:
                raise EntityResolutionError(
                    f"El atributo '{attribute}' ya esta definido "
                    "como clave de negocio."
                )

            if strategy not in self.SUPPORTED_STRATEGIES:
                raise EntityResolutionError(
                    f"Estrategia de resolucion no soportada: "
                    f"'{strategy}'."
                )

            if attribute in attribute_names:
                raise EntityResolutionError(
                    f"El atributo '{attribute}' esta configurado "
                    "mas de una vez."
                )

            optional_sources = config.get(
                "optional_sources",
                []
            )

            if not isinstance(optional_sources, list):
                raise EntityResolutionError(
                    f"optional_sources de '{attribute}' "
                    "debe ser una lista."
                )

            attribute_names.append(attribute)

    # ---------------------------------------------------------
    # IDENTIFICACION DE ENTIDADES
    # ---------------------------------------------------------

    def identify_entities(
        self,
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> DataFrame:
        """
        Identifica las entidades existentes entre fuentes.

        Devuelve una fila unica por combinacion de claves
        de negocio.
        """

        self._validate_sources(dataframes)

        prepared = self._prepare_sources(
            dataframes,
            business_keys,
            source_mapping
        )

        self._validate_source_uniqueness(
            dataframes,
            business_keys,
            source_mapping
        )

        key_dataframes = [
            dataframe.select(*business_keys)
            for dataframe in prepared.values()
        ]

        result = reduce(
            lambda left, right: left.unionByName(right),
            key_dataframes
        )

        valid_condition = reduce(
            lambda left, right: left & right,
            [
                F.col(key).isNotNull()
                for key in business_keys
            ]
        )

        return (
            result
            .filter(valid_condition)
            .dropDuplicates(business_keys)
        )

    # ---------------------------------------------------------
    # VALIDACION DE CONSISTENCIA
    # ---------------------------------------------------------

    def validate_consistency(
        self,
        dataframes: Dict[str, DataFrame],
        attribute: str,
        business_keys: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ) -> DataFrame:
        """
        Identifica claves de negocio cuyos valores de un atributo
        difieren entre fuentes.

        Devuelve los conflictos sin seleccionar un valor ganador.

        Los atributos nulos no participan en la comparacion.
        """

        self._validate_sources(dataframes)

        if (
            not isinstance(attribute, str)
            or not attribute.strip()
        ):
            raise EntityResolutionError(
                "Se debe proporcionar un atributo valido."
            )

        prepared = self._prepare_sources(
            dataframes,
            business_keys,
            source_mapping
        )

        self._validate_source_uniqueness(
            dataframes,
            business_keys,
            source_mapping
        )

        attribute_dataframes = []

        for source_name, dataframe in prepared.items():

            source_column = self._get_source_column(
                source_name,
                attribute,
                source_mapping
            )

            if source_column not in dataframe.columns:
                raise EntityResolutionError(
                    f"El atributo '{source_column}' no existe "
                    f"en la fuente '{source_name}'."
                )

            attribute_dataframes.append(
                dataframe.select(
                    *business_keys,
                    F.col(
                        f"`{source_column}`"
                    ).cast("string").alias("attribute_value")
                )
                .withColumn(
                    "source_name",
                    F.lit(source_name)
                )
            )

        combined = reduce(
            lambda left, right: left.unionByName(right),
            attribute_dataframes
        )

        values = (
            combined
            .filter(F.col("attribute_value").isNotNull())
            .groupBy(*business_keys)
            .agg(
                F.countDistinct(
                    "attribute_value"
                ).alias("distinct_value_count"),

                F.collect_set(
                    F.struct(
                        "source_name",
                        "attribute_value"
                    )
                ).alias("source_values")
            )
        )

        return values.filter(
            F.col("distinct_value_count") > 1
        )

    # ---------------------------------------------------------
    # RESOLUCION DE ATRIBUTOS
    # ---------------------------------------------------------

    def resolve_attribute(
        self,
        dataframes: Dict[str, DataFrame],
        attribute: str,
        business_keys: List[str],
        priority: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        optional_sources: Optional[List[str]] = None,
        identities: Optional[DataFrame] = None
    ) -> DataFrame:
        """
        Resuelve un atributo utilizando el primer valor no nulo
        segun el orden de prioridad.

        Devuelve una fila por clave de negocio.

        Por defecto, el atributo debe existir en todas las fuentes
        incluidas en priority.

        Una fuente puede omitirse explicitamente mediante
        optional_sources.
        """

        self._validate_sources(dataframes)

        if (
            not isinstance(attribute, str)
            or not attribute.strip()
        ):
            raise EntityResolutionError(
                "Se debe proporcionar un atributo valido."
            )

        if (
            not isinstance(priority, list)
            or not priority
        ):
            raise EntityResolutionError(
                f"No se definio prioridad para el atributo "
                f"'{attribute}'."
            )

        if len(priority) != len(set(priority)):
            raise EntityResolutionError(
                f"La prioridad del atributo '{attribute}' "
                "contiene fuentes duplicadas."
            )

        unknown_sources = [
            source
            for source in priority
            if source not in dataframes
        ]

        if unknown_sources:
            raise EntityResolutionError(
                "Las siguientes fuentes de prioridad no existen: "
                f"{', '.join(unknown_sources)}"
            )

        optional_sources = optional_sources or []

        if not isinstance(optional_sources, list):
            raise EntityResolutionError(
                "optional_sources debe ser una lista."
            )

        invalid_optional_sources = [
            source
            for source in optional_sources
            if source not in priority
        ]

        if invalid_optional_sources:
            raise EntityResolutionError(
                "Las fuentes opcionales deben estar incluidas "
                "en la prioridad del atributo."
            )

        prepared = self._prepare_sources(
            dataframes,
            business_keys,
            source_mapping
        )

        self._validate_source_uniqueness(
            dataframes,
            business_keys,
            source_mapping
        )

        if identities is None:
            identities = self.identify_entities(
                dataframes,
                business_keys,
                source_mapping
            )

        candidates = []

        for index, source_name in enumerate(priority):

            dataframe = prepared[source_name]

            source_column = self._get_source_column(
                source_name,
                attribute,
                source_mapping
            )

            if source_column not in dataframe.columns:

                if source_name in optional_sources:
                    continue

                raise EntityResolutionError(
                    f"El atributo '{source_column}' no existe "
                    f"en la fuente '{source_name}'. "
                    "Si su ausencia es esperada, configure "
                    "la fuente como opcional."
                )

            candidate_name = f"__candidate_{index}"

            candidate = dataframe.select(
                *business_keys,
                F.col(
                    f"`{source_column}`"
                ).alias(candidate_name)
            )

            candidates.append(candidate_name)

            if index == 0:
                resolved = identities

            resolved = resolved.join(
                candidate,
                on=business_keys,
                how="left"
            )

        if not candidates:
            raise EntityResolutionError(
                f"No se encontro el atributo '{attribute}' "
                "en ninguna fuente configurada."
            )

        result = resolved.select(
            *[
                F.col(key)
                for key in business_keys
            ],
            F.coalesce(
                *[
                    F.col(candidate)
                    for candidate in candidates
                ]
            ).alias(attribute)
        )

        return result

    # ---------------------------------------------------------
    # RESOLUCION CON VALIDACION DE CONSISTENCIA
    # ---------------------------------------------------------

    def resolve_consistent_attribute(
        self,
        dataframes: Dict[str, DataFrame],
        attribute: str,
        business_keys: List[str],
        priority: List[str],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        optional_sources: Optional[List[str]] = None,
        identities: Optional[DataFrame] = None
    ) -> DataFrame:
        """
        Valida que un atributo sea consistente entre fuentes.

        Si encuentra conflictos, detiene la resolucion.

        Si no encuentra conflictos, selecciona el primer valor
        no nulo segun la prioridad configurada.
        """

        conflicts = self.validate_consistency(
            dataframes=dataframes,
            attribute=attribute,
            business_keys=business_keys,
            source_mapping=source_mapping
        )

        if conflicts.limit(1).count() > 0:

            samples = [
                row.asDict(recursive=True)
                for row in conflicts.limit(10).collect()
            ]

            raise EntityResolutionError(
                f"Se encontraron conflictos de consistencia "
                f"en el atributo '{attribute}'. "
                f"Ejemplos: {samples}"
            )

        return self.resolve_attribute(
            dataframes=dataframes,
            attribute=attribute,
            business_keys=business_keys,
            priority=priority,
            source_mapping=source_mapping,
            optional_sources=optional_sources,
            identities=identities
        )

    # ---------------------------------------------------------
    # RESOLUCION DE ENTIDADES
    # ---------------------------------------------------------

    def resolve_entity(
        self,
        dataframes: Dict[str, DataFrame],
        business_keys: List[str],
        attributes_config: List[Dict[str, Any]],
        source_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        source_deduplication: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Construye una entidad consolidada a partir de varias fuentes.

        Estrategias soportadas:

            SOURCE_PRIORITY:
                Selecciona el primer valor no nulo segun priority.

            FIRST_NON_NULL:
                Selecciona el primer valor no nulo segun el orden
                de las fuentes recibidas.

            CONSISTENT:
                Valida que todas las fuentes tengan el mismo valor
                no nulo. Si existen conflictos, detiene el proceso.

        source_deduplication:
            Configuración opcional por fuente para aplicar DISTINCT sobre
            las claves de negocio antes de resolver atributos.
        """

        self._validate_sources(dataframes)

        self._validate_attribute_configuration(
            attributes_config,
            business_keys
        )

        self._validate_business_keys(
            dataframes,
            business_keys,
            source_mapping
        )

        dataframes = self.deduplicate_sources(
            dataframes=dataframes,
            business_keys=business_keys,
            source_deduplication=source_deduplication,
            source_mapping=source_mapping
        )

        identities = self.identify_entities(
            dataframes,
            business_keys,
            source_mapping
        )

        resolved = identities

        for attribute_config in attributes_config:

            attribute = attribute_config["name"]
            strategy = attribute_config["strategy"]

            optional_sources = attribute_config.get(
                "optional_sources",
                []
            )

            if strategy == "SOURCE_PRIORITY":

                priority = attribute_config.get(
                    "priority",
                    []
                )

                attribute_dataframe = self.resolve_attribute(
                    dataframes=dataframes,
                    attribute=attribute,
                    business_keys=business_keys,
                    priority=priority,
                    source_mapping=source_mapping,
                    optional_sources=optional_sources,
                    identities=identities
                )

            elif strategy == "FIRST_NON_NULL":

                priority = list(dataframes.keys())

                attribute_dataframe = self.resolve_attribute(
                    dataframes=dataframes,
                    attribute=attribute,
                    business_keys=business_keys,
                    priority=priority,
                    source_mapping=source_mapping,
                    optional_sources=optional_sources,
                    identities=identities
                )

            elif strategy == "CONSISTENT":

                priority = attribute_config.get(
                    "priority",
                    list(dataframes.keys())
                )

                attribute_dataframe = (
                    self.resolve_consistent_attribute(
                        dataframes=dataframes,
                        attribute=attribute,
                        business_keys=business_keys,
                        priority=priority,
                        source_mapping=source_mapping,
                        optional_sources=optional_sources,
                        identities=identities
                    )
                )

            else:
                raise EntityResolutionError(
                    f"Estrategia no soportada: '{strategy}'."
                )

            resolved = resolved.join(
                attribute_dataframe,
                on=business_keys,
                how="left"
            )

        return resolved
