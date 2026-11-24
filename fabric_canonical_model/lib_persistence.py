
"""
Persistencia de entidades canonicas en Delta Lake.
"""

import re
from typing import List, Optional

from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fabric_canonical_model.exceptions import PersistenceError
from fabric_canonical_model.lib_utils import get_spark


class CanonicalPersistence:
    """
    Administra la persistencia de entidades canonicas.

    Modos soportados:
        FULL_LOAD
        UPSERT
        SCD2

    Reglas generales:
        - Id es un identificador sustituto persistente.
        - fecha_creacion se conserva desde la primera insercion.
        - fecha_actualizacion cambia cuando cambia el contenido.
        - hash_contenido representa los atributos funcionales.
    """

    AUDIT_COLUMNS = [
        "fecha_creacion",
        "fecha_actualizacion"
    ]

    SCD2_COLUMNS = [
        "fecha_inicio_vigencia",
        "fecha_fin_vigencia",
        "registro_actual"
    ]

    HASH_COLUMN = "hash_contenido"
    ID_COLUMN = "Id"

    TECHNICAL_COLUMNS = {
        "Id",
        "fecha_creacion",
        "fecha_actualizacion",
        "hash_contenido",
        "fecha_inicio_vigencia",
        "fecha_fin_vigencia",
        "registro_actual"
    }

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        target_schema: str = "canonical"
    ):
        self.spark = spark or get_spark()
        self.target_schema = target_schema

        self._validate_identifier(target_schema)
        self._ensure_schema()

    # ---------------------------------------------------------
    # VALIDACIONES GENERALES
    # ---------------------------------------------------------

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        """
        Valida identificadores SQL.
        """

        if (
            not isinstance(identifier, str)
            or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*",
                identifier
            )
        ):
            raise PersistenceError(
                f"Identificador SQL no valido: '{identifier}'."
            )

    @staticmethod
    def _quote_column(column_name: str) -> str:
        """
        Escapa nombres de columnas para Spark SQL.
        """

        escaped = column_name.replace("`", "``")
        return f"`{escaped}`"

    @staticmethod
    def _column(column_name: str) -> F.Column:
        """
        Obtiene una referencia segura a una columna.
        """

        escaped = column_name.replace("`", "``")
        return F.col(f"`{escaped}`")

    def _ensure_schema(self) -> None:
        """
        Crea el schema de destino si no existe.
        """

        try:
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS "
                f"`{self.target_schema}`"
            )

        except Exception as exc:
            raise PersistenceError(
                f"No fue posible crear o verificar "
                f"el schema '{self.target_schema}'. "
                f"Detalle: {str(exc)}"
            ) from exc

    def _get_table_name(self, table_name: str) -> str:
        """
        Construye el nombre completo de la tabla.
        """

        self._validate_identifier(table_name)

        return f"{self.target_schema}.{table_name}"

    def _table_exists(self, table_name: str) -> bool:
        """
        Verifica si existe una tabla en el catalogo.
        """

        try:
            return self.spark.catalog.tableExists(table_name)

        except Exception as exc:
            raise PersistenceError(
                f"No fue posible verificar la tabla "
                f"'{table_name}'. Detalle: {str(exc)}"
            ) from exc

    @staticmethod
    def _validate_dataframe(
        dataframe: DataFrame,
        business_keys: List[str]
    ) -> None:
        """
        Valida el DataFrame y sus claves de negocio.
        """

        if not isinstance(dataframe, DataFrame):
            raise PersistenceError(
                "El objeto de entrada no es un DataFrame valido."
            )

        if (
            not isinstance(business_keys, list)
            or not business_keys
        ):
            raise PersistenceError(
                "Se debe proporcionar una lista de claves de negocio."
            )

        if any(
            not isinstance(key, str) or not key.strip()
            for key in business_keys
        ):
            raise PersistenceError(
                "Las claves de negocio deben ser nombres de "
                "columnas validos."
            )

        if len(business_keys) != len(set(business_keys)):
            raise PersistenceError(
                "La lista de claves de negocio contiene duplicados."
            )

        normalized_columns = [
            column.lower()
            for column in dataframe.columns
        ]

        if len(normalized_columns) != len(set(normalized_columns)):
            raise PersistenceError(
                "El DataFrame contiene nombres de columnas "
                "duplicados o ambiguos."
            )

        missing_columns = [
            key
            for key in business_keys
            if key not in dataframe.columns
        ]

        if missing_columns:
            raise PersistenceError(
                "Faltan claves de negocio en el DataFrame: "
                f"{', '.join(missing_columns)}"
            )

        reserved_keys = (
            CanonicalPersistence.TECHNICAL_COLUMNS
        )

        invalid_keys = [
            key
            for key in business_keys
            if key in reserved_keys
        ]

        if invalid_keys:
            raise PersistenceError(
                "Las columnas tecnicas no pueden ser claves "
                "de negocio: "
                f"{', '.join(invalid_keys)}"
            )

    @staticmethod
    def _validate_unique_keys(
        dataframe: DataFrame,
        business_keys: List[str]
    ) -> None:
        """
        Verifica que no existan claves nulas, vacias o duplicadas.
        """

        null_condition = None

        for key in business_keys:

            column = CanonicalPersistence._column(key)

            condition = column.isNull()

            if dataframe.schema[key].dataType.simpleString() in (
                "string",
                "char",
                "varchar"
            ):
                condition = (
                    condition
                    | (F.length(F.trim(column)) == 0)
                )

            null_condition = (
                condition
                if null_condition is None
                else null_condition | condition
            )

        if dataframe.filter(null_condition).limit(1).count() > 0:
            raise PersistenceError(
                "Existen registros con claves de negocio nulas "
                "o vacias."
            )

        duplicates = (
            dataframe
            .groupBy(*business_keys)
            .count()
            .filter(F.col("count") > 1)
        )

        if duplicates.limit(1).count() > 0:
            raise PersistenceError(
                "El DataFrame contiene claves de negocio duplicadas. "
                "No se realizara la persistencia."
            )

    # ---------------------------------------------------------
    # HASH Y PREPARACION DEL DATAFRAME
    # ---------------------------------------------------------

    @classmethod
    def _generate_hash(
        cls,
        dataframe: DataFrame,
        excluded_columns: Optional[List[str]] = None
    ) -> F.Column:
        """
        Genera un SHA-256 del contenido funcional.

        El orden de las columnas es deterministico.
        Los valores nulos se representan explicitamente.
        """

        excluded = set(
            excluded_columns or []
        )

        columns = sorted(
            column
            for column in dataframe.columns
            if column not in excluded
        )

        if not columns:
            raise PersistenceError(
                "No existen columnas para calcular el hash."
            )

        content = F.struct(
            *[
                cls._column(column).alias(column)
                for column in columns
            ]
        )

        serialized = F.to_json(
            content,
            options={
                "ignoreNullFields": "false"
            }
        )

        return F.sha2(serialized, 256)

    def _prepare_source(
        self,
        dataframe: DataFrame,
        business_keys: List[str],
        scd2: bool = False
    ) -> DataFrame:
        """
        Prepara el DataFrame para persistencia.

        Los campos tecnicos se administran internamente.
        No se reciben desde las fuentes como valores finales.
        """

        functional_columns = [
            column
            for column in dataframe.columns
            if column not in self.TECHNICAL_COLUMNS
        ]

        if not functional_columns:
            raise PersistenceError(
                "El DataFrame no contiene atributos funcionales."
            )

        prepared = dataframe.select(
            *[
                self._column(column)
                for column in functional_columns
            ]
        )

        prepared = prepared.withColumn(
            "Id",
            F.expr("uuid()")
        )

        prepared = prepared.withColumn(
            "fecha_creacion",
            F.current_timestamp()
        )

        prepared = prepared.withColumn(
            "fecha_actualizacion",
            F.current_timestamp()
        )

        prepared = prepared.withColumn(
            "hash_contenido",
            self._generate_hash(prepared)
        )

        if scd2:

            prepared = prepared.withColumn(
                "fecha_inicio_vigencia",
                F.current_timestamp()
            )

            prepared = prepared.withColumn(
                "fecha_fin_vigencia",
                F.lit(None).cast("timestamp")
            )

            prepared = prepared.withColumn(
                "registro_actual",
                F.lit(True)
            )

        return prepared

    # ---------------------------------------------------------
    # VALIDACIONES DE TABLAS EXISTENTES
    # ---------------------------------------------------------

    def _validate_target_schema(
        self,
        target: DataFrame,
        source: DataFrame,
        table_name: str,
        business_keys: List[str],
        scd2: bool = False
    ) -> None:
        """
        Comprueba que el esquema de destino sea compatible.
        """

        required_columns = (
            [
                "Id",
                "fecha_creacion",
                "fecha_actualizacion",
                "hash_contenido"
            ]
            + (
                [
                    "fecha_inicio_vigencia",
                    "fecha_fin_vigencia",
                    "registro_actual"
                ]
                if scd2
                else []
            )
        )

        missing_columns = [
            column
            for column in required_columns
            if column not in target.columns
        ]

        if missing_columns:
            raise PersistenceError(
                f"La tabla '{table_name}' no tiene todas las "
                "columnas tecnicas requeridas: "
                f"{', '.join(missing_columns)}. "
                "Se requiere una migracion controlada."
            )

        target_is_scd2 = (
            "registro_actual" in target.columns
        )

        if target_is_scd2 != scd2:
            raise PersistenceError(
                f"La tabla '{table_name}' tiene una estructura "
                "incompatible con el modo de persistencia solicitado."
            )

        source_functional = {
            column
            for column in source.columns
            if column not in self.TECHNICAL_COLUMNS
        }

        target_functional = {
            column
            for column in target.columns
            if column not in self.TECHNICAL_COLUMNS
        }

        if source_functional != target_functional:

            missing_in_target = sorted(
                source_functional - target_functional
            )

            missing_in_source = sorted(
                target_functional - source_functional
            )

            raise PersistenceError(
                f"El esquema funcional de '{table_name}' "
                "no coincide con el origen. "
                f"Columnas faltantes en destino: {missing_in_target}. "
                f"Columnas faltantes en origen: {missing_in_source}. "
                "Se requiere una migracion controlada."
            )

        for key in business_keys:
            if key not in target.columns:
                raise PersistenceError(
                    f"La clave de negocio '{key}' no existe "
                    f"en la tabla '{table_name}'."
                )

    @staticmethod
    def _validate_target_unique_keys(
        target: DataFrame,
        business_keys: List[str],
        current_only: bool = False
    ) -> None:
        """
        Verifica la unicidad de las claves en destino.
        """

        dataframe = target

        if current_only:
            dataframe = dataframe.filter(
                F.col("registro_actual") == True
            )

        CanonicalPersistence._validate_unique_keys(
            dataframe,
            business_keys
        )

    @staticmethod
    def _join_condition(
        business_keys: List[str],
        source_alias: str = "source",
        target_alias: str = "target"
    ):
        """
        Construye la condicion de union por claves de negocio.
        """

        condition = None

        for key in business_keys:

            source_column = F.col(
                f"{source_alias}.`{key}`"
            )

            target_column = F.col(
                f"{target_alias}.`{key}`"
            )

            expression = (
                source_column == target_column
            )

            condition = (
                expression
                if condition is None
                else condition & expression
            )

        return condition

    # ---------------------------------------------------------
    # CONSERVACION DE METADATOS
    # ---------------------------------------------------------

    def _preserve_existing_metadata(
        self,
        source: DataFrame,
        target: DataFrame,
        business_keys: List[str]
    ) -> DataFrame:
        """
        Conserva Id y fecha_creacion.

        fecha_actualizacion solo cambia cuando cambia el hash.
        """

        metadata = target.select(
            *[
                self._column(key)
                for key in business_keys
            ],
            F.col("Id").alias("__existing_Id"),
            F.col("fecha_creacion").alias(
                "__existing_fecha_creacion"
            ),
            F.col("fecha_actualizacion").alias(
                "__existing_fecha_actualizacion"
            ),
            F.col("hash_contenido").alias(
                "__existing_hash"
            )
        )

        comparison = (
            source.alias("source")
            .join(
                metadata.alias("target"),
                self._join_condition(business_keys),
                "left"
            )
        )

        hash_changed = ~(
            F.col("source.hash_contenido")
            .eqNullSafe(
                F.col("target.__existing_hash")
            )
        )

        columns = [
            F.col(f"source.{self._quote_column(column)}")
            for column in source.columns
            if column not in (
                "Id",
                "fecha_creacion",
                "fecha_actualizacion"
            )
        ]

        columns.extend([
            F.coalesce(
                F.col("target.__existing_Id"),
                F.col("source.Id")
            ).alias("Id"),

            F.coalesce(
                F.col("target.__existing_fecha_creacion"),
                F.col("source.fecha_creacion")
            ).alias("fecha_creacion"),

            F.when(
                F.col("target.__existing_Id").isNull(),
                F.col("source.fecha_actualizacion")
            )
            .when(
                hash_changed,
                F.current_timestamp()
            )
            .otherwise(
                F.col("target.__existing_fecha_actualizacion")
            ).alias("fecha_actualizacion")
        ])

        return comparison.select(*columns)

    # ---------------------------------------------------------
    # FULL LOAD
    # ---------------------------------------------------------

    def full_load(
        self,
        dataframe: DataFrame,
        table_name: str,
        business_keys: List[str]
    ) -> None:
        """
        Reemplaza completamente el contenido de una tabla.

        Conserva Id y fecha_creacion de registros existentes.
        Actualiza fecha_actualizacion solo si cambia el contenido.
        """

        full_table_name = self._get_table_name(table_name)

        self._validate_dataframe(
            dataframe,
            business_keys
        )

        self._validate_unique_keys(
            dataframe,
            business_keys
        )

        try:

            source = self._prepare_source(
                dataframe,
                business_keys
            )

            if self._table_exists(full_table_name):

                target = self.spark.table(
                    full_table_name
                )

                self._validate_target_schema(
                    target,
                    source,
                    full_table_name,
                    business_keys
                )

                self._validate_target_unique_keys(
                    target,
                    business_keys
                )

                prepared = self._preserve_existing_metadata(
                    source,
                    target,
                    business_keys
                )

            else:
                prepared = source

            prepared.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(full_table_name)

        except PersistenceError:
            raise

        except Exception as exc:
            raise PersistenceError(
                f"Error ejecutando FULL_LOAD sobre "
                f"'{full_table_name}'. Detalle: {str(exc)}"
            ) from exc

    # ---------------------------------------------------------
    # UPSERT
    # ---------------------------------------------------------

    def upsert(
        self,
        dataframe: DataFrame,
        table_name: str,
        business_keys: List[str]
    ) -> None:
        """
        Inserta registros nuevos y actualiza los modificados.

        Conserva:
            Id
            fecha_creacion

        Actualiza:
            fecha_actualizacion
            hash_contenido
            atributos funcionales modificados

        Los registros sin cambios no se actualizan.
        """

        full_table_name = self._get_table_name(table_name)

        self._validate_dataframe(
            dataframe,
            business_keys
        )

        self._validate_unique_keys(
            dataframe,
            business_keys
        )

        try:

            source = self._prepare_source(
                dataframe,
                business_keys
            )

            if not self._table_exists(full_table_name):

                source.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(full_table_name)

                return

            target = self.spark.table(
                full_table_name
            )

            self._validate_target_schema(
                target,
                source,
                full_table_name,
                business_keys
            )

            self._validate_target_unique_keys(
                target,
                business_keys
            )

            delta_table = DeltaTable.forName(
                self.spark,
                full_table_name
            )

            merge_condition = " AND ".join(
                [
                    f"target.{self._quote_column(key)} = "
                    f"source.{self._quote_column(key)}"
                    for key in business_keys
                ]
            )

            update_columns = [
                column
                for column in source.columns
                if column not in (
                    "Id",
                    "fecha_creacion"
                )
            ]

            update_set = {
                column: f"source.{self._quote_column(column)}"
                for column in update_columns
            }

            insert_values = {
                column: f"source.{self._quote_column(column)}"
                for column in source.columns
            }

            delta_table.alias("target") \
                .merge(
                    source.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdate(
                    condition=(
                        "NOT (target.hash_contenido "
                        "<=> source.hash_contenido)"
                    ),
                    set=update_set
                ) \
                .whenNotMatchedInsert(
                    values=insert_values
                ) \
                .execute()

        except PersistenceError:
            raise

        except Exception as exc:
            raise PersistenceError(
                f"Error ejecutando UPSERT sobre "
                f"'{full_table_name}'. Detalle: {str(exc)}"
            ) from exc

    # ---------------------------------------------------------
    # SCD TYPE 2
    # ---------------------------------------------------------

    def scd2(
        self,
        dataframe: DataFrame,
        table_name: str,
        business_keys: List[str]
    ) -> None:
        """
        Persiste una entidad mediante Slowly Changing Dimension 2.

        Cuando cambia un registro:
            - Cierra la version vigente.
            - Inserta una nueva version.
            - Conserva el Id de la entidad.
            - Conserva fecha_creacion.
            - Actualiza fecha_actualizacion.
        """

        full_table_name = self._get_table_name(table_name)

        self._validate_dataframe(
            dataframe,
            business_keys
        )

        self._validate_unique_keys(
            dataframe,
            business_keys
        )

        try:

            source = self._prepare_source(
                dataframe,
                business_keys,
                scd2=True
            )

            if not self._table_exists(full_table_name):

                source.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(full_table_name)

                return

            target = self.spark.table(
                full_table_name
            )

            self._validate_target_schema(
                target,
                source,
                full_table_name,
                business_keys,
                scd2=True
            )

            current = target.filter(
                F.col("registro_actual") == True
            )

            self._validate_target_unique_keys(
                current,
                business_keys
            )

            comparison = (
                source.alias("source")
                .join(
                    current.alias("target"),
                    self._join_condition(business_keys),
                    "left"
                )
            )

            hash_changed = ~(
                F.col("source.hash_contenido")
                .eqNullSafe(
                    F.col("target.hash_contenido")
                )
            )

            changed = (
                comparison
                .filter(
                    F.col("target.Id").isNull()
                    | hash_changed
                )
                .select(
                    *[
                        F.col(
                            f"source.{self._quote_column(column)}"
                        ).alias(column)
                        for column in source.columns
                    ],
                    F.col("target.Id").alias(
                        "__existing_Id"
                    ),
                    F.col("target.fecha_creacion").alias(
                        "__existing_fecha_creacion"
                    )
                )
            )

            if changed.limit(1).count() == 0:
                return

            changed = (
                changed
                .withColumn(
                    "Id",
                    F.coalesce(
                        F.col("__existing_Id"),
                        F.col("Id")
                    )
                )
                .withColumn(
                    "fecha_creacion",
                    F.coalesce(
                        F.col("__existing_fecha_creacion"),
                        F.col("fecha_creacion")
                    )
                )
                .withColumn(
                    "fecha_actualizacion",
                    F.current_timestamp()
                )
                .withColumn(
                    "fecha_inicio_vigencia",
                    F.current_timestamp()
                )
                .withColumn(
                    "fecha_fin_vigencia",
                    F.lit(None).cast("timestamp")
                )
                .withColumn(
                    "registro_actual",
                    F.lit(True)
                )
                .drop(
                    "__existing_Id",
                    "__existing_fecha_creacion"
                )
            )

            # -------------------------------------------------
            # Preparacion de acciones CLOSE e INSERT
            # -------------------------------------------------

            insert_rows = (
                changed
                .withColumn(
                    "__action",
                    F.lit("INSERT")
                )
            )

            close_rows = changed.select(
                *[
                    self._column(key)
                    for key in business_keys
                ]
            ).withColumn(
                "__action",
                F.lit("CLOSE")
            )

            for field in insert_rows.schema.fields:

                if field.name not in (
                    business_keys + ["__action"]
                ):
                    close_rows = close_rows.withColumn(
                        field.name,
                        F.lit(None).cast(field.dataType)
                    )

            close_rows = close_rows.select(
                *insert_rows.columns
            )

            staged = close_rows.unionByName(
                insert_rows
            )

            # -------------------------------------------------
            # MERGE unico para cerrar e insertar versiones
            # -------------------------------------------------

            delta_table = DeltaTable.forName(
                self.spark,
                full_table_name
            )

            merge_condition = " AND ".join(
                [
                    f"target.{self._quote_column(key)} = "
                    f"source.{self._quote_column(key)}"
                    for key in business_keys
                ]
            )

            merge_condition += (
                " AND source.__action = 'CLOSE'"
                " AND target.registro_actual = true"
            )

            update_set = {
                "registro_actual": "false",
                "fecha_fin_vigencia": "current_timestamp()"
            }

            insert_values = {
                column: f"source.{self._quote_column(column)}"
                for column in changed.columns
            }

            delta_table.alias("target") \
                .merge(
                    staged.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdate(
                    set=update_set
                ) \
                .whenNotMatchedInsert(
                    condition="source.__action = 'INSERT'",
                    values=insert_values
                ) \
                .execute()

        except PersistenceError:
            raise

        except Exception as exc:
            raise PersistenceError(
                f"Error ejecutando SCD2 sobre "
                f"'{full_table_name}'. Detalle: {str(exc)}"
            ) from exc