# Fabric Canonical Model Framework

## 1. Descripción

Framework metadata-driven desarrollado en Python para la construcción y administración de modelos canónicos sobre Microsoft Fabric.

Permite integrar información proveniente de una o múltiples fuentes Curated, aplicando reglas de identificación, mapeo, transformación, validación y persistencia mediante configuraciones YAML.

El framework está diseñado para ser reutilizable entre diferentes dominios de datos, evitando implementar un proceso independiente para cada entidad canónica.

## 2. Objetivo

Centralizar la construcción de modelos canónicos mediante un motor configurable que permita:

- Integrar múltiples fuentes de información en una entidad canónica.
- Identificar registros equivalentes mediante claves de negocio.
- Definir reglas de correspondencia entre atributos de origen y destino.
- Resolver diferencias y conflictos entre fuentes.
- Ejecutar transformaciones mediante YAML, SQL y PySpark.
- Procesar cambios de manera incremental utilizando campos de auditoría.
- Administrar diferentes estrategias de persistencia.
- Mantener trazabilidad y control de las ejecuciones.

## 3. Alcance

El framework consume tablas previamente preparadas en el schema Curated de los Lakehouses de Microsoft Fabric.

No contempla los procesos de:

- Ingesta desde sistemas de origen.
- Procesamiento Bronze.
- Preparación y transformación Silver.
- Construcción de las tablas Curated.

Estos procesos son responsabilidad del framework existente de Bronze y Silver.

El Canonical Model Framework comienza su ejecución a partir de las tablas Curated disponibles.

## 4. Arquitectura

La arquitectura contempla los siguientes componentes:

1. **Curated Sources:** tablas preparadas que alimentan las entidades canónicas.
2. **Configuration Registry:** registro de entidades habilitadas y sus configuraciones.
3. **Source Reader:** lectura de las fuentes Curated.
4. **Watermark Engine:** detección y control de cambios incrementales.
5. **Entity Resolution Engine:** identificación de registros equivalentes entre fuentes.
6. **Mapping Engine:** correspondencia de atributos de origen y destino.
7. **Transformation Engine:** ejecución de transformaciones SQL y PySpark.
8. **Validation Engine:** validación de claves, atributos y reglas de calidad.
9. **Persistence Engine:** escritura y actualización de entidades canónicas.
10. **Audit and Execution:** trazabilidad y registro de resultados.

## 5. Estrategias de procesamiento

### 5.1. Full Load

Reconstrucción completa de una entidad canónica a partir de sus fuentes configuradas.

### 5.2. Upsert

Inserción de registros nuevos y actualización de registros existentes cuando se identifican cambios.

La identificación de registros se realiza mediante las claves de negocio definidas para cada entidad.

### 5.3. SCD Type 2

Conservación del historial de cambios de los atributos mediante versionamiento de registros.

## 6. Procesamiento incremental

El framework utilizará los campos de auditoría disponibles en las tablas Curated:

- `fecha_creacion`: fecha original de creación del registro.
- `fecha_actualizacion`: fecha de la última modificación del registro.

Cada fuente tendrá un watermark independiente, administrado por el framework.

Cuando se detecten cambios, se identificarán las entidades afectadas y se reconstruirán sus registros considerando las fuentes relacionadas.

Los watermarks se actualizarán únicamente después de confirmar exitosamente la persistencia correspondiente.

## 7. Configuración metadata-driven

La definición de los modelos canónicos se realizará mediante archivos YAML.

Se contemplan dos niveles de configuración:

- **Registry:** determina las entidades habilitadas para procesamiento.
- **Entity Configuration:** define fuentes, atributos, claves de negocio, integraciones, transformaciones y estrategias de persistencia de cada entidad.

La configuración YAML será la fuente oficial de definición del modelo.

## 8. Estructura del proyecto

```text
fabric_canonical_model_framework/
│
├── fabric_canonical_model/
│   └── __init__.py
│
├── config/
│   └── entities/
│
├── tests/
│
├── pyproject.toml
└── README.md
```

La estructura se ampliará a medida que se implementen los componentes del framework.

## 9. Entorno tecnológico

- Microsoft Fabric.
- Python.
- PySpark.
- Delta Lake.
- YAML.
- SQL.
- Azure DevOps.

## 10. Instalación

La librería se distribuye mediante un paquete Python Wheel (`.whl`).

La compilación se realiza desde la raíz del proyecto:

```bash
python -m pip install build
python -m build
```

Los artefactos generados se almacenan en el directorio `dist/`.

## 11. Ubicación de configuraciones en Microsoft Fabric

Los archivos YAML desplegados se almacenarán en la sección Files del Lakehouse:

```text
Files/
└── CanonicalModel/
    ├── config/
    │   ├── registry.yml
    │   └── entities/
    │       ├── pozo.yml
    │       ├── campo.yml
    │       └── pozo_bore.yml
    │
    └── sql/
```

## 12. Ejecución

La ejecución del framework se centralizará mediante un notebook transversal:

`nb_create_canonical_model`

Este notebook invocará el motor de construcción y permitirá seleccionar las entidades y estrategias de procesamiento.

## 13. Principios de diseño

- Separación entre ingesta, preparación y construcción del modelo canónico.
- Configuración externa mediante YAML.
- Reutilización de componentes.
- Independencia entre entidades.
- Procesamiento incremental por fuente.
- Persistencia idempotente.
- Trazabilidad de ejecuciones.
- Validación de integridad y calidad.
- Extensibilidad para nuevos dominios y entidades.

## 14. Versionamiento

El proyecto utiliza versionamiento semántico:

`MAJOR.MINOR.PATCH`

La versión inicial del framework es `0.1.0`.