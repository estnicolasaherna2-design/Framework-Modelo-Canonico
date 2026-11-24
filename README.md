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

El notebook operativo transversal está en:

`notebooks/nb_create_canonical_model.ipynb`

En Fabric, instala el wheel, adjunta el Lakehouse que contiene `Files/CanonicalModel/config/` y ejecuta el notebook completo. En uso normal solo se ajusta su celda `PARÁMETROS`: Lakehouse/config root, entidad opcional, modo opcional y modo PRECHECK o escritura. Los detalles de cada entidad permanecen en los YAML.

## 12.1. Notebooks de ejemplo

Los notebooks auxiliares están en `examples/notebooks/` y no son necesarios para una ejecución normal:

- `nb_framework_components_demo.ipynb`: utiliza datos sintéticos para probar mapping, resolución, transformaciones y validaciones sin leer ni escribir tablas del Lakehouse.
- `nb_fabric_integration_tests.ipynb`: ejecuta pruebas Delta de `FULL_LOAD`, `UPSERT`, `SCD2`, auditoría y watermarks en un schema de prueba único. Deja las tablas de prueba para su inspección y limpieza manual.

El notebook operativo hace precheck físico y luego usa los componentes existentes para leer, mapear, resolver, validar y persistir. **Limitación actual:** aunque los YAML declaren watermarks, esta versión lee las fuentes completas y no aplica procesamiento incremental. El precheck del notebook tampoco sustituye un validador central JSON Schema completo.

Las pruebas unitarias disponibles se ejecutan desde la raíz del repositorio con `python -m unittest discover -s tests -v`. Las pruebas Delta requieren ejecutar el notebook de integración en Fabric; no modifican ni eliminan tablas de negocio.

## 12.2. Ejemplo de entidad `pozo`

La configuración de ejemplo está en `entities/pozo.yml` y usa las hojas compartidas como fuentes `AVM` (`avm_pozos_ubicacion`) y `BDP` (`bdp_well`), con `uwi` como clave y `fecha_actualizacion` como watermark independiente por fuente. `Well_Name` se resuelve en orden AVM y luego BDP. Al desplegarla en el Lakehouse de esta implementación, ubícala en `Files/CanonicalModel/config/entities/pozo.yml`, junto con su `registry.yml` y `framework.yml`. Para reutilizar el notebook en otro Lakehouse, reemplaza estos YAML por la configuración de ese proyecto; el paquete Python no requiere cambios.

El ejemplo no incluye OFM porque no aparece en el archivo compartido. Tampoco declara un atributo `Well_ID`: las estructuras muestran `id_pozo` en AVM y `well_numeric_id` en BDP, que no se deben tratar como equivalentes sin una regla de negocio confirmada. Las columnas de coordenadas del ejemplo presentan valores numéricos escalados/no convencionales; no se mapean hasta confirmar su unidad y representación. La columna `index` de la vista del Excel se trata como índice de presentación, no como clave de negocio.

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