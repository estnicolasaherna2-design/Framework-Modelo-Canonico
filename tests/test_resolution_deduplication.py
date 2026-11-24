import unittest
from datetime import datetime

from fabric_canonical_model.exceptions import EntityResolutionError

try:
    from pyspark.sql import SparkSession

    from fabric_canonical_model.lib_mapping import AttributeMapper
    from fabric_canonical_model.lib_resolution import EntityResolver
except ModuleNotFoundError as exc:
    if exc.name != "pyspark" and not exc.name.startswith("pyspark."):
        raise
    SparkSession = None
    AttributeMapper = None
    EntityResolver = None


@unittest.skipIf(SparkSession is None, "PySpark is required for resolver tests.")
class SourceDeduplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("canonical-model-source-deduplication-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _source(self, rows):
        return self.spark.createDataFrame(
            rows,
            schema=(
                "uwi string, Well_Name string, "
                "fecha_actualizacion timestamp"
            )
        )

    @staticmethod
    def _deduplication_config():
        return {
            source: {"strategy": "DISTINCT"}
            for source in ("AVM", "BDP")
        }

    def test_distinct_by_uwi_keeps_one_row_per_well(self):
        sources = {
            "AVM": self._source([
                ("W1", "AVM anterior", datetime(2025, 1, 1)),
                ("W1", "AVM vigente", datetime(2025, 1, 2)),
                ("W1", "AVM vigente", datetime(2025, 1, 2)),
                ("W2", "Pozo AVM", datetime(2025, 1, 1))
            ]),
            "BDP": self._source([
                ("W1", "Pozo BDP", datetime(2025, 1, 3)),
                ("W3", "Pozo BDP 3", datetime(2025, 1, 1))
            ])
        }

        result = EntityResolver().resolve_entity(
            dataframes=sources,
            business_keys=["uwi"],
            attributes_config=[{
                "name": "Well_Name",
                "strategy": "SOURCE_PRIORITY",
                "priority": ["AVM", "BDP"]
            }],
            source_deduplication=self._deduplication_config()
        )

        actual = {
            row["uwi"]: row["Well_Name"]
            for row in result.select("uwi", "Well_Name")
            .orderBy("uwi")
            .collect()
        }
        self.assertEqual(set(actual), {"W1", "W2", "W3"})
        self.assertEqual(len(actual), 3)
        self.assertIn(actual["W1"], {"AVM anterior", "AVM vigente"})
        self.assertEqual(actual["W2"], "Pozo AVM")
        self.assertEqual(actual["W3"], "Pozo BDP 3")

    def test_distinct_by_uwi_does_not_reject_different_transactions(self):
        sources = {
            "AVM": self._source([
                ("W1", "AVM valor A", datetime(2025, 1, 2)),
                ("W1", "AVM valor B", datetime(2025, 1, 2))
            ]),
            "BDP": self._source([
                ("W1", "Pozo BDP", datetime(2025, 1, 3))
            ])
        }

        result = EntityResolver().resolve_entity(
            dataframes=sources,
            business_keys=["uwi"],
            attributes_config=[{
                "name": "Well_Name",
                "strategy": "SOURCE_PRIORITY",
                "priority": ["AVM", "BDP"]
            }],
            source_deduplication=self._deduplication_config()
        )

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["uwi"], "W1")
        self.assertIn(rows[0]["Well_Name"], {"AVM valor A", "AVM valor B"})

    def test_source_mapping_uses_fallback_column_and_keeps_non_null_transaction(self):
        source = self.spark.createDataFrame(
            [
                ("W1", None, None),
                ("W1", None, "Plot name")
            ],
            schema="uwi string, well_name string, plot_name string"
        )
        mapped = AttributeMapper().map_source(
            dataframe=source,
            source_name="BDP",
            attributes_config=[{
                "name": "Well_Name",
                "type": "string",
                "mapping": {
                    "BDP": ["well_name", "plot_name"]
                }
            }],
            include_unmapped=True
        )

        deduplicated = EntityResolver.deduplicate_sources(
            dataframes={"BDP": mapped},
            business_keys=["uwi"],
            source_deduplication={
                "BDP": {"strategy": "DISTINCT"}
            }
        )

        rows = deduplicated["BDP"].select("uwi", "Well_Name").collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Well_Name"], "Plot name")

    def test_priority_controls_other_source_fallback_or_null(self):
        sources = {
            "BDP": self._source([
                ("W1", None, datetime(2025, 1, 1))
            ]),
            "AVM": self._source([
                ("W1", "AVM name", datetime(2025, 1, 1))
            ])
        }
        attribute = {
            "name": "Well_Name",
            "strategy": "SOURCE_PRIORITY",
            "priority": ["BDP", "AVM"]
        }
        with_fallback = EntityResolver().resolve_entity(
            dataframes=sources,
            business_keys=["uwi"],
            attributes_config=[attribute]
        )
        without_fallback = EntityResolver().resolve_entity(
            dataframes=sources,
            business_keys=["uwi"],
            attributes_config=[{
                "name": "Well_Name",
                "strategy": "SOURCE_PRIORITY",
                "priority": ["BDP"]
            }]
        )

        self.assertEqual(with_fallback.first()["Well_Name"], "AVM name")
        self.assertIsNone(without_fallback.first()["Well_Name"])

    def test_deduplication_config_rejects_undeclared_sources(self):
        with self.assertRaisesRegex(
            EntityResolutionError,
            "fuentes no declaradas"
        ):
            EntityResolver.validate_source_deduplication_config(
                ["AVM", "BDP"],
                {"OFM": {"strategy": "DISTINCT"}}
            )


if __name__ == "__main__":
    unittest.main()
