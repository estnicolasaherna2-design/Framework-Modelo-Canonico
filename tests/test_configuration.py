import ast
import json
import tempfile
import unittest
from pathlib import Path

from fabric_canonical_model.exceptions import ConfigurationError, RegistryError
from fabric_canonical_model.lib_config import FrameworkConfig
from fabric_canonical_model.lib_registry import CanonicalRegistry
from fabric_canonical_model.lib_utils import read_yaml_file


def framework_config():
    return {
        "environment": "TEST",
        "execution": {
            "default_mode": "UPSERT",
            "timezone": "America/Bogota"
        },
        "paths": {
            "config_root": "Files/CanonicalModel/config",
            "registry": "Files/CanonicalModel/config/registry.yml",
            "entities": "Files/CanonicalModel/config/entities"
        },
        "schemas": {
            "canonical": "canonical",
            "metadata": "canonical_metadata"
        }
    }


class ConfigurationTests(unittest.TestCase):
    def test_repository_framework_yaml_loads(self):
        config = FrameworkConfig.from_yaml("framework.yml")

        self.assertEqual(config.get_environment(), "DEV")
        self.assertEqual(config.get("execution")["default_mode"], "UPSERT")
        self.assertEqual(config.get_schema("canonical"), "canonical")

    def test_pozo_example_matches_the_two_attached_source_schemas(self):
        registry = CanonicalRegistry("registry.yml")
        entity_path = registry.get_entity_config_path("pozo")
        entity = read_yaml_file(entity_path)

        self.assertEqual(entity["name"], "pozo")
        self.assertEqual(
            [(source["name"], source["table"]) for source in entity["sources"]],
            [("AVM", "avm_pozos_ubicacion"), ("BDP", "bdp_well")]
        )
        self.assertEqual(entity["business_keys"], ["uwi"])
        self.assertEqual(
            entity["attributes"][0]["mapping"],
            {"AVM": "nombre", "BDP": "well_name"}
        )
        self.assertEqual(
            entity["attributes"][0]["priority"],
            ["AVM", "BDP"]
        )
        self.assertTrue(
            all(source["watermark"]["column"] == "fecha_actualizacion"
                for source in entity["sources"])
        )

    def test_framework_rejects_lakehouse_names_and_unknown_properties(self):
        invalid = framework_config()
        invalid["lakehouse"] = "project-specific-lakehouse"

        with self.assertRaises(ConfigurationError):
            FrameworkConfig(invalid)

        invalid = framework_config()
        invalid["unexpected"] = True
        with self.assertRaises(ConfigurationError):
            FrameworkConfig(invalid)

    def test_framework_rejects_invalid_modes_and_missing_paths(self):
        invalid = framework_config()
        invalid["execution"]["default_mode"] = "TRUNCATE"
        with self.assertRaises(ConfigurationError):
            FrameworkConfig(invalid)

        invalid = framework_config()
        del invalid["paths"]["registry"]
        with self.assertRaises(ConfigurationError):
            FrameworkConfig(invalid)

    def test_yaml_loader_rejects_duplicate_keys_at_nested_levels(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.yml"
            path.write_text(
                "framework:\n  execution:\n    default_mode: UPSERT\n"
                "    default_mode: FULL_LOAD\n",
                encoding="utf-8"
            )

            with self.assertRaisesRegex(ConfigurationError, "duplicate key"):
                read_yaml_file(str(path))

    def test_yaml_loader_rejects_empty_and_non_object_documents(self):
        with tempfile.TemporaryDirectory() as directory:
            empty_path = Path(directory) / "empty.yml"
            empty_path.write_text("", encoding="utf-8")
            with self.assertRaises(ConfigurationError):
                read_yaml_file(str(empty_path))

            list_path = Path(directory) / "list.yml"
            list_path.write_text("- item\n", encoding="utf-8")
            with self.assertRaises(ConfigurationError):
                read_yaml_file(str(list_path))

    def test_registry_lists_only_enabled_entities_and_resolves_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "registry.yml"
            registry_path.write_text(
                "canonical_model:\n"
                "  name: test_model\n"
                "  version: '1.0.0'\n"
                "  entities:\n"
                "    - name: enabled_entity\n"
                "      enabled: true\n"
                "      config: entities/enabled_entity.yml\n"
                "    - name: disabled_entity\n"
                "      enabled: false\n"
                "      config: entities/disabled_entity.yml\n",
                encoding="utf-8"
            )
            (root / "entities").mkdir()

            registry = CanonicalRegistry(str(registry_path))

            self.assertEqual(
                [item["name"] for item in registry.get_enabled_entities()],
                ["enabled_entity"]
            )
            self.assertEqual(
                Path(registry.get_entity_config_path("enabled_entity")),
                root / "entities" / "enabled_entity.yml"
            )

    def test_registry_rejects_duplicate_entity_names(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.yml"
            path.write_text(
                "canonical_model:\n"
                "  name: test_model\n"
                "  version: '1.0.0'\n"
                "  entities:\n"
                "    - name: duplicate\n"
                "      enabled: true\n"
                "      config: entities/one.yml\n"
                "    - name: duplicate\n"
                "      enabled: false\n"
                "      config: entities/two.yml\n",
                encoding="utf-8"
            )

            with self.assertRaises(RegistryError):
                CanonicalRegistry(str(path))

    def test_example_notebooks_are_valid_json_with_parseable_code(self):
        notebooks = sorted(Path("examples/notebooks").glob("*.ipynb"))
        notebooks.extend(sorted(Path("notebooks").glob("*.ipynb")))
        self.assertGreaterEqual(len(notebooks), 3)

        for path in notebooks:
            with self.subTest(notebook=path.name):
                notebook = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(notebook["nbformat"], 4)
                self.assertTrue(notebook["cells"])
                for index, cell in enumerate(notebook["cells"]):
                    if cell["cell_type"] == "code":
                        ast.parse(
                            "".join(cell["source"]),
                            filename=f"{path}[{index}]"
                        )

    def test_operational_notebook_has_one_parameter_cell(self):
        path = Path("notebooks/nb_create_canonical_model.ipynb")
        notebook = json.loads(path.read_text(encoding="utf-8"))
        parameter_cells = [
            cell for cell in notebook["cells"]
            if "parameters" in cell.get("metadata", {}).get("tags", [])
        ]

        self.assertEqual(len(parameter_cells), 1)
        parameter_source = "".join(parameter_cells[0]["source"])
        for parameter in (
            "LAKEHOUSE_ROOT",
            "CONFIG_RELATIVE_ROOT",
            "ENTITY_NAME",
            "EXECUTION_MODE",
            "PRECHECK_ONLY"
        ):
            self.assertIn(parameter, parameter_source)


if __name__ == "__main__":
    unittest.main()
