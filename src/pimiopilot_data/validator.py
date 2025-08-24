import json
from pathlib import Path
from jsonschema import Draft202012Validator, validators
import yaml

def _extend_with_default(validator_class):
    validate_props = validator_class.VALIDATORS["properties"]
    def set_defaults(validator, properties, instance, schema):
        for prop, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(prop, subschema["default"])
        for error in validate_props(validator, properties, instance, schema):
            yield error
    return validators.extend(validator_class, {"properties": set_defaults})

DefaultFillingValidator = _extend_with_default(Draft202012Validator)

def load_and_validate(config_path: str | Path, schema_path: str | Path) -> dict:
    cfg = yaml_safe_load(config_path)
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    v = DefaultFillingValidator(schema)
    v.validate(cfg)
    return cfg

def yaml_safe_load(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
