import yaml
from jsonschema import Draft7Validator, RefResolver, validators

API_FILENAME = "probeinfo_api.yaml"
with open(API_FILENAME, "r") as f:
    API = yaml.load(f, Loader=yaml.SafeLoader)
SCHEMAS = API["components"]["schemas"]
RESOLVER = RefResolver("", API)


def extend_with_default(validator_class):
    """
    Apply default values from the schema when not present.

    See https://python-jsonschema.readthedocs.io/en/stable/faq/
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator,
            properties,
            instance,
            schema,
        ):
            yield error

    return validators.extend(
        validator_class,
        {"properties": set_defaults},
    )


Validator = extend_with_default(Draft7Validator)


def validate_as(instance, model_name):
    schema = SCHEMAS[model_name]
    Draft7Validator(schema, resolver=RESOLVER).validate(instance)


def apply_defaults_and_validate(instance, model_name):
    schema = SCHEMAS[model_name]
    Validator(schema, resolver=RESOLVER).validate(instance)
    # Send through validation again to be sure any inject default values
    # still validate with the schema.
    validate_as(instance, model_name)
