from voluptuous import Schema, Required, Any, Optional

bw2package_validator = Schema(
    {
        Required("metadata"): {str: object},
        Required("name"): Any(str, tuple, list),
        "class": {
            Required("module"): str,
            Required("name"): str,
            "unrolled dict": bool,
        },
        Optional("unrolled_dict"): bool,
        Required("data"): object,
    }
)
