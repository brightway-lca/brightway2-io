from voluptuous import Schema, Required, Any, Optional

bw2package_validator = Schema({
    Required('metadata'): {basestring: object},
    Required('name'): Any(basestring, tuple, list),
    'class': {
        Required('module'): basestring,
        Required('name'): basestring,
        "unrolled dict": bool,
    },
    Optional('unrolled_dict'): bool,
    Required('data'): object
})
