import asteval
import re


RESERVED = {
    "and", "as", "assert", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "False", "finally", "for", "from", "global", "if",
    "import", "in", "is", "lambda", "None", "nonlocal", "not", "or", "pass",
    "raise", "return", "True", "try", "while", "with", "yield"
}


class ReservedVariableNameSubstitutor():
    def __init__(self):
        reserved_pattern_template = "(^|[^A-Za-z_]){}([^A-Za-z_]|$)"
        reserved_substitution_template = r"\1{}\2"

        self.symbols = set(asteval.make_symbol_table()).union(RESERVED)
        self.matches = [
            (
                re.compile(reserved_pattern_template.format(symbol)),
                reserved_substitution_template.format(symbol.upper()),
            )
            for symbol in self.symbols
        ]

    def fix_formula(self, string):
        for pattern, substitution in self.matches:
            string = pattern.sub(substitution, string)
        return string

    def fix_variable_name(self, string):
        string = string.strip()
        if string in self.symbols:
            return string.upper()
        else:
            return string


variable_subtitutor = ReservedVariableNameSubstitutor()
