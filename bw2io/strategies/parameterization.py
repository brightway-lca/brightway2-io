import asteval
import re


RESERVED = {
    "and", "as", "assert", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "False", "finally", "for", "from", "global", "if",
    "import", "in", "is", "lambda", "None", "nonlocal", "not", "or", "pass",
    "raise", "return", "True", "try", "while", "with", "yield"
}


class ReservedVariableNameSubstitutor():
    """
    A class to substitute reserved variable names in formulas with their uppercase versions.

    This class replaces reserved Python keywords, as well as built-in function names,
    with their uppercase versions in a given formula string.

    Attributes
    ----------
    symbols : set
        A set of reserved Python keywords and built-in function names.
    matches : list
        A list of tuples, where each tuple contains a compiled regular expression pattern
        and a substitution string for each reserved symbol.

    Examples
    --------
    >>> variable_substitutor = ReservedVariableNameSubstitutor()
    >>> formula = "sum = a + b + max(1, 2)"
    >>> variable_substitutor.fix_formula(formula)
    'SUM = a + b + MAX(1, 2)'

    >>> variable_name = "sum"
    >>> variable_substitutor.fix_variable_name(variable_name)
    'SUM'
    """
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
        """
        Substitute reserved variable names in a formula with their uppercase versions.

        Parameters
        ----------
        string : str
            The formula containing reserved variable names to be replaced.

        Returns
        -------
        str
            The updated formula with reserved variable names replaced with their uppercase versions.
        """
        for pattern, substitution in self.matches:
            string = pattern.sub(substitution, string)
        return string

    def fix_variable_name(self, string):
        """
        Substitute a reserved variable name with its uppercase version if necessary.

        Parameters
        ----------
        string : str
            The variable name to be checked and possibly replaced.

        Returns
        -------
        str
            The updated variable name, replaced with its uppercase version if it was a reserved variable name.
        """
        string = string.strip()
        if string in self.symbols:
            return string.upper()
        else:
            return string


variable_subtitutor = ReservedVariableNameSubstitutor()
