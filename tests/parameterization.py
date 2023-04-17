from bw2io.strategies.parameterization import variable_subtitutor


def test_yield_in_formula():
    given = "foo * yield + yielder / yield_one + two_yield + 2*yield/foo"
    expected = "foo * YIELD + yielder / yield_one + two_yield + 2*YIELD/foo"
    assert variable_subtitutor.fix_formula(given) == expected


def test_yield_as_variable():
    assert variable_subtitutor.fix_variable_name("Yield") == "Yield"
    assert variable_subtitutor.fix_variable_name("yield") == "YIELD"
    assert variable_subtitutor.fix_variable_name("yield_") == "yield_"


def test_yield_at_beginning():
    given = "yield*foo"
    expected = "YIELD*foo"
    assert variable_subtitutor.fix_formula(given) == expected


def test_yield_at_end():
    given = "foo*yield"
    expected = "foo*YIELD"
    assert variable_subtitutor.fix_formula(given) == expected
