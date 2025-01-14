import math

import pytest
from stats_arrays import (
    LognormalUncertainty,
    NormalUncertainty,
    NoUncertainty,
    TriangularUncertainty,
    UndefinedUncertainty,
    UniformUncertainty,
)

from bw2io.errors import UnsupportedExchange
from bw2io.utils import (
    activity_hash,
    es2_activity_hash,
    format_for_logging,
    load_json_data_file,
    rescale_exchange,
    standardize_method_to_len_3,
)


def test_load_json_data():
    assert load_json_data_file("test") == {"1": {"2": 3}}
    assert load_json_data_file("test.json") == {"1": {"2": 3}}


def test_activity_hash():
    assert activity_hash({}) == "d41d8cd98f00b204e9800998ecf8427e"
    assert activity_hash({}) == "d41d8cd98f00b204e9800998ecf8427e"
    assert isinstance(activity_hash({}), str)

    ds = {"name": "care bears", "unit": "kilogram", "location": "GLO"}
    assert activity_hash(ds) == "a6d6dd46cc33acd23826fa5b4e83377f"

    ds = {
        "name": "care bears",
        "categories": ["toys", "fun"],
        "unit": "kilogram",
        "reference product": "lollipops",
        "location": "GLO",
        "extra": "irrelevant",
    }
    assert activity_hash(ds) == "90d8689ec08dceb9507d28a36df951cd"

    ds = {"name": "正しい馬のバッテリーの定番"}
    assert activity_hash(ds) == "d2b18b4f9f9f88189c82224ffa524e93"


def test_format_for_logging():
    ds = {"name": "care bears", "unit": "kilogram", "location": "GLO"}
    answer = "{'location': 'GLO', 'name': 'care bears', 'unit': 'kilogram'}"
    assert format_for_logging(ds) == answer


def test_es2_activity_hash():
    ds = ("foo", "bar")
    assert es2_activity_hash(*ds) == "3858f62230ac3c915f300c664312c63f"
    assert isinstance(es2_activity_hash(*ds), str)

    ds = ("正しい馬", "バッテリーの定番")
    assert es2_activity_hash(*ds) == "008e9536b44699d8b0d631d9acd76515"


def test_standardize_method_to_len_3():
    a = ("foo", "bar")
    b = ()
    c = tuple("abcde")
    d = tuple("abc")
    e = list("ab")
    f = list("abcde")

    assert standardize_method_to_len_3(a) == ("foo", "bar", "--")
    assert standardize_method_to_len_3(a, "##") == ("foo", "bar", "##")
    assert standardize_method_to_len_3(b) == ("--", "--", "--")
    assert standardize_method_to_len_3(c) == ("a", "b", "c,d,e")
    assert standardize_method_to_len_3(c, joiner="; ") == ("a", "b", "c; d; e")
    assert standardize_method_to_len_3(d) == ("a", "b", "c")
    assert standardize_method_to_len_3(e) == ("a", "b", "--")
    assert standardize_method_to_len_3(f) == ("a", "b", "c,d,e")


def test_rescale_exchange_nonnumber():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    with pytest.raises(ValueError):
        rescale_exchange(given, True)
    with pytest.raises(ValueError):
        rescale_exchange(given, "forty two")


def test_rescale_exchange_unsupported_exchange():
    given = {
        "uncertainty type": 999,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    with pytest.raises(UnsupportedExchange):
        rescale_exchange(given, 7.14)


def test_rescale_exchange_zero():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    expected = {"amount": 0.0, "loc": 0.0, "uncertainty type": UndefinedUncertainty.id}
    assert rescale_exchange(given, 0) == expected


def test_rescale_exchange_formula():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
        "formula": "foo * 7 + bar",
    }
    assert rescale_exchange(given, 3)["formula"] == "(foo * 7 + bar) * 3"


def test_rescale_exchange_distribution_not_given():
    given = {
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    expected = {
        "minimum": 0,
        "maximum": 20,
        "amount": 4,
        "loc": 4,
    }
    assert rescale_exchange(given, 2) == expected


def test_rescale_exchange_no_uncertainty():
    given = {
        "uncertainty type": NoUncertainty.id,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    expected = {
        "uncertainty type": NoUncertainty.id,
        "minimum": 0,
        "maximum": 20,
        "amount": 4,
        "loc": 4,
    }
    assert rescale_exchange(given, 2) == expected


def test_rescale_exchange_flip_min_max():
    given = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": 0,
        "maximum": 10,
        "amount": 2,
    }
    expected = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": -20.0,
        "maximum": 0.0,
        "amount": -4.0,
        "loc": -4.0,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_min_to_max():
    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "minimum": 1,
        "amount": 2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "maximum": -2.0,
        "amount": -4.0,
        "loc": -4.0,
    }
    assert rescale_exchange(given, -2) == expected

    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "minimum": -10,
        "amount": -2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "maximum": 20.0,
        "amount": 4.0,
        "loc": 4.0,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_max_to_min():
    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "maximum": 10,
        "amount": 2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "minimum": -20.0,
        "amount": -4.0,
        "loc": -4.0,
    }
    assert rescale_exchange(given, -2) == expected

    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "maximum": -1,
        "amount": -2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "minimum": 2.0,
        "amount": 4.0,
        "loc": 4.0,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_normal_distribution():
    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "amount": -2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "amount": 4.0,
        "loc": 4.0,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_normal_distribution_scale_always_positive():
    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "amount": 2,
    }
    expected = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 1.0,
        "amount": -4.0,
        "loc": -4.0,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_lognormal_distribution():
    given = {
        "uncertainty type": LognormalUncertainty.id,
        "scale": 0.5,
        "amount": -2,
        "negative": True,
    }
    expected = {
        "uncertainty type": LognormalUncertainty.id,
        "scale": 0.5,
        "amount": 4.0,
        "loc": math.log(4),
        "negative": False,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_lognormal_distribution_turn_negative():
    given = {
        "uncertainty type": LognormalUncertainty.id,
        "scale": 0.5,
        "amount": 2,
    }
    expected = {
        "uncertainty type": LognormalUncertainty.id,
        "scale": 0.5,
        "amount": -4.0,
        "loc": math.log(4),
        "negative": True,
    }
    assert rescale_exchange(given, -2) == expected


def test_rescale_exchange_remove_negative():
    given = {
        "uncertainty type": NormalUncertainty.id,
        "scale": 0.5,
        "amount": 2,
        "negative": True,
    }
    assert "negative" not in rescale_exchange(given, -2)


def test_rescale_exchange_triangular():
    given = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": 1,
        "amount": 2,
        "maximum": 3,
    }
    expected = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": 10,
        "amount": 20,
        "loc": 20,
        "maximum": 30,
    }
    assert rescale_exchange(given, 10) == expected


def test_rescale_exchange_triangular_flip_sign_from_positive():
    given = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": 1,
        "amount": 2,
        "maximum": 3,
    }
    expected = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": -30,
        "amount": -20,
        "loc": -20,
        "maximum": -10,
    }
    assert rescale_exchange(given, -10) == expected


def test_rescale_exchange_triangular_flip_sign_from_negative():
    given = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": -3,
        "amount": -2,
        "maximum": -1,
    }
    expected = {
        "uncertainty type": TriangularUncertainty.id,
        "minimum": 10,
        "amount": 20,
        "loc": 20,
        "maximum": 30,
    }
    assert rescale_exchange(given, -10) == expected


def test_rescale_exchange_uniform():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 1,
        "amount": 2,
        "maximum": 3,
    }
    expected = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 10,
        "amount": 20,
        "loc": 20,
        "maximum": 30,
    }
    assert rescale_exchange(given, 10) == expected


def test_rescale_exchange_uniform_flip_sign_from_positive():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 1,
        "amount": 2,
        "maximum": 3,
    }
    expected = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": -30,
        "amount": -20,
        "loc": -20,
        "maximum": -10,
    }
    assert rescale_exchange(given, -10) == expected


def test_rescale_exchange_uniform_flip_sign_from_negative():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": -3,
        "amount": -2.5,
        "maximum": -1,
    }
    expected = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 10,
        "amount": 25,
        "loc": 25,
        "maximum": 30,
    }
    assert rescale_exchange(given, -10) == expected


def test_rescale_exchange_uniform_no_amount():
    given = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 1,
        "maximum": 3,
    }
    expected = {
        "uncertainty type": UniformUncertainty.id,
        "minimum": 10,
        "amount": 20,
        "loc": 20,
        "maximum": 30,
    }
    assert rescale_exchange(given, 10) == expected
