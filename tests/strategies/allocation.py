import unittest

from bw2io.strategies import es1_allocate_multioutput


def test_allocation():
    data = [
        {
            "exchanges": [
                {
                    "type": "production",
                    "code": "p1",
                    "amount": 1,
                },
                {
                    "type": "production",
                    "code": "p2",
                    "amount": 2,
                },
                {
                    "type": "emission",
                    "code": "e1",
                    "amount": 10,
                },
                {
                    "type": "technosphere",
                    "code": "t1",
                    "amount": 20,
                },
            ],
            "allocations": [
                {
                    "exchanges": ["e1", "t1"],
                    "fraction": 50.0,
                    "reference": "p1",
                },
                {
                    "exchanges": ["e1"],
                    "fraction": 10.0,
                    "reference": "p2",
                },
                {
                    "exchanges": ["t1"],
                    "fraction": 100.0,
                    "reference": "p2",
                },
            ],
        }
    ]
    expected = [
        {
            "exchanges": [
                {"amount": 10 * 0.5, "code": "e1", "type": "emission", 'loc': 5},
                {"amount": 1, "code": "p1", "type": "production"},
                {"amount": 20 * 0.5, "code": "t1", "type": "technosphere", 'loc': 10},
            ],
        },
        {
            "exchanges": [
                {"amount": 10 * 0.1, "code": "e1", "type": "emission", "loc": 1},
                {"amount": 2, "code": "p2", "type": "production"},
                {"amount": 20 * 1.0, "code": "t1", "type": "technosphere", "loc": 20},
            ],
        },
    ]
    answer = es1_allocate_multioutput(data)
    answer[0]["exchanges"].sort(key=lambda x: x["type"])
    answer[1]["exchanges"].sort(key=lambda x: x["type"])
    assert answer == expected
