from bw2io.extractors.ecospold2 import Ecospold2DataExtractor
import os

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures", "ecospold2")


def test_extraction_without_synonyms():
    data = Ecospold2DataExtractor.extract(FIXTURES, "ei")
    expected = [{
        'activity': 'c40e3c0a-292f-45a5-88cd-ed18265cb7d7',
        'activity type': 'ordinary transforming activity',
        'authors': {'data entry': {'email': 'yummy@example.org',
                                 'name': 'Don Ron Bon-Bon'},
                  'data generator': {'email': 'spicy@exaxmple.org',
                                     'name': 'Rhyme Thyme'}},
        'classifications': [('EcoSpold01Categories',
                           'construction materials/concrete'),
                          ('ISIC rev.4 ecoinvent',
                           '2395:Manufacture of articles of concrete, cement and '
                           'plaster')],
        'comment': 'Things and stuff and whatnot\n'
                 'Technology:  typical technology for ze Germans!',
        'database': 'ei',
        'exchanges': [{'activity': '759fac54-b912-4781-9833-0ddd6e8cda24',
                     'amount': 9999.0,
                     'classifications': {'CPC': ['53269: Other constructions for '
                                                 'manufacturing']},
                     'comment': 'estimated',
                     'flow': '6fe4040b-39c7-4d58-b95b-6ee1de4aedb3',
                     'loc': 0.0,
                     'name': 'clay pit infrastructure',
                     'pedigree': {'completeness': 1,
                                  'further technological correlation': 1,
                                  'geographical correlation': 1,
                                  'reliability': 1,
                                  'temporal correlation': 5},
                     'production volume': 0.0,
                     'scale': 0.4472135954999579,
                     'scale without pedigree': 0.31622776601683794,
                     'type': 'technosphere',
                     'uncertainty type': 2,
                     'unit': 'unit'},
                    {'activity': None,
                     'amount': 1.0,
                     'classifications': {'CPC': ['37510: Non-refractory mortars '
                                                 'and concretes']},
                     'flow': 'd4ee8f39-342b-4443-bbb9-c49b6801b5d6',
                     'loc': 1.0,
                     'name': 'concrete block',
                     'production volume': 42.0,
                     'type': 'production',
                     'uncertainty type': 0,
                     'unit': 'kg'},
                    {'amount': 123456.0,
                     'classifications': {'CPC': []},
                     'comment': 'Calculated value based on literature values and, '
                                'like, experts, and stuff.',
                     'flow': '075e433b-4be4-448e-9510-9a5029c1ce94',
                     'loc': 8.0,
                     'name': 'Water',
                     'pedigree': {'completeness': 2,
                                  'further technological correlation': 1,
                                  'geographical correlation': 1,
                                  'reliability': 2,
                                  'temporal correlation': 5},
                     'production volume': 0.0,
                     'scale': 2.449489742783178,
                     'scale without pedigree': 2.6457513110645907,
                     'type': 'biosphere',
                     'uncertainty type': 2,
                     'unit': 'm3'}],
        'filename': '00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000.spold',
        'location': 'DE',
        'name': 'concrete block production',
        'synonyms': [],
        'parameters': {'does_it_hurt_when_dropped_on_foot': {
            'amount': 7777.0,
            'comment': 'This is where the people type the words!!!',
            'description': "How much owwies PLEASE DON'T TELL MOM",
            'id': 'daadf2d4-7bbb-4f69-8ab5-58df4c1685eb',
            'loc': 2.0,
            'pedigree': {
                'completeness': 4,
                'further technological correlation': 4,
                'geographical correlation': 2,
                'reliability': 4,
                'temporal correlation': 3
            },
            'scale': 2.0,
            'scale without pedigree': 1.7320508075688772,
            'uncertainty type': 2,
            'unit': 'dimensionless'}},
        'type': 'process'
    }]

    assert [data[0]] == expected


def test_extraction_with_synonyms():
    data = Ecospold2DataExtractor.extract(FIXTURES, "ei")
    expected = [{
        'activity': 'c40e3c0a-292f-45a5-88cd-ed18265cb7d7',
        'activity type': 'ordinary transforming activity',
        'authors': {'data entry': {'email': 'yummy@example.org',
                                 'name': 'Don Ron Bon-Bon'},
                  'data generator': {'email': 'spicy@exaxmple.org',
                                     'name': 'Rhyme Thyme'}},
        'classifications': [('EcoSpold01Categories',
                           'construction materials/concrete'),
                          ('ISIC rev.4 ecoinvent',
                           '2395:Manufacture of articles of concrete, cement and '
                           'plaster')],
        'comment': 'Things and stuff and whatnot\n'
                 'Technology:  typical technology for ze Germans!',
        'database': 'ei',
        'exchanges': [{'activity': '759fac54-b912-4781-9833-0ddd6e8cda24',
                     'amount': 9999.0,
                     'classifications': {'CPC': ['53269: Other constructions for '
                                                 'manufacturing']},
                     'comment': 'estimated',
                     'flow': '6fe4040b-39c7-4d58-b95b-6ee1de4aedb3',
                     'loc': 0.0,
                     'name': 'clay pit infrastructure',
                     'pedigree': {'completeness': 1,
                                  'further technological correlation': 1,
                                  'geographical correlation': 1,
                                  'reliability': 1,
                                  'temporal correlation': 5},
                     'production volume': 0.0,
                     'scale': 0.4472135954999579,
                     'scale without pedigree': 0.31622776601683794,
                     'type': 'technosphere',
                     'uncertainty type': 2,
                     'unit': 'unit'},
                    {'activity': None,
                     'amount': 1.0,
                     'classifications': {'CPC': ['37510: Non-refractory mortars '
                                                 'and concretes']},
                     'flow': 'd4ee8f39-342b-4443-bbb9-c49b6801b5d6',
                     'loc': 1.0,
                     'name': 'concrete block',
                     'production volume': 42.0,
                     'type': 'production',
                     'uncertainty type': 0,
                     'unit': 'kg'},
                    {'amount': 123456.0,
                     'classifications': {'CPC': []},
                     'comment': 'Calculated value based on literature values and, '
                                'like, experts, and stuff.',
                     'flow': '075e433b-4be4-448e-9510-9a5029c1ce94',
                     'loc': 8.0,
                     'name': 'Water',
                     'pedigree': {'completeness': 2,
                                  'further technological correlation': 1,
                                  'geographical correlation': 1,
                                  'reliability': 2,
                                  'temporal correlation': 5},
                     'production volume': 0.0,
                     'scale': 2.449489742783178,
                     'scale without pedigree': 2.6457513110645907,
                     'type': 'biosphere',
                     'uncertainty type': 2,
                     'unit': 'm3'}],
        'filename': '00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000_with_synonyms.spold',
        'location': 'DE',
        'name': 'concrete block production',
        'synonyms': ['concrete slab production', 'concrete block manufacturing'],
        'parameters': {'does_it_hurt_when_dropped_on_foot': {
            'amount': 7777.0,
            'comment': 'This is where the people type the words!!!',
            'description': "How much owwies PLEASE DON'T TELL MOM",
            'id': 'daadf2d4-7bbb-4f69-8ab5-58df4c1685eb',
            'loc': 2.0,
            'pedigree': {
                'completeness': 4,
                'further technological correlation': 4,
                'geographical correlation': 2,
                'reliability': 4,
                'temporal correlation': 3
            },
            'scale': 2.0,
            'scale without pedigree': 1.7320508075688772,
            'uncertainty type': 2,
            'unit': 'dimensionless'}},
        'type': 'process'
    }]

    assert [data[1]] == expected
