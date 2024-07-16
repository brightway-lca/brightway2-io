from pathlib import Path

from bw2io.extractors.ecospold1 import Ecospold1DataExtractor

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ecospold1"

EXPECTED = {
    "tags": [
        ("ecoSpold01datasetRelatesToProduct", True),
        ("ecoSpold01infrastructureProcess", False),
        ("ecoSpold01infrastructureIncluded", False),
        (
            "ecoSpold01localName",
            "Acrylonitrile-butadiene-styrene copolymer resin, at plant, CTR",
        ),
        ("ecoSpold01localCategory", "Chemical Manufacturing"),
        ("ecoSpold01localSubCategory", "All other basic organic manufacturing"),
        ("ecoSpold01category", "Chemical Manufacturing"),
        ("ecoSpold01subCategory", "All other basic organic manufacturing"),
        (
            "ecoSpold01includedProcesses",
            "Includes material and energy requirements and environmental emissions for one kilogram of acrylonitrile-butadiene-styrene copolymer resin, ABS production.",
        ),
        ("ecoSpold01dataValidForEntirePeriod", True),
        ("ecoSpold01endDate", "2004-12-31"),
        ("ecoSpold01startDate", "2003-01-01"),
        ("ecoSpold01type", 1),
        ("ecoSpold01impactAssessmentResult", False),
        ("ecoSpold01version", "1"),
        ("ecoSpold01internalVersion", "1.0"),
        ("ecoSpold01timestamp", "2013-11-13T17:26:25"),
        ("ecoSpold01languageCode", "en"),
        ("ecoSpold01localLanguageCode", "de"),
        ("ecoSpold01energyValues", 2),
    ],
    "references": [
        {
            "identifier": 1,
            "type": "Seperate publication",
            "authors": ["THE PLASTICS DIVISION OF \nTHE ACC\n", "Franklin Associates"],
            "year": 2011,
            "title": "CRADLE-TO-GATE LIFE CYCLE INVENTORY OF NINE PLASTIC RESINS AND FOUR POLYURETHANE PRECURSORS",
            "pages": "",
            "editors": "",
            "anthology": "",
            "place_of_publication": 'See "Text"',
            "publisher": "",
            "journal": "",
            "volume": 0,
            "issue": "",
            "text": "http://www.americanchemistry.com/s_plastics/sec_content.asp?CID=1593&DID=6056",
        }
    ],
    "categories": ["Chemical Manufacturing", "All other basic organic manufacturing"],
    "code": 1,
    "comment": "Scrap and heat are produced as coproducts during this process. A mass basis was used to partition the credit for scrap, while the energy amount for the heat is reported separately as recovered energy. Complete inventory data and metadata are available in full in the final report and appendices, Cradle-to-Gate Life Cycle Inventory of Nine Plastic Resins and Four Polyurethane Precursors. This report has been extensively reviewed within Franklin Associates and has undergone partial critical review by ACC Plastics Division members and is available at: www.americanchemistry.com. Quantities may vary slightly between the main source and this module due to rounding.  Important note: although most of the data in the US LCI database has undergone some sort of review, the database as a whole has not yet undergone a formal validation process.  Please email comments to lci@nrel.gov.\nIncludes material and energy requirements and environmental emissions for one kilogram of acrylonitrile-butadiene-styrene copolymer resin, ABS production.\nLocation: US and Mexico\nTechnology: Suspension and mass polymerization\nTime period: \nProduction volume: 0.5\nSampling: Data are from both primary and secondary sources.  Data for the production of ABS were provided by three leading producers (5 plants) in North America.  The ABS producers who provided data for this module verified that the characteristics of their plants are representative of a majority of North American ABS production.\nExtrapolations: \nUncertainty adjustments: ",
    "comments": {
        "generalComment": "Scrap and heat are produced as coproducts during this process. A mass basis was used to partition the credit for scrap, while the energy amount for the heat is reported separately as recovered energy. Complete inventory data and metadata are available in full in the final report and appendices, Cradle-to-Gate Life Cycle Inventory of Nine Plastic Resins and Four Polyurethane Precursors. This report has been extensively reviewed within Franklin Associates and has undergone partial critical review by ACC Plastics Division members and is available at: www.americanchemistry.com. Quantities may vary slightly between the main source and this module due to rounding.  Important note: although most of the data in the US LCI database has undergone some sort of review, the database as a whole has not yet undergone a formal validation process.  Please email comments to lci@nrel.gov.",
        "includedProcesses": "Includes material and energy requirements and environmental emissions for one kilogram of acrylonitrile-butadiene-styrene copolymer resin, ABS production.",
        "location": "Location: US and Mexico",
        "technology": "Technology: Suspension and mass polymerization",
        "timePeriod": "Time period: ",
        "productionVolume": "Production volume: 0.5",
        "sampling": "Sampling: Data are from both primary and secondary sources.  Data for the production of ABS were provided by three leading producers (5 plants) in North America.  The ABS producers who provided data for this module verified that the characteristics of their plants are representative of a majority of North American ABS production.",
        "extrapolations": "Extrapolations: ",
        "uncertaintyAdjustments": "Uncertainty adjustments: ",
    },
    "authors": {
        "data_entry": {
            "address": "Prairie Village, KS",
            "company": "FA-ERG",
            "country": "US",
            "email": "melissa.huff@erg.com",
            "name": "Franklin Associates",
            "identifier": 1,
        },
        "people": [
            {
                "address": "Prairie Village, KS",
                "company": "FA-ERG",
                "country": "US",
                "email": "melissa.huff@erg.com",
                "name": "Franklin Associates",
                "identifier": 1,
            }
        ],
    },
    "database": "foo",
    "exchanges": [
        {
            "code": 1,
            "categories": ("resource", "Unspecified"),
            "location": "",
            "unit": "kg",
            "name": "Coal, lignite, in ground",
            "type": "biosphere",
            "infrastructureProcess": False,
            "uncertainty type": 0,
            "amount": 0.034028,
            "loc": 0.034028,
        }
    ],
    "filename": "Acrylonitrile-butadiene-styrene copolymer (ABS), resin, at plant CTR.xml",
    "location": "RNA",
    "name": "Acrylonitrile-butadiene-styrene copolymer resin, at plant, CTR",
    "unit": "kg",
    "type": "process",
}


def test_ecospold1_extractor():
    ei = Ecospold1DataExtractor.process_file(
        FIXTURES
        / "Acrylonitrile-butadiene-styrene copolymer (ABS), resin, at plant CTR.xml",
        "foo",
    )[0]
    for key, value in ei.items():
        if key == "exchanges":
            continue
        assert value == EXPECTED[key]
    assert EXPECTED["exchanges"][0] == ei["exchanges"][0]
