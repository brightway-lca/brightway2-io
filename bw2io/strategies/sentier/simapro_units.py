from functools import lru_cache
from typing import List, Optional

from bw2data.logs import get_logger
from SPARQLWrapper import JSON, SPARQLWrapper

from ...utils import activity_hash, rescale_exchange

logger = get_logger("io-vocab.sentier.dev.log")


class SimaProUnitConverter:
    def __init__(
        self,
        sparql_url: str = "https://fuseki.d-d-s.ch/skosmos/query",
        simapro_graph: str = "https://vocab.sentier.dev/simapro/",
        qudt_graph: str = "https://vocab.sentier.dev/qudt/",
        ecoinvent_uri: str = "https://glossary.ecoinvent.org/",
        qk_uri: str = "https://vocab.sentier.dev/qudt/quantity-kind/",
    ):
        self.simapro_graph = simapro_graph
        self.qudt_graph = qudt_graph
        self.qk_uri = qk_uri
        self.ecoinvent_uri = ecoinvent_uri
        self.data_cache = {}
        self.sparql = SPARQLWrapper(sparql_url)
        self.sparql.setReturnFormat(JSON)
        self.populate_qudt_cache()

    def uri_for_unit_string(self, unit: str) -> str:
        return self.simapro_graph + "unit/" + unit

    def unit_string_from_uri(self, uri: str) -> str:
        return uri.replace(self.simapro_graph + "unit/", "")

    @lru_cache(maxsize=512)
    def get_simapro_conversions(self, unit: str, qk: Optional[str] = None) -> list:
        uri = self.uri_for_unit_string(unit)
        logger.info(f"SimaPro URI: {uri}")
        applicable = [
            elem
            for elem in self.data_cache
            if elem.get("simapro") == uri and "factor" in elem
        ]
        logger.debug("Applicable conversions: {a}", a=applicable)
        if qk is not None:
            applicable = [elem for elem in applicable if elem.get("qk") == qk]
        if not applicable:
            return []

        qk_reference_values = {elem["qk"]: elem["factor"] for elem in applicable}
        logger.debug(f"Reference conversion factors: {qk_reference_values}")
        results = []

        for elem in self.data_cache:
            if (
                elem["qk"] in qk_reference_values
                and "simapro" in elem
                and "factor" in elem
                and elem["simapro"] != uri
            ):
                results.append(
                    {
                        "qk": elem["qk"],
                        "factor": qk_reference_values[elem["qk"]] / elem["factor"],
                        "unit": self.unit_string_from_uri(elem["simapro"]),
                    }
                )

        return results

    def populate_qudt_cache(self) -> None:
        logger.info(
            "Retrieving data for all QUDT units, quantity kinds, and conversions"
        )
        QUERY = f"""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT ?quantitykind ?qudt ?conversion
FROM <{self.qudt_graph}>
WHERE {{
    ?quantitykind a skos:Concept .
    ?quantitykind skos:inScheme <{self.qudt_graph}> .
    FILTER (
      contains(STR(?quantitykind), "{self.qk_uri}")
    )
    OPTIONAL {{
      ?quantitykind skos:narrowerTransitive ?qudt .
      ?qudt a skos:Concept .
      ?qudt qudt:conversionMultiplier ?conversion .
    }}
}}
        """
        self.sparql.setQuery(QUERY)
        logger.debug(f"Executing query:\n{QUERY}")
        results = self.sparql.queryAndConvert()["results"]["bindings"]
        self.data_cache = [
            {
                "qudt": row["qudt"]["value"],
                "factor": float(row["conversion"]["value"]),
                "qk": row["quantitykind"]["value"],
            }
            for row in results
            if "qudt" in row
        ]

        logger.info("Retrieving data on SimaPro equivalencies")
        QUERY = f"""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT ?simapro ?qudt
FROM <{self.simapro_graph}>
FROM <{self.qudt_graph}>
WHERE {{
    ?simapro a skos:Concept .
    ?simapro skos:inScheme <{self.simapro_graph}> .
    ?simapro skos:exactMatch ?qudt .
    ?qudt a skos:Concept .
    ?qudt skos:inScheme <{self.qudt_graph}> .
}}
        """
        self.sparql.setQuery(QUERY)
        logger.debug(f"Executing query:\n{QUERY}")
        results = {
            row["qudt"]["value"]: row["simapro"]["value"]
            for row in self.sparql.queryAndConvert()["results"]["bindings"]
        }
        for row in self.data_cache:
            try:
                row["simapro"] = results[row["qudt"]]
            except KeyError:
                continue

        logger.info("Retrieving data on ecoinvent equivalencies")
        QUERY = f"""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT ?qudt ?ecoinvent
FROM <{self.qudt_graph}>
WHERE {{
    ?qudt a skos:Concept .
    ?qudt skos:inScheme <{self.qudt_graph}> .
    ?qudt skos:exactMatch ?ecoinvent .
    FILTER (
      contains(STR(?ecoinvent), "{self.ecoinvent_uri}")
    )
}}
        """
        self.sparql.setQuery(QUERY)
        logger.debug(f"Executing query:\n{QUERY}")
        results = {
            row["qudt"]["value"]: row["ecoinvent"]["value"]
            for row in self.sparql.queryAndConvert()["results"]["bindings"]
        }
        for row in self.data_cache:
            try:
                row["ecoinvent"] = results[row["qudt"]]
            except KeyError:
                continue


def match_internal_simapro_simapro_with_unit_conversion(
    data: list, type: str = "technosphere", fields: Optional[List[str]] = None
) -> list:
    spuc = SimaProUnitConverter()

    if not fields:
        fields = ["name", "location", "unit"]

    lookup = {activity_hash(ds, fields): (ds["database"], ds["code"]) for ds in data}

    for ds in data:
        for exc in filter(lambda x: not x.get("input"), ds.get("exchanges", [])):
            for conversion in spuc.get_simapro_conversions(exc.get("unit", "")):
                try:
                    exc["input"] = lookup[
                        activity_hash(
                            {key: value for key, value in exc.items() if key in fields}
                            | {"unit": conversion["unit"]},
                            fields,
                        )
                    ]
                    exc["unit"] = conversion["unit"]
                    rescale_exchange(exc, conversion["factor"])
                except KeyError:
                    continue

    return data
