import json
from numbers import Number
from pathlib import Path
from urllib.parse import quote_plus

import requests

DIRPATH = Path(__file__).parent.resolve() / "data"


def canonical_cas(s):
    """
    CAS numbers have up to ten digits; we remove zero padding and add hyphens where needed.

    Parameters
    ----------
    s : str
        CAS number.

    Returns
    -------
    str
        Canonical CAS number.
    """
    if isinstance(s, Number):
        # Remove ".0" from string conversion
        s = int(s)
    if s in ("None", None) or not s:
        return
    try:
        s = str(int(str(s).replace("-", "")))
    except ValueError:
        # Dirty data
        return
    # TODO: Verify check number?
    return "{}-{}-{}".format(s[:-3], s[-3:-1], s[-1])


class Multiple(Exception):
    """
    Multiple results for given search query.

    Parameters
    ----------
    exception : Exception
        Exception to raise.
    """
    pass


class Missing(Exception):
    """
    404 or other error code returned.

    Parameters
    ----------
    exception : Exception
        Exception to raise.
    """

    pass


class ChemIDPlus:
    """
    Use the `ChemIDPlus <https://chem.nlm.nih.gov/api/swagger-ui.html#/SubstanceController>`__ API to lookup synonyms for chemicals, including pesticides.

    Always used to match against a master list. Seeded with names from ecoinvent.

    Attributes
    ----------
    api_cache : dict
        Dictionary with raw data from API, key is canonical name.
    master_mapping : dict
        Dictionary from synonyms, including canonical names, to master flows.
    forbidden_keys : set
        Identifiers that aren't unique in the ChemIDPlus system.

    Methods
    -------
    match(synonym, search=True)
        Match a synonym to a master flow.
    match_cas(number)
        Match a CAS number to a master flow.
    process_request(request)
        Process a request to the ChemIDPlus API.
    load_cache()
        Load the cache of API results.
    save_cache()
        Save the cache of API results.
    """

    CAS_TEMPLATE = (
        "https://chem.nlm.nih.gov/api/data/search?data=complete&exp=rn%2Feq%2F{cas}"
    )
    NAME_TEMPLATE = (
        "https://chem.nlm.nih.gov/api/data/search?data=complete&exp=na%2Feq%2F{name}"
    )

    def __init__(self):
        # Dictionary with raw data from API, key is canonical name
        self.api_cache = {}
        # Dictionary from synonyms, including canonical names, to master flows
        self.master_mapping = {}
        # Identifiers that aren't unique in the ChemIDPlus system
        self.forbidden_keys = set()
        if (DIRPATH / "chemid_cache.json").is_file():
            self.load_cache()

    def match(self, synonym, search=True):
        synonym = str(synonym).lower()
        if synonym in self.forbidden_keys:
            return False
        try:
            return self.master_mapping[synonym]
        except KeyError:
            if not search:
                return False
            result = self.process_request(
                requests.get(self.NAME_TEMPLATE.format(name=quote_plus(synonym)))
            )
            master = self.master_mapping.get(result["canonical"].lower())
            if master:
                self.master_mapping[synonym] = master
                return master
            else:
                return False

    def match_cas(self, number):
        return self.master_mapping[canonical_cas(number)]

    def add_master_term(self, term, CAS):
        term = str(term).lower()
        if term in self.master_mapping.values():
            return
        try:
            result = self.process_request(
                requests.get(self.NAME_TEMPLATE.format(name=quote_plus(term)))
            )
        except (Missing, Multiple):
            if CAS:
                result = result = self.process_request(
                    requests.get(self.CAS_TEMPLATE.format(cas=quote_plus(CAS)))
                )
            else:
                raise Missing
        self.master_mapping[result["canonical"].lower()] = term
        self.api_cache[result["canonical"]] = result
        if result.get("CAS"):
            self.master_mapping[result["CAS"]] = term
        for synonym in result["synonyms"]:
            if synonym in self.master_mapping:
                self.forbidden_keys.add(synonym.lower())
                del self.master_mapping[synonym.lower()]
            else:
                self.master_mapping[synonym.lower()] = term

    def save_cache(self):
        data = {
            "master_mapping": self.master_mapping,
            "api_cache": self.api_cache,
            "forbidden_keys": list(self.forbidden_keys),
        }
        json.dump(
            data, open(DIRPATH / "chemid_cache.json", "w"), ensure_ascii=False, indent=2
        )

    def load_cache(self):
        data = json.load(open(DIRPATH / "chemid_cache.json"))
        self.forbidden_keys = set(data["forbidden_keys"])
        self.master_mapping = {k.lower(): v for k, v in data["master_mapping"].items()}
        self.api_cache = data["api_cache"]

    def process_request(self, response):
        if not response.status_code == 200:
            raise Missing
        data = response.json()
        if not data["total"]:
            raise Missing
        elif not data["total"] == 1:
            raise Multiple
        data = data["results"][0]
        return {
            "CAS": data["summary"].get("rn"),
            "canonical": data["summary"]["na"],
            "synonyms": sorted(
                [
                    elem["d"]
                    for obj in data["names"]
                    for elem in obj["e"]
                    if obj["t"] == 616
                ]
            ),
        }
