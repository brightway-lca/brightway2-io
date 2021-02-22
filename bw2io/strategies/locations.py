GEO_UPDATE = {
    "Al producing Area 2, North America": "IAI Area, North America",
    "IAI Area 2, North America": "IAI Area, North America",
    "IAI Area, North America, without Quebec": "IAI Area, North America, without Quebec",
    "IAI Area 1": "IAI Area, Africa",
    "IAI Area 3": "IAI Area, South America",
    "IAI Area 4&5 without China": "IAI Area, Asia, without China and GCC",
    "IAI Area 6A": "IAI Area, West Europe",
    "IAI Area, Europe outside EU & EFTA": "IAI Area, Russia & RER w/o EU27 & EFTA",
    "IAI Area 8": "IAI Area, Gulf Cooperation Council",
    "Ashmore and Cartier Islands": "AUS-AC",
    "Indian Ocean Territories": "AUS-IOT",
    "CC": "AUS-IOT",
    "CX": "AUS-IOT",
    "ROC": "Canada without Quebec",
    "MRO, US only": "US-MRO",
    "NPCC, US only": "US-NPCC",
    "WECC, US only": "US-WECC",
    "CSG": "CN-CSG",
    "SGCC": "CN-SGCC",
    "FRCC": "US-FRCC",
    "HICC": "US-HICC",
    "RFC": "US-RFC",
    "SERC": "US-SERC",
    "SPP": "US-SPP",
    "TRE": "US-TRE",
    "ASCC": "US-ASCC",
}


def update_ecoinvent_locations(db):
    """Update old ecoinvent location codes"""
    for ds in db:
        if "location" in ds:
            ds["location"] = GEO_UPDATE.get(ds["location"], ds["location"])
        for exc in ds.get("exchanges", []):
            if "location" in exc:
                exc["location"] = GEO_UPDATE.get(exc["location"], exc["location"])
    return db
