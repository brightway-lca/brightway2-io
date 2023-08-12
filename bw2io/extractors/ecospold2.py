import math
import multiprocessing
import os

from tqdm import tqdm
from lxml import objectify
from stats_arrays.distributions import (
    LognormalUncertainty,
    NormalUncertainty,
    TriangularUncertainty,
    UndefinedUncertainty,
    UniformUncertainty,
)


PM_MAPPING = {
    "reliability": "reliability",
    "completeness": "completeness",
    "temporalCorrelation": "temporal correlation",
    "geographicalCorrelation": "geographical correlation",
    "furtherTechnologyCorrelation": "further technological correlation",
}

ACTIVITY_TYPES = {
    0: "ordinary transforming activity",
    1: "market activity",
    2: "IO activity",
    3: "Residual activity",
    4: "production mix",
    5: "import activity",
    6: "supply mix",
    7: "export activity",
    8: "re-export activity",
    9: "correction activity",
    10: "market group",
}


def getattr2(obj, attr):
    """
    Get attribute of an object; return empty dict if AttributeError occurs.

    Parameters
    ----------
    obj : object
        The object to get attribute from.
    attr : str
        The name of the attribute to get.

    Returns
    -------
    dict
        The attribute value if it exists, else an empty dict.

    """
    try:
        return getattr(obj, attr)
    except:
        return {}


TOO_LOW = """Lognormal scale value at or below zero: {}.
Reverting to undefined uncertainty."""
TOO_HIGH = """Lognormal scale value impossibly high: {}.
Reverting to undefined uncertainty."""


class Ecospold2DataExtractor(object):
    @classmethod
    def extract_technosphere_metadata(cls, dirpath):
        """
        Extract technosphere metadata from ecospold2 directory.

        Parameters
        ----------
        dirpath : str
            The path to the ecospold2 directory.
        
        Returns
        -------
        List of dict
            List of names, units, and IDs
        """
        def extract_metadata(o):
            return {"name": o.name.text, "unit": o.unitName.text, "id": o.get("id")}

        fp = os.path.join(dirpath, "IntermediateExchanges.xml")
        assert os.path.exists(fp), "Can't find IntermediateExchanges.xml"
        root = objectify.parse(open(fp, encoding="utf-8")).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract(cls, dirpath, db_name, use_mp=True):
        """
        Extract data from all ecospold2 files in a directory.

        Parameters
        ----------
        dirpath : str
            The path to the directory containing the ecospold2 files.
        db_name : str
            The name of the database to create.
        use_mp : bool, optional
            Whether to use multiprocessing to extract the data (default is True).

        Returns
        -------
        list
            A list of the extracted data from the ecospold2 files.

        Raises
        ------
        FileNotFoundError
            If no .spold files are found in the directory.

        """
        assert os.path.exists(dirpath)
        if os.path.isdir(dirpath):
            filelist = [
                filename
                for filename in os.listdir(dirpath)
                if os.path.isfile(os.path.join(dirpath, filename))
                and filename.split(".")[-1].lower() == "spold"
            ]
        elif os.path.isfile(dirpath):
            filelist = [dirpath]
        else:
            raise OSError("Can't understand path {}".format(dirpath))

        if len(filelist) == 0:
            raise FileNotFoundError(f"No .spold files found. Please check the path and try again: {dirpath}")

        if use_mp:
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                print("Extracting XML data from {} datasets".format(len(filelist)))
                results = [
                    pool.apply_async(
                        Ecospold2DataExtractor.extract_activity,
                        args=(dirpath, x, db_name),
                    )
                    for x in filelist
                ]
                data = [p.get() for p in results]
        else:
            data = []
            for index, filename in enumerate(tqdm(filelist)):
                data.append(cls.extract_activity(dirpath, filename, db_name))

        return data

    @classmethod
    def condense_multiline_comment(cls, element):
        """
        Concatenate the text of all child elements with the tag
        "{http://www.EcoInvent.org/EcoSpold02}text" and the text of all child
        elements with the tag "{http://www.EcoInvent.org/EcoSpold02}imageUrl"
        in the given `element` XML element.

         Args
         ----
            cls (type): The class object.
            element (lxml.etree.Element): The XML element.

        Returns
        -------
            str: The concatenated text of all child elements with the tag
            "{http://www.EcoInvent.org/EcoSpold02}text" and the text of all child
            elements with the tag "{http://www.EcoInvent.org/EcoSpold02}imageUrl".
            If an error occurs, an empty string is returned.
        """
        try:
            return "\n".join(
                [
                    child.text
                    for child in element.iterchildren()
                    if child.tag == "{http://www.EcoInvent.org/EcoSpold02}text"
                ]
                + [
                    "Image: " + child.text
                    for child in element.iterchildren()
                    if child.tag == "{http://www.EcoInvent.org/EcoSpold02}imageUrl"
                ]
            )
        except:
            return ""

    @classmethod
    def extract_activity(cls, dirpath, filename, db_name):
        """
        Extract and return the data of an activity from an XML file with the given
        `filename` in the directory with the path `dirpath`.

         Args
         ----
            cls (type): The class object.
            dirpath (str): The path of the directory containing the XML file.
            filename (str): The name of the XML file.
            db_name (str): The name of the database.

        Returns
        -------
        dict: The dictionary of data for the activity. The keys and values are as
            follows:
                - "comment": str. The condensed multiline comment.
                - "classifications": list of tuples. The classification systems and
                  values of the activity.
                - "activity type": str. The type of the activity.
                - "activity": str. The ID of the activity.
                - "database": str. The name of the database.
                - "exchanges": list of dicts. The exchanges of the activity.
                - "filename": str. The name of the XML file.
                - "location": str. The short name of the location of the activity.
                - "name": str. The name of the activity.
                - "synonyms": list of str. The synonyms of the activity.
                - "parameters": dict. The parameters of the activity.
                - "authors": dict of dicts. The authors of the activity. The keys and
                  values of the inner dicts are as follows:
                    - "name": str. The name of the author.
                    - "email": str. The email of the author.
                - "type": str. The type of the activity.
        """
        root = objectify.parse(
            open(os.path.join(dirpath, filename), encoding="utf-8")
        ).getroot()
        if hasattr(root, "activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset

        comments = [
            cls.condense_multiline_comment(
                getattr2(stem.activityDescription.activity, "generalComment")
            ),
            (
                "Included activities start: ",
                getattr2(
                    stem.activityDescription.activity, "includedActivitiesStart"
                ).get("text"),
            ),
            (
                "Included activities end: ",
                getattr2(
                    stem.activityDescription.activity, "includedActivitiesEnd"
                ).get("text"),
            ),
            (
                "Geography: ",
                cls.condense_multiline_comment(
                    getattr2(stem.activityDescription.geography, "comment")
                ),
            ),
            (
                "Technology: ",
                cls.condense_multiline_comment(
                    getattr2(stem.activityDescription.technology, "comment")
                ),
            ),
            (
                "Time period: ",
                cls.condense_multiline_comment(
                    getattr2(stem.activityDescription.timePeriod, "comment")
                ),
            ),
        ]
        comment = "\n".join(
            [
                (" ".join(x) if isinstance(x, tuple) else x)
                for x in comments
                if (x[1] if isinstance(x, tuple) else x)
            ]
        )

        classifications = [
            (el.classificationSystem.text, el.classificationValue.text)
            for el in stem.activityDescription.iterchildren()
            if el.tag == u"{http://www.EcoInvent.org/EcoSpold02}classification"
        ]

        data = {
            "comment": comment,
            "classifications": classifications,
            "activity type": ACTIVITY_TYPES[
                int(stem.activityDescription.activity.get("specialActivityType") or 0)
            ],
            "activity": stem.activityDescription.activity.get("id"),
            "database": db_name,
            "exchanges": [
                cls.extract_exchange(exc)
                for exc in stem.flowData.iterchildren()
                if "parameter" not in exc.tag
            ],
            "filename": os.path.basename(filename),
            "location": stem.activityDescription.geography.shortname.text,
            "name": stem.activityDescription.activity.activityName.text,
            "synonyms": [
                s.text
                for s in getattr(stem.activityDescription.activity, "synonym", [])
            ],
            "parameters": dict(
                [
                    cls.extract_parameter(exc)
                    for exc in stem.flowData.iterchildren()
                    if "parameter" in exc.tag
                ]
            ),
            "authors": {
                "data entry": {
                    "name": stem.administrativeInformation.dataEntryBy.get(
                        "personName"
                    ),
                    "email": stem.administrativeInformation.dataEntryBy.get(
                        "personEmail"
                    ),
                },
                "data generator": {
                    "name": stem.administrativeInformation.dataGeneratorAndPublication.get(
                        "personName"
                    ),
                    "email": stem.administrativeInformation.dataGeneratorAndPublication.get(
                        "personEmail"
                    ),
                },
            },
            "type": "process",
        }
        return data

    @classmethod
    def abort_exchange(cls, exc, comment=None):
        """
        Set the uncertainty type of the input exchange to UndefinedUncertainty.id. Remove the keys "scale", "shape", "minimum", and "maximum" from the dictionary. 
        Update the "loc" key to "amount". Append "comment" to "exc['comment']" if "comment" is not None, 
        otherwise append "Invalid parameters - set to undefined uncertainty." to "exc['comment']".

        Args:
            exc (dict): The input exchange.
            comment (str, optional): A string to append to "exc['comment']". Defaults to None.

        Returns:
            None
        """
        exc["uncertainty type"] = UndefinedUncertainty.id
        exc["loc"] = exc["amount"]
        for key in ("scale", "shape", "minimum", "maximum"):
            if key in exc:
                del exc[key]
        exc["comment"] = exc.get("comment", "")
        if exc["comment"]:
            exc["comment"] += "\n"
        exc["comment"] += (
            comment or "Invalid parameters - set to undefined uncertainty."
        )

    @classmethod
    def extract_uncertainty_dict(cls, obj):
        """
        Extract uncertainty information from "obj" and return it as a dictionary.

        Args:
            obj: The input object.

        Returns:
            dict: The extracted uncertainty information.
        """
        data = {
            "amount": float(obj.get("amount")),
        }
        if hasattr(obj, "uncertainty"):
            unc = obj.uncertainty
            if hasattr(unc, "pedigreeMatrix"):
                data["pedigree"] = dict(
                    [
                        (PM_MAPPING[key], int(unc.pedigreeMatrix.get(key)))
                        for key in PM_MAPPING
                    ]
                )

            if hasattr(unc, "lognormal"):
                data.update(
                    {
                        "uncertainty type": LognormalUncertainty.id,
                        "loc": float(unc.lognormal.get("mu")),
                        "scale": math.sqrt(
                            float(unc.lognormal.get("varianceWithPedigreeUncertainty"))
                        ),
                    }
                )
                if unc.lognormal.get("variance"):
                    data["scale without pedigree"] = math.sqrt(
                        float(unc.lognormal.get("variance"))
                    )
                if data["scale"] <= 0:
                    cls.abort_exchange(data, TOO_LOW.format(data["scale"]))
                elif data["scale"] > 25:
                    cls.abort_exchange(data, TOO_HIGH.format(data["scale"]))
            elif hasattr(unc, "normal"):
                data.update(
                    {
                        "uncertainty type": NormalUncertainty.id,
                        "loc": float(unc.normal.get("meanValue")),
                        "scale": math.sqrt(
                            float(unc.normal.get("varianceWithPedigreeUncertainty"))
                        ),
                    }
                )
                if unc.normal.get("variance"):
                    data["scale without pedigree"] = math.sqrt(
                        float(unc.normal.get("variance"))
                    )
                if data["scale"] <= 0:
                    cls.abort_exchange(data)
            elif hasattr(unc, "triangular"):
                data.update(
                    {
                        "uncertainty type": TriangularUncertainty.id,
                        "minimum": float(unc.triangular.get("minValue")),
                        "loc": float(unc.triangular.get("mostLikelyValue")),
                        "maximum": float(unc.triangular.get("maxValue")),
                    }
                )
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, "uniform"):
                data.update(
                    {
                        "uncertainty type": UniformUncertainty.id,
                        "loc": data["amount"],
                        "minimum": float(unc.uniform.get("minValue")),
                        "maximum": float(unc.uniform.get("maxValue")),
                    }
                )
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, "undefined"):
                data.update(
                    {
                        "uncertainty type": UndefinedUncertainty.id,
                        "loc": data["amount"],
                    }
                )
            else:
                raise ValueError("Unknown uncertainty type")
        else:
            data.update(
                {
                    "uncertainty type": UndefinedUncertainty.id,
                    "loc": data["amount"],
                }
            )
        return data

    @classmethod
    def extract_parameter(cls, exc):
        """
        Extract parameter information from "exc" and return it as a tuple.

        Args:
            exc (dict): The input exchange.

        Returns:
            tuple: A tuple containing the parameter name and a dictionary containing the parameter information.
        
        """
        name = exc.get("variableName")
        data = {
            "description": exc.name.text,
            "id": exc.get("parameterId"),
        }
        if hasattr(exc, "unitName"):
            data["unit"] = exc.unitName.text
        if hasattr(exc, "comment"):
            data["comment"] = exc.comment.text
        data.update(cls.extract_uncertainty_dict(exc))
        if name is None:
            name = "Unnamed parameter: {}".format(data["id"])
            data["unnamed"] = True
        return name, data

    @classmethod
    def extract_properties(cls, exc):
        """
        Extract the properties of an exchange.

        Parameters
        ----------
        exc : lxml.etree.Element
            An XML element representing an exchange.

        Returns
        -------
        dict
            A dictionary of the properties of the exchange. Each key in the dictionary
            is a string representing the name of a property, and the corresponding value
            is a dictionary with the following keys:

            - "amount" (float): The numerical value of the property.
            - "comment" (str, optional): A comment describing the property, if available.
            - "unit" (str, optional): The unit of the property, if available.
            - "variable name" (str, optional): The name of the variable associated with
            the property, if available.
        """
        properties = {}

        for obj in exc.iterchildren():
            if not obj.tag.endswith("property"):
                continue

            properties[obj.name.text] = {"amount": float(obj.get("amount"))}
            if hasattr(obj, "comment"):
                properties[obj.name.text]["comment"] = obj.comment.text
            if hasattr(obj, "unitName"):
                properties[obj.name.text]["unit"] = obj.unitName.text
            if obj.get("variableName"):
                properties[obj.name.text]["variable name"] = obj.get("variableName")

        return properties

    @classmethod
    def extract_exchange(cls, exc):
        """Process exchange.

        Input groups are:

            1. Materials/fuels
            2. Electricity/Heat
            3. Services
            4. From environment (elementary exchange only)
            5. FromTechnosphere

        Output groups are:

            0. ReferenceProduct
            2. By-product
            3. MaterialForTreatment
            4. To environment (elementary exchange only)
            5. Stock addition

        """
        if exc.tag == "{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == "{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        else:
            print(exc.tag)
            raise ValueError

        is_product = hasattr(exc, "outputGroup") and exc.outputGroup.text in ("0", "2")

        if is_biosphere and is_product:
            raise ValueError("Impossible output group")

        if is_product:
            kind = "production"
        elif is_biosphere:
            kind = "biosphere"
        else:
            kind = "technosphere"

        data = {
            "flow": exc.get(flow),
            "type": kind,
            "name": exc.name.text,
            "classifications": {
                "CPC": [
                    o.classificationValue.text
                    for o in exc.iterchildren()
                    if "classification" in o.tag
                    and o.classificationSystem.text == "CPC"
                ]
            },
            "production volume": float(exc.get("productionVolumeAmount") or 0),
            "properties": cls.extract_properties(exc),
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere:
            data["activity"] = exc.get("activityLinkId")
        if hasattr(exc, "unitName"):
            data["unit"] = exc.unitName.text
        if hasattr(exc, "comment"):
            data["comment"] = exc.comment.text
        if exc.get("variableName"):
            data["variable name"] = exc.get("variableName")
        if exc.get("formula"):
            data["chemical formula"] = exc.get("formula")
        if exc.get("mathematicalRelation"):
            data["formula"] = exc.get("mathematicalRelation")

        data.update(cls.extract_uncertainty_dict(exc))
        return data
