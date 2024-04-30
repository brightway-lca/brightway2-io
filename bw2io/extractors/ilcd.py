import zipfile
from pathlib import Path
from typing import Union
import pandas as pd

from lxml import etree


def extract(path_to_zip) -> list:
    """from a path to the ilcd zip file, extracts info required to create a
    brightway database. Hardly any transformation is done here.

    Parameters
    ----------
    path_to_zip : _type_
        _description_

    Returns
    -------
    list
        list of dicts representing actitivies (nodes).
    """
    etrees_dict = extract_zip(path_to_zip)

    # get the product system model if exists
    if "lifecyclemodels" in etrees_dict:
        psm = get_systemmodel(etrees_dict)
    else:
        psm = None

    # get contanct data
    contact_list = get_contact_from_etree(etrees_dict)

    # get unit group data
    unit_gr_dict = get_unitdata_from_etree(etree_dict=etrees_dict)

    # get flow properties
    flow_properties_list = get_flowproperties_from_etree(etrees_dict)

    # combine the flow property and unit data
    unit_fp_dict = fp_dict(flow_properties=flow_properties_list, ug_dict=unit_gr_dict)

    # extract more info from `flows` folder
    flow_list = get_flows_from_etree(etrees_dict)

    # general data from activities
    act_info = get_act_info(etrees_dict)

    # exchange data from activities
    exchanges_list = get_exchange_data(etrees_dict)

    assert len(exchanges_list) == len(act_info)

    ## combine

    # add flow property and unit to flows
    for f in flow_list:
        f["flow property"] = unit_fp_dict[f["refobj"]]["flow property"]
        f["unit"] = unit_fp_dict[f["refobj"]]["unit"]
        f["unit_multiplier"] = unit_fp_dict[f["refobj"]]["unit_multiplier"]
        f["unit_group"] = unit_fp_dict[f["refobj"]]["unit_group"]

    # reorganise following brightway expected format
    activity_info_list = combine_act_exchanges(
        act_info, exchanges_list, flow_list, contact_list, psm
    )

    return activity_info_list


def xpaths() -> dict:
    """contains xpaths used in the extraction

    Returns:
        dict: xpaths related to the different folders
    """

    xpaths_activity_info = {
        # process info
        "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
        "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
        "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
        "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
        "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
        "general_comment": "/processDataSet/processInformation/dataSetInformation/common:generalComment/text()",
        "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
        "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
        "time_representativeness_description": "/processDataSet/processInformation/time/common:timeRepresentativenessDescription/text()",
        "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
        "LatLong": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@latitudeAndLongitude",
        "reference_to_reference_flow": "/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow/text()",
        # administrative info
        "intended_application": "/processDataSet/administrativeInformation/common:commissionerAndGoal/common:intendedApplications/text()",
        "dataset_format": "/processDataSet/administrativeInformation/dataEntryBy/common:referenceToDataSetFormat/common:shortDescription/text()",
        "licensetype": "/processDataSet/administrativeInformation/publicationAndOwnership/common:licenseType/text()",
    }

    param_xpaths_unformatted = {
        "parameter_name": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/@name",
        "parameter_comment": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/comment/text()",
        "parameter_mean_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/meanValue/text()",
        "parameter_minimum_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/minimumValue/text()",
        "parameter_maximum_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/maximumValue/text()",
        "parameter_std95": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/relativeStandardDeviation95In/text()",
        "parameter_formula": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/formula/text()",
        "parameter_distrib": "/processDataSet/processInformation/mathematicalRelations/variableParameter[@name='{parameter_name}']/uncertaintyDistributionType/text()",
    }

    # Xpath for values in process XML file will return one value in a list
    xpaths_process = {
        # process information
        "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
        "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
        "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
        "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
        "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
        "general_comment": "/processDataSet/processInformation/dataSetInformation/common:generalComment/text()",
        "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
        "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
        "time_representativeness_description": "/processDataSet/processInformation/time/common:timeRepresentativenessDescription/text()",
        "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
        "LatLong": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@latitudeAndLongitude",
        "reference_to_reference_flow": "/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow/text()",
        # Xpath for values in process XML file, will return multiple values as a list
        "parameter_name": "/processDataSet/processInformation/mathematicalRelations/variableParameter/@name",
        "parameter_comment": "/processDataSet/processInformation/mathematicalRelations/variableParameter/comment/text()",
        "parameter_mean_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter/meanValue/text()",
        "parameter_minimum_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter/minimumValue/text()",
        "parameter_maximum_value": "/processDataSet/processInformation/mathematicalRelations/variableParameter/maximumValue/text()",
        "parameter_std95": "/processDataSet/processInformation/mathematicalRelations/variableParameter/relativeStandardDeviation95In/text()",
        "parameter_formula": "/processDataSet/processInformation/mathematicalRelations/variableParameter/formula/text()",
        "parameter_distrib": "/processDataSet/processInformation/mathematicalRelations/variableParameter/uncertaintyDistributionType/text()",
        # administrative info
        "intended_application": "/processDataSet/administrativeInformation/common:commissionerAndGoal/common:intendedApplications/text()",
        "dataset_format": "/processDataSet/administrativeInformation/dataEntryBy/common:referenceToDataSetFormat/common:shortDescription/text()",
        "licensetype": "/processDataSet/administrativeInformation/publicationAndOwnership/common:licenseType/text()",
        # exchanges (we start exchange data by exchange_ to parse it later )
        "exchanges_internal_id": "/processDataSet/exchanges/exchange/@dataSetInternalID",
        "exchanges_name": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/common:shortDescription/text()",
        "exchanges_uuid": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/@refObjectId",
        "exchanges_direction": "/processDataSet/exchanges/exchange/exchangeDirection/text()",
        "exchanges_amount": "/processDataSet/exchanges/exchange/resultingAmount/text()",
        "exchanges_amount_min": "/processDataSet/exchanges/exchange/minimumAmount/text()",
        "exchanges_amount_max": "/processDataSet/exchanges/exchange/maximumAmount/text()",
        "exchanges_amount_distrib": "/processDataSet/exchanges/exchange/uncertaintyDistributionType/text()",
        "exchanges_amount_std95": "/processDataSet/exchanges/exchange/relativeStandardDeviation95In/text()",
        #    "exchanges_param":"/processDataSet/exchanges/exchange/referenceToVariable/text()",
    }

    xpaths_exchanges = {
        "exchanges_name": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/common:shortDescription/text()",
        "exchanges_uuid": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/@refObjectId",
        "flow_uuid": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToFlowDataSet/@refObjectId",
        "exchanges_amount": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/resultingAmount/text()",
        "exchanges_param_name": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/referenceToVariable/text()",
        "exchanges_amount_min": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/minimumAmount/text()",
        "exchanges_amount_max": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/maximumAmount/text()",
        "exchanges_amount_distrib": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/uncertaintyDistributionType/text()",
        "exchanges_amount_std95": "/processDataSet/exchanges/exchange[@dataSetInternalID={internal_id}]/relativeStandardDeviation95In/text()",
    }

    # Xpath for values in flow XML files, will return one values in a list
    internal_id = "/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()"
    xpaths_flows = {
        # flowinformation
        "basename": "/flowDataSet/flowInformation/dataSetInformation/name/baseName/text()",
        "uuid": "/flowDataSet/flowInformation/dataSetInformation/common:UUID/text()",
        "category_0": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=0]/text()",
        "category_1": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=1]/text()",
        "category_2": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]/text()",
        "CAS number": "/flowDataSet/flowInformation/dataSetInformation/CASNumber/text()",
        # modelling and validation
        "type": "/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet/text()",
        "value": f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/meanValue/text()",
        # flow properties
        "refobj": f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/referenceToFlowPropertyDataSet/@refObjectId",
        "flow property description": f"/flowDataSet/flowProperties/flowProperty[@dataSetInternalID={internal_id}]/referenceToFlowPropertyDataSet/common:shortDescription/text()",
    }

    xpath_contacts = {
        "email": "/contactDataSet/contactInformation/dataSetInformation/email/text()",
        "website": "/contactDataSet/contactInformation/dataSetInformation/WWWAddress/text()",
        "short_name": "/contactDataSet/contactInformation/dataSetInformation/common:shortName/text()",
    }
    xpath_flowproperties = {
        "flow_property_name": "/flowPropertyDataSet/flowPropertiesInformation/dataSetInformation/common:name[@xml:lang='en']/text()",  # only the english one
        "refObjectId_unitgroup": "/flowPropertyDataSet/flowPropertiesInformation/quantitativeReference/referenceToReferenceUnitGroup/@refObjectId",
        "refobjuuid": "/flowPropertyDataSet/flowPropertiesInformation/dataSetInformation/common:UUID/text()",
    }

    # TODO: check, this may use the same internal id for all
    unit_internal_id = "/unitGroupDataSet/unitGroupInformation/quantitativeReference/referenceToReferenceUnit/text()"
    xpath_unitgroups = {
        "ref_to_refunit": unit_internal_id,
        "ug_uuid": "/unitGroupDataSet/unitGroupInformation/dataSetInformation/common:UUID/text()",
        "unit_name": f"/unitGroupDataSet/units/unit[@dataSetInternalID={unit_internal_id}]/name/text()",
        "unit_amount": f"/unitGroupDataSet/units/unit[@dataSetInternalID={unit_internal_id}]/meanValue/text()",
        "ug_name": "/unitGroupDataSet/unitGroupInformation/dataSetInformation/common:name[@xml:lang='en']/text()",
    }

    xpaths_lifecyclemodel = {
        "ref_to_refproc": "/lifeCycleModelDataSet/lifeCycleModelInformation/quantitativeReference/referenceToReferenceProcess/text()",
        "internal_ids": "/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance/@dataSetInternalID",
        "ref_to_act_uuid": "/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance/referenceToProcess/@refObjectId",
    }

    xpaths_dict = {
        "xpath_contacts": xpath_contacts,
        "xpaths_flows": xpaths_flows,
        # 'xpaths_process':xpaths_process,
        "xpath_flowproperties": xpath_flowproperties,
        "xpaths_unitgroups": xpath_unitgroups,
        "xpaths_exchanges": xpaths_exchanges,
        "xpaths_lifecyclemodel": xpaths_lifecyclemodel,
        "xpaths_activity_info": xpaths_activity_info,
        "param_xpaths_unformatted": param_xpaths_unformatted,
    }

    return xpaths_dict


def namespaces_dict() -> dict:
    """returns a dict with namespaces

    Returns
    -------
    dict
        _description_
    """
    # Namespaces to use with the XPath (from files under xmlns)
    namespaces = {
        "default_process_ns": {"pns": "http://lca.jrc.it/ILCD/Process"},
        "default_flow_ns": {"fns": "http://lca.jrc.it/ILCD/Flow"},
        "others": {"common": "http://lca.jrc.it/ILCD/Common"},
        "default_contact_ns": {"contact": "http://lca.jrc.it/ILCD/Contact"},
        "default_fp_ns": {"fpns": "http://lca.jrc.it/ILCD/FlowProperty"},
        "default_unitgroup_ns": {"ugns": "http://lca.jrc.it/ILCD/UnitGroup"},
        "lifecyclemodel_ns": {
            "lcmns": "http://eplca.jrc.ec.europa.eu/ILCD/LifeCycleModel/2017"
        },
    }

    return namespaces


def extract_zip(path: Union[Path, str] = None) -> dict:
    """

    Args:
        path (Union[Path, str], optional): _description_. Defaults to None.

    Returns:
        dict: _description_
    """
    # ILCD should be read in a particular order
    sort_order = {
        "contacts": 0,
        "sources": 1,
        "unitgroups": 2,
        "flowproperties": 3,
        "flows": 4,
        "processes": 5,
        "external_docs": 6,
        "lifecyclemodels": 7,
    }

    # for the moment we ignore some of the folders
    to_ignore = [
        "sources",
        "external_docs",
    ]

    with zipfile.ZipFile(path, mode="r") as archive:
        filelist = archive.filelist

        # filter dirs, only files, sometimes there is empty folders there
        filelist = [f for f in filelist if f.is_dir() == False]

        # remove folders that we do not need
        filelist = [
            file
            for file in filelist
            if Path(file.filename).parent.name not in to_ignore
        ]

        # remove non xml files
        filelist = [file for file in filelist if Path(file.filename).suffix == ".xml"]

        # sort by folder (a default key for folders that go at the end wo order)
        filelist = sorted(
            filelist, key=lambda x: sort_order.get(Path(x.filename).parts[1], 99)
        )

        trees = {}
        for file in filelist:
            file_type = Path(file.filename).parts[1]
            if file_type not in trees:
                trees[file_type] = {}
            f = archive.read(file)
            trees[file_type][file.filename] = etree.fromstring(f)

    return trees


def apply_xpaths_to_xml_file(xpath_dict: dict, xml_tree) -> dict:
    """_summary_

    Args:
        xpath_dict (dict): _description_
        xml_tree (_type_): _description_

    Returns:
        dict: _description_
    """
    namespaces = namespaces_dict()

    results = {}
    hint = list(xpath_dict.items())[0][1].split("/")[1]

    selec_namespace = {
        "contactDataSet": namespaces["default_contact_ns"],
        "flowDataSet": namespaces["default_flow_ns"],
        "processDataSet": namespaces["default_process_ns"],
        "flowPropertyDataSet": namespaces["default_fp_ns"],
        "unitGroupDataSet": namespaces["default_unitgroup_ns"],
        "lifeCycleModelDataSet": namespaces["lifecyclemodel_ns"],
    }

    default_ns = selec_namespace[hint]

    for k in xpath_dict:
        results[k] = get_xml_value(
            xml_tree, xpath_dict[k], default_ns, namespaces["others"]
        )
    return results


def get_xml_value(xml_tree, xpath_str, default_ns, namespaces) -> dict:
    assert len(default_ns) == 1, "The general namespace is not clearly defined."
    namespaces.update(default_ns)

    # Adding the general namespace name to xpath expression
    xpath_segments = xpath_str.split("/")
    namespace_abbrevation = list(default_ns.keys())[0]
    for i in range(len(xpath_segments)):
        if (
            ":" not in xpath_segments[i]
            and "(" not in xpath_segments[i]
            and "@" not in xpath_segments[i][:1]
            and "" != xpath_segments[i]
        ):
            xpath_segments[i] = namespace_abbrevation + ":" + xpath_segments[i]
    xpath_str = "/".join(xpath_segments)
    r = xml_tree.xpath(xpath_str, namespaces=namespaces)

    if len(r) == 0:
        return None
    if len(r) == 1:
        return r[0]
    return r


class ILCDExtractor(object):
    """_summary_

    Args:
        object (_type_): _description_

    Returns:
        _type_: _description_
    """

    @classmethod
    def _extract(cls, path):
        assert Path(path).exists(), "path to file does not seem to exist"
        data = extract(path)

        return data


def get_unitdata_from_etree(etree_dict: dict) -> dict:
    """extracts data from the unitgroups xml files. for each dataset the uuid in
    dataset information, and the unit name and multiplier

    Args:
        etree_dict (dict): _description_

    Returns:
        dict: refobj uuid as key and the name of the unit and multiplier factor
        as values inside a dict
    """
    unit_d = {}

    xpaths_dict = xpaths()
    xpaths_unitgr = xpaths_dict["xpaths_unitgroups"]

    for _, etree in etree_dict.get("unitgroups").items():
        unit_gr = apply_xpaths_to_xml_file(xpaths_unitgr, etree)

        unit_d[unit_gr["ug_uuid"]] = {
            "unit": unit_gr["unit_name"],
            "multiplier": float(unit_gr["unit_amount"]),
            "unit_group": unit_gr["ug_name"],
        }

    return unit_d


def get_systemmodel(etree_dict: dict) -> dict:
    """gets data from the system model. Only run if present

    Parameters
    ----------
    etree_dict : dict
        _description_

    Returns
    -------
    dict
        _description_
    """
    xpaths_dict = xpaths()
    xpaths_psm = xpaths_dict["xpaths_lifecyclemodel"]

    for _, etree in etree_dict.get("lifecyclemodels").items():
        psm = apply_xpaths_to_xml_file(xpaths_psm, etree)

    # with the internal id we can get the input and output flow connexions
    connexions = {}
    for internal_id, act_uuid in zip(psm["internal_ids"], psm["ref_to_act_uuid"]):
        d = {
            f"downstream_id_{internal_id}": f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID='{internal_id}']/connections/outputExchange/downstreamProcess/@id",
            "downstream_uuid": f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID='{internal_id}']/connections/outputExchange/downstreamProcess/@flowUUID",
            "upstream_uuid": f"/lifeCycleModelDataSet/lifeCycleModelInformation/technology/processes/processInstance[@dataSetInternalID='{internal_id}']/connections/outputExchange/@flowUUID",
        }

        r = apply_xpaths_to_xml_file(d, etree)

        # it would be better to link it to something that we can relate to the activity ..
        connexions[act_uuid] = {
            "upstream": r["upstream_uuid"],
            "downstream": r["downstream_uuid"],
        }

    return connexions


def reorganise_unit_group_data(unit_list):
    ug_dict = {}
    for ug in unit_list:
        # transform multipliers to numbers
        ug["unit_amount"] = [float(number) for number in ug["unit_amount"]]

        unit_name = ug["unit_name"][int(ug["ref_to_refunit"])]
        unit_amount = float(ug["unit_amount"][int(ug["ref_to_refunit"])])
        ref_unit_name = ug["unit_name"][ug["unit_amount"].index(1)]

        ug_dict[ug["ug_uuid"]] = {
            "name": unit_name,
            "amount": unit_amount,
            "ref_unit": ref_unit_name,
        }

    return ug_dict


def get_contact_from_etree(etree_dict: dict) -> list:
    """extracts data from the 'contacts' folder

    Args:
        etree_dict (dict): _description_

    Returns:
        list: list of dicts with contact information
    """
    contact_list = []

    xpaths_dict = xpaths()
    xpath_contacts = xpaths_dict["xpath_contacts"]

    for _, etree in etree_dict.get("contacts").items():
        contacts = apply_xpaths_to_xml_file(xpath_contacts, etree)
        contact_list.append(contacts)

    return contact_list


def get_flows_from_etree(etrees_dict: dict) -> list:
    """extracts data from 'flows' folder

    Parameters
    ----------
    etrees_dict : dict
        _description_

    Returns
    -------
    list
        _description_
    """
    namespaces = namespaces_dict()
    default_ns = namespaces["default_flow_ns"]
    ns = namespaces["others"]
    ns.update(default_ns)

    xpaths_dict = xpaths()
    xpaths_flows = xpaths_dict["xpaths_flows"]

    flow_list = []
    for path, etree in etrees_dict["flows"].items():
        thing = apply_xpaths_to_xml_file(xpaths_flows, etree)
        flow_list.append(thing)

    return flow_list


def get_flowproperties_from_etree(etree_dict: dict) -> list:
    """extracts data from the 'flowproperties' folder

    Args:
        etree_dict (dict): _description_

    Returns:
        list: _description_
    """
    fp_list = []

    xpaths_dict = xpaths()
    xpath_contacts = xpaths_dict["xpath_flowproperties"]

    for _, etree in etree_dict.get("flowproperties").items():
        fp = apply_xpaths_to_xml_file(xpath_contacts, etree)

        # TODO: modify so it returns a list when is just one element
        fp_list.append(fp)

    return fp_list


def fp_dict(flow_properties: list, ug_dict: dict):
    """combines data from the unit group folder and the flow properties folder
    to construct get the unit and the flow property of each exchange using as key
    data from the ... exchanges? folder as well as the multiplier associated with
    a reference unit

    Parameters
    ----------
    flow_properties : list
        _description_
    ug_dict : dict
        _description_

    Returns
    -------
    _type_
        _description_
    """
    fp_dict = {}
    for fp in flow_properties:
        d = {
            "flow property": fp["flow_property_name"],
            "unit": ug_dict[fp["refObjectId_unitgroup"]]["unit"],
            "unit_multiplier": ug_dict[fp["refObjectId_unitgroup"]]["multiplier"],
            "unit_group": ug_dict[fp["refObjectId_unitgroup"]]["unit_group"],
        }

        fp_dict[fp["refobjuuid"]] = d

    return fp_dict


def get_exchanges_ids(etrees_dict) -> dict:
    """the internal exchange ids of the different processes in the ilcd dataset.
    This is later used to better parse the exchanges files

    Parameters
    ----------
    etrees_dict : dict
        _description_

    Returns
    -------
    dict
        uuids of activities as keys and internalids of exchanges as values
    """

    reorganised = {}
    for etree in etrees_dict["processes"].values():
        exchanges_internal_ids = apply_xpaths_to_xml_file(
            {
                "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
                "exchanges_id": "/processDataSet/exchanges/exchange/@dataSetInternalID",
            },
            etree,
        )

        reorganised[exchanges_internal_ids["uuid"]] = exchanges_internal_ids[
            "exchanges_id"
        ]

    return reorganised


def get_exchange_data(etree_dict):
    """extracts the data on exchanges of the processes files

    Parameters
    ----------
    etree_dict : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    exchanges_internal_ids = get_exchanges_ids(etree_dict)
    d = xpaths()["xpaths_exchanges"]

    ex = []

    for path, etree in etree_dict["processes"].items():
        uuid = Path(path).stem[0:36]  # trick ..
        etre_internal_ex_ids = exchanges_internal_ids[uuid]

        exchanges_dict = {}
        for internal_id in etre_internal_ex_ids:
            # formats to be specific to the process
            formatted_xpaths = {
                k: v.format(internal_id=internal_id) for k, v in d.items()
            }
            extract_procceses = apply_xpaths_to_xml_file(formatted_xpaths, etree)

            # reorganise
            exchanges_dict[extract_procceses["flow_uuid"]] = {
                "exchanges_name": extract_procceses["exchanges_name"],
                "exchanges_uuid": extract_procceses["exchanges_uuid"],
                "exchanges_resulting_amount": extract_procceses["exchanges_amount"],
                "exchanges_param_name": extract_procceses["exchanges_param_name"],
                "exchanges_amount_min": extract_procceses["exchanges_amount_min"],
                "exchanges_amount_max": extract_procceses["exchanges_amount_max"],
                "exchanges_amount_distrib": extract_procceses[
                    "exchanges_amount_distrib"
                ],
                "exchanges_amount_rStd": extract_procceses["exchanges_amount_std95"],
                "exchanges_internal_id": internal_id,
            }
        ex.append(exchanges_dict)

    return ex


def get_param_data(etree_dict: dict) -> list:
    """extract parameter data if existing

    Parameters
    ----------
    etree_dict : dict
        _description_

    Returns
    -------
    list
        each element contains a list of parameters (if existing of each of the
        activities in the ilcd zip file)
    """
    xpaths_dict = xpaths()

    # get parameter names
    pnames_d = {}

    for file, etree in etree_dict["processes"].items():
        pnames = apply_xpaths_to_xml_file(
            {
                "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
                "parameter_name": "/processDataSet/processInformation/mathematicalRelations/variableParameter/@name",
            },
            etree,
        )
        pnames_d[pnames["uuid"]] = pnames["parameter_name"]

    # preformated xpaths
    param_xpaths_unformatted = xpaths_dict["param_xpaths_unformatted"]

    # get
    act_param_list = []
    for file, etree in etree_dict["processes"].items():
        uuid = Path(file).stem[0:36]

        parameters = pnames_d[uuid]

        if parameters is None:
            parameters = []

        act_param = []
        for p in parameters:
            formatted_xpaths = {
                k: v.format(parameter_name=p)
                for k, v in param_xpaths_unformatted.items()
            }
            param_data = apply_xpaths_to_xml_file(formatted_xpaths, etree)
            act_param.append(param_data)

        act_param_list.append(act_param)

    return act_param_list


def get_act_info(etree_dict: dict) -> list:
    """extracts info from the processes files relative to the activity (nodes)

    Parameters
    ----------
    etree_dict : dict
        _description_

    Returns
    -------
    list
        _description_
    """
    xpaths_dict = xpaths()
    xpaths_activity_info = xpaths_dict["xpaths_activity_info"]

    act_info_list = []
    for file, etree in etree_dict["processes"].items():
        act_info = apply_xpaths_to_xml_file(xpaths_activity_info, etree)
        act_info_list.append(act_info)

    act_param_list = get_param_data(etree_dict)

    for act, params in zip(act_info_list, act_param_list):
        act["parameters"] = params

    return act_info_list


def combine_act_exchanges(
    act_info: list, exchanges_list: list, flow_list: list, contact_list: list, psm: dict
) -> list:
    """reorganises the data from activities, exchanges, flows and contacts
    according to the brightway logic. It scales the amount by the `meanvalue` in
    flow properties (see
    https://eplca.jrc.ec.europa.eu/LCDN/downloads/ILCD_Format_1.1_Documentation/ILCD_FlowDataSet.html)

    Parameters
    ----------
    act_info : list
        _description_
    exchanges_list : list
        _description_
    flow_list : list
        _description_
    contact_list : list
        _description_

    Returns
    -------
    list
        _description_
    """

    flow_df = pd.DataFrame(flow_list)

    activity_info_list = []
    for act, exchanges in zip(act_info, exchanges_list):
        # put flow data into the exchanges (lazy approach using pandas)
        exchanges_df = pd.DataFrame(exchanges).T.merge(
            flow_df, left_on="exchanges_uuid", right_on="uuid", how="inner"
        )

        # scale according to bizzare logic (perhaps move to a strategy)
        exchanges_df["amount"] = exchanges_df["exchanges_resulting_amount"].map(
            float
        ) * exchanges_df["value"].map(float)

        act["exchanges"] = exchanges_df.to_dict("records")
        act["contacts"] = contact_list

        if psm is not None:
            connexion = psm.get(act["uuid"])
            act["connexions"] = connexion

        activity_info_list.append(act)

    return activity_info_list