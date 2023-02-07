from ..utils import ExchangeLinker
from bw2data.parameters import Interpreter, get_activity

link_activities_to_database = ExchangeLinker.link_activities_to_database


def add_dummy_amounts(activities, amount, overwrite=False):
    for act in activities:
        for ex in act.get("exchanges", []):
            if "amount" not in ex or overwrite is True:
                ex["amount"] = amount
    return activities


def add_dummy_inputs(activities, default_input=None, overwrite=False):
    default_input = default_input or (
        activities[0]["database"],
        activities[0]["code"],
    )
    for act in activities:
        for ex in act.get("exchanges", []):
            if "input" not in ex or overwrite is True:
                ex["input"] = default_input
                ex["dummy input"] = True
    return activities


def delete_dummy_inputs(activities):
    for act in activities:
        for ex in act.get("exchanges", []):
            if ex.get("dummy input", False):
                del ex["input"]
                del ex["dummy input"]
    return activities


def convert_exchange_unit_to_input_unit(activities):
    for act in activities:
        for ex in act.get("exchanges", []):
            if "input" not in ex:
                continue
            input_act = get_activity(ex["input"])
            Interpreter().set_amount_and_unit(
                obj=ex,
                to_unit=input_act.get("unit")
            )

    return activities
