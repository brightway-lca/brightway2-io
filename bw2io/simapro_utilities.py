# -*- coding: utf-8 -*
from bw2data import Database
from copy import copy
from unidecode import unidecode
import re

detox = re.compile("(?P<product>.+)\s+\{(?P<location>.+)\}\s*\|\s+(?P<activity>.+)\s+\| (?P<model>.+), [US]")


def is_simapro8(string):
    match = detox.search(string)
    if match:
        return match.groupdict()
    else:
        return False


class NotFound(StandardError):
    """Can't find dataset match."""
    pass


class SimaProMangler(object):
    """Turn ecoinvent 3 names into SimaPro names. SimaPro does the following:

    * Forces everything to fit into the ASCII character set (we do the same with https://pypi.python.org/pypi/Unidecode)
    * Removes product names from activity names (e.g. ([sulfonyl]urea-compound, market for [sulfonyl]urea-compound) becomes ([sulfonyl]urea-compound, market for)).
    * But not always! Sometimes the activity name stays!
    * Other random changes

    So, we create a new copy of the database with what we think are the SimaPro names.
    """
    def __init__(self, background_database):
        self.db = self.create_mangled_database(background_database)

    def create_mangled_database(self, name):
        def _(string):
            """Change strings to be like SimaPro exports.

            1. Replace "°" with " degrees"
            2. Replace UTF-8 encoded unicode with ASCII representation
            3. Make lower case (as SimaPro randomly capitalizes things)
            4. Strip whitespace characters
            """
            subst_degrees = lambda s: s.replace(u"°", u" degrees")
            subst_air = lambda s: s.replace(u"air/ kiln", u"air / kiln")
            return unidecode(subst_air(subst_degrees(string))).lower().strip()

        def subtract_product(obj):
            if obj[u'reference product'] == obj[u'name']:
                return obj
            elif obj[u'reference product'] in obj[u'name']:
                obj[u'name'] = obj[u'name'].replace(obj[u'reference product'], "") \
                    .replace(" ,", ",") \
                    .replace(",,", ",") \
                    .replace("  ", " ") \
                    .strip()
                if obj[u'name'][:2] == ", ":
                    obj[u'name'] = obj[u'name'][1:].strip()
            return obj

        if u"reference product" not in Database(name).load()[Database(name).random()]:
            return []

        return [(k, subtract_product({
            j: _(w)
            for j, w in v.items()
            if j in {u'reference product', u'location', u'name'}
        })) for k, v in Database(name).load().items()] + [
        # Sometime you subtract the activity, sometimes not. Consistency is boring!
        (k, {
            j: _(w)
            for j, w in v.items()
            if j in {u'reference product', u'location', u'name'}
        }) for k, v in Database(name).load().items()]

    def match(self, obj, debug=False):
        def replace_beginning_and_end(string, word):
            lw = len(word)
            if len(word) < (lw + 1):
                return string
            if string[:lw] == word and string[lw] in {",", " "}:
                return string[lw:].strip()
            elif string[-lw:] == word and string[-lw - 1] == " ":
                return string[:-lw].strip()
            else:
                return string

        obj = {k: v.lower() for k, v in obj.items()}
        if debug:
            print "Matching:"
            print "SP name:", obj[u'activity']
            print "SP product:", obj[u'product']
        for key, value in self.db:
            if      value[u'reference product'] == obj[u'product'] and \
                    value[u'location']          == obj[u'location']:
                if debug:
                    print "Testing possible activity:"
                    print "EI name:", value[u'name']
                    print "EI product:", value[u'reference product']
                # Try 1: Identical activity names
                if value[u'name'] == obj[u'activity']:
                    return key
                # Try 2: Handle cases like the following:
                # EI product:  glazing, double, U<1.1 W/m2K
                # EI activity: glazing production, double, U<1.1 W/m2K
                # SP product: production
                # SP activity: Glazing, double, U<1.1 W/m2K
                appellation = copy(value[u'name']).replace(",", "")
                for word in obj[u'activity'].replace(",", "").split(" "):
                    appellation = appellation.replace(" " + word + " ", " ")
                    appellation = replace_beginning_and_end(appellation, word)
                if debug:
                    print "After substitution:"
                    print "Is:", appellation
                    print "Should be:", obj[u'product']
                if appellation.replace(" ", "").strip() == \
                        obj[u'product'].replace(" ", "").replace(",", "").strip():
                    return key
                # Handle cases like
                # EI product: alfalfa-grass mixture, Swiss integrated production
                # EI activity: alfalfa-grass mixture production, Swiss integrated production
                # SP product:  Alfalfa-grass mixture, Swiss integrated production
                # SP activity: production
                elif appellation.replace(" ", "").strip() == \
                        obj[u'product'].replace(obj[u'activity'], '').replace(" ", "").replace(",", "").strip():
                    return key
        raise NotFound
