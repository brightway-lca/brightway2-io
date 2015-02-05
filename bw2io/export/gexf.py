# -*- coding: utf-8 -*
from bw2data import config, Database, Filter
from lxml.builder import ElementMaker
from lxml.etree import tostring
import datetime
import itertools
import os
import progressbar


class DatabaseToGEXF(object):
    """Export a Gephi graph for a database.

    Call ``.export()`` to export the file after class instantiation.

    Args:
        * *database* (str): Database name.
        * *include_descendants* (bool): Include databases which are linked from ``database``.

    .. warning:: ``include_descendants`` is not yet implemented.

    """
    def __init__(self, database, include_descendants=False):
        self.database = database
        self.descendants = include_descendants
        if self.descendants:
            raise NotImplemented
        filename = database + ("_plus" if include_descendants else "")
        self.filepath = os.path.join(config.request_dir("output"),
            filename + ".gexf")
        self.data = Database(self.database).load()
        self.id_mapping = dict([(key, str(i)) for i, key in enumerate(
            self.data)])

    def export(self):
        """Export the Gephi XML file. Returns the filepath of the created file."""
        E = ElementMaker(namespace="http://www.gexf.net/1.2draft",
            nsmap={None: "http://www.gexf.net/1.2draft"})
        meta = E.meta(E.creator("Brightway2"), E.description(self.database),
            lastmodified=datetime.date.today().strftime("%Y-%m-%d"))
        attributes = E.attributes(
            E.attribute(id="0", title="category", type="string"),
            **{"class": "node"}
        )
        nodes, edges = self.get_data(E)
        graph = E.graph(attributes, nodes, edges, mode="static",
            defaultedgetype="directed")
        with open(self.filepath, "w") as f:
            f.write(tostring(E.gexf(meta, graph, version="1.2"),
                xml_declaration=True, encoding="utf-8",
                pretty_print=True))
        return self.filepath

    def get_data(self, E):
        """Get Gephi nodes and edges."""
        count = itertools.count()
        nodes = []
        edges = []

        widgets = [
            progressbar.SimpleProgress(sep="/"), " (",
            progressbar.Percentage(), ') ',
            progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
            progressbar.ETA()
        ]
        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(self.data)
            ).start()

        for i, (key, value) in enumerate(self.data.iteritems()):
            nodes.append(E.node(
                E.attvalues(
                    E.attvalue(
                        value="-".join(value["categories"]),
                        **{"for": "0"}
                    )
                ),
                id=self.id_mapping[key],
                label=value.get("name", "Unknown")
            ))
            for exc in value.get("exchanges", []):
                if exc["input"] not in self.id_mapping:
                    continue
                elif exc["input"] == key:
                    # Don't need production process in graph
                    continue
                else:
                    edges.append(E.edge(
                        id=str(count.next()),
                        source=self.id_mapping[exc["input"]],
                        target=self.id_mapping[key],
                        label="%.3g" % exc['amount'],
                    ))
            pbar.update(i)
        pbar.finish()

        return E.nodes(*nodes), E.edges(*edges)


class DatabaseSelectionToGEXF(DatabaseToGEXF):
    """Export a Gephi graph for a selection of activities from a database.

    Also includes all inputs for the filtered activities.

    Args:
        * *database* (str): Database name.
        * *keys* (str): The activity keys to export.

    """
    def __init__(self, database, keys):
        self.database = database
        self.filepath = os.path.join(config.request_dir("output"),
            database + "selection.gexf")
        unfiltered_data = Database(self.database).load()
        self.data = {key: value for key, value in unfiltered_data.iteritems() if key in keys}
        self.id_mapping = dict([(key, str(i)) for i, key in enumerate(
            self.data)])


def keyword_to_gephi_graph(database, keyword):
    """Export a Gephi graph for a database for all activities whose names include the string ``keyword``.

    Args:
        * *database* (str): Database name.
        * *keyword* (str): Keyword to search for.

    Returns:
        The filepath of the exported file.

    """
    query = Database(database).query(Filter("name", "ihas", keyword))
    return DatabaseSelectionToGEXF(database, set(query.keys())).export()
