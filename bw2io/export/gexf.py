import datetime
import itertools
import os

from bw2data import Database, projects
from bw2data.query import Filter
from lxml.builder import ElementMaker
from lxml.etree import tostring
from tqdm import tqdm


class DatabaseToGEXF(object):
    """
    Export a Gephi graph for a database.

    Parameters
    ----------
    database : str
        Database name.
    include_descendants : bool, optional
        Include databases which are linked from ``database``. (default False)
    
    Warnings
    --------
    ``include_descendants`` is not yet implemented.

    Raises
    ------
    NotImplemented
        If ``include_descendants`` is True, as this option is not yet implemented.

    Methods
    -------
    export()
        Export the Gephi XML file.
    get_data(E)
        Get the nodes and edges for the Gephi XML file.

    Examples
    --------
    >>> dtg = DatabaseToGEXF(database='example_db', include_descendants=False)
    >>> dtg.export()
    '/path/to/example_db.gexf'

    >>> dtg = DatabaseToGEXF(database='example_db', include_descendants=True)
    >>> dtg.get_data()
    (nodes, edges)
    """

    def __init__(self, database, include_descendants=False):
        self.database = database
        self.descendants = include_descendants
        if self.descendants:
            raise NotImplemented
        filename = database + ("_plus" if include_descendants else "")
        self.filepath = os.path.join(projects.output_dir, filename + ".gexf")
        self.data = Database(self.database).load()
        self.id_mapping = dict([(key, str(i)) for i, key in enumerate(self.data)])

    def export(self):
        """
        Export the Gephi XML file.

        Parameters
        ----------
        None

        Returns
        -------
        str
            Filepath of the created file.

        Examples
        --------
        >>> dtg = DatabaseToGEXF(database='example_db', include_descendants=False)
        >>> dtg.export()
        '/path/to/example_db.gexf'
        """
        E = ElementMaker(
            namespace="http://www.gexf.net/1.2draft",
            nsmap={None: "http://www.gexf.net/1.2draft"},
        )
        meta = E.meta(
            E.creator("Brightway2"),
            E.description(self.database),
            lastmodified=datetime.date.today().strftime("%Y-%m-%d"),
        )
        attributes = E.attributes(
            E.attribute(id="0", title="category", type="string"), **{"class": "node"}
        )
        nodes, edges = self.get_data(E)
        graph = E.graph(
            attributes, nodes, edges, mode="static", defaultedgetype="directed"
        )
        with open(self.filepath, "w", encoding="utf-8") as f:
            # Need XML declaration, but then ``tostring`` returns bytes
            # so need to decode.
            # See https://bugs.python.org/issue10942
            # and http://makble.com/python-why-lxml-etree-tostring-method-returns-bytes
            f.write(
                tostring(
                    E.gexf(meta, graph, version="1.2"),
                    xml_declaration=True,
                    encoding="utf-8",
                    pretty_print=True,
                ).decode("utf-8")
            )
        return self.filepath

    def get_data(self, E):
        """
        Get Gephi nodes and edges.

        Parameters
        ----------
        E : lxml.builder.ElementMaker
            ElementMaker object for GEXF XML

        Returns
        -------
        nodes : lxml.etree._Element
            GEXF nodes
        edges : lxml.etree._Element
            GEXF edges

        Examples
        --------
        >>> dtg = DatabaseToGEXF(database='example_db', include_descendants=False)
        >>> dtg.get_data(E)
        (nodes, edges)
        """

        count = itertools.count()
        nodes = []
        edges = []

        for key, value in tqdm(self.data.items()):
            nodes.append(
                E.node(
                    E.attvalues(
                        E.attvalue(
                            value="-".join(value.get("categories", [])), **{"for": "0"}
                        )
                    ),
                    id=self.id_mapping[key],
                    label=value.get("name", "Unknown"),
                )
            )
            for exc in value.get("exchanges", []):
                if exc["input"] not in self.id_mapping:
                    continue
                elif exc["input"] == key:
                    # Don't need production process in graph
                    continue
                else:
                    edges.append(
                        E.edge(
                            id=str(next(count)),
                            source=self.id_mapping[exc["input"]],
                            target=self.id_mapping[key],
                            label="%.3g" % exc["amount"],
                        )
                    )

        return E.nodes(*nodes), E.edges(*edges)


class DatabaseSelectionToGEXF(DatabaseToGEXF):
    """
    Export a Gephi graph for a selection of activities from a database.

    Also includes all inputs for the filtered activities.

    Parameters
    ----------
    database : str
        Database name.
    keys : str
        The activity keys to export.

    Examples
    --------
    >>> dstg = DatabaseSelectionToGEXF(database='example_db', keys=['foo', 'bar'])

    """

    def __init__(self, database, keys):
        self.database = database
        self.filepath = os.path.join(projects.output_dir, database + ".selection.gexf")
        unfiltered_data = Database(self.database).load()
        self.data = {
            key: value for key, value in unfiltered_data.items() if key in keys
        }
        self.id_mapping = dict([(key, str(i)) for i, key in enumerate(self.data)])


def keyword_to_gephi_graph(database, keyword):
    """
    Export a Gephi graph for a database for all activities whose names include the string ``keyword``.

    Parameters
    ----------
    database : str
        Database name.
    keyword : str
        Keyword to search for.

    Returns
    -------
    str
        The filepath of the exported file.

    Examples
    --------
    >>> keyword_to_gephi_graph(database='example_db', keyword='foo')
    '/path/to/example_db.gexf'
    """
    query = Database(database).query(Filter("name", "ihas", keyword))
    return DatabaseSelectionToGEXF(database, set(query.keys())).export()
