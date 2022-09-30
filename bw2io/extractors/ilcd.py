import zipfile
from pathlib import Path
from lxml import etree
from typing import Union


def extract_zip(path:Union[Path,str]=None):
    # ILCD should be read in a particular order
    sort_order = {
    "contacts": 0,
    "sources":1,
    "unitgroups":2,
    "flowproperties": 3,
    "flows":4,
    "processes":5,
    "external_docs": 6,
    }

    # for the moment we ignore some of the folders
    to_ignore = ['contacts', 'sources',
    'unitgroups', 'flowproperties',
    'external_docs',"processes"]

    if path is None:
        path = Path(__file__).parent.parent / 'data' /'examples'/"ilcd_example.zip"

    with zipfile.ZipFile(path, mode="r") as archive:
        filelist = archive.filelist

        # remove folders that we do not need
        filelist = [file for file in filelist if Path(file.filename).parts[1]
        not in to_ignore]

        # sort by folder
        filelist = sorted(filelist,key=lambda x:sort_order.get(Path(x.filename).parts[1]))

        for file in filelist:
            f = archive.read(file)
            tree = etree.fromstring(f)
            break