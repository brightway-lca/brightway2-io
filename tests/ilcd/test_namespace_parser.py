from bw2io.extractors.ilcd import extract_zip
from pathlib import Path
from lxml.etree import _Element, _ElementTree

example_file = Path(".").absolute().parent.parent / "bw2io/data/examples/ilcd-example.zip"

def get_namespaces(element):
    # Works with trees and elements
    # Returns always the namespaces of the root element
    # Make sure that element is root element
    if isinstance(element, _Element):
        element = element.getroottree().getroot()
    elif isinstance(element, _ElementTree):
        element = element.getroot()
    namespaces = element.nsmap
    return namespaces

if __name__ == "__main__":
    # Beware: extract_zip so far does not return tree objects but the root elements of the trees
    trees = extract_zip(example_file)
    for k,v in trees.items():
        namespaces = get_namespaces(v)
        # Default namespace is always namespaces[None]
        print("Namespaces of ", k, "are", get_namespaces(v))
        print("/n")

