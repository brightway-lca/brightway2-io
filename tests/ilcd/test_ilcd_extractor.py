from bw2io.extractors.ilcd import extract_zip
from pathlib import Path

def test_extract_zip():
    file = Path(".").parent / "bw2io/data/examples/ilcd-example.zip"
    extract_zip(file)