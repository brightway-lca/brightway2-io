from .ecoinvent_lcia import EcoinventLCIAImporter
from .ecospold1 import (
    MultiOutputEcospold1Importer,
    NoIntegerCodesEcospold1Importer,
    SingleOutputEcospold1Importer,
)
from .ecospold1_lcia import Ecospold1LCIAImporter
from .ecospold2 import SingleOutputEcospold2Importer
from .ecospold2_biosphere import Ecospold2BiosphereImporter
from .excel import CSVImporter, ExcelImporter
from .excel_lcia import CSVLCIAImporter, ExcelLCIAImporter
from .exiobase3_hybrid import Exiobase3HybridImporter
from .exiobase3_monetary import Exiobase3MonetaryImporter
from .simapro_csv import SimaProCSVImporter
from .simapro_lcia_csv import SimaProLCIACSVImporter

try:
    from .simapro_block_csv import SimaProBlockCSVImporter
except ImportError:
    from bw2data.logs import stdout_feedback_logger

    stdout_feedback_logger.warning(
        "Can't import `SimaProBlockCSVImporter` - please install `bw2io` with `pip install bw2io[multifunctional]` or install `multifunctional` and `bw_simapro_csv` manually."
    )
