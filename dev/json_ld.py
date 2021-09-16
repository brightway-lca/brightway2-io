import bw2io as bi
import bw2data as bd
from bw2io.extractors.json_ld import JSONLDExtractor
from pathlib import Path
from bw2io.strategies.json_ld import *

bd.projects.set_current("json-ld")
fp = "/Users/akim/Documents/LCA_files/US_Forest_Service_Forest_Products_Lab-Woody_biomass"
js = JSONLDExtractor()
data = js.extract(fp)

db = json_ld_convert_db_dict_into_list(data['processes'])
db = json_ld_rename_metadata_fields(db)
db = json_ld_add_units(db)

print()
