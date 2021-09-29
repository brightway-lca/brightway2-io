import bw2io as bi
import bw2data as bd
from bw2io.extractors.json_ld import JSONLDExtractor
from pathlib import Path
from bw2io.strategies.json_ld import *

bd.projects.set_current("json-ld")
fp = "/Users/akim/Documents/LCA_files/US_Forest_Service_Forest_Products_Lab-Woody_biomass"
data = JSONLDExtractor.extract(fp)

data = json_ld_get_normalized_exchange_locations(data)
data = json_ld_get_normalized_exchange_units(data)
db = json_ld_get_activities_list_from_rawdata(data)
db = json_ld_add_activity_unit(db)
db = json_ld_rename_metadata_fields(db)


print()
