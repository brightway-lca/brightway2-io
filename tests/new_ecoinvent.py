import bw2data as bd
import bw2io as bi

# bd.projects.set_current("testX1")
# bi.import_ecoinvent_release(
#     version="3.9.1",
#     system_model="cutoff",
# )
# print(bd.databases)
# print(bd.Database("ecoinvent-3.9.1-cutoff").random().lca(bd.methods.random()).score)
# print(len(bd.Database("ecoinvent-3.9.1-cutoff")))
# print(len(bd.Database("ecoinvent-3.9.1-biosphere")))


# output
'''
Applying strategy: normalize_units
Applying strategy: drop_unspecified_subcategories
Applying strategy: ensure_categories_are_tuples
Applied 3 strategies in 0.00 seconds
4718 datasets
        0 exchanges
        Links to the following databases:

        0 unlinked exchanges (0 types)

100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4718/4718 [00:00<00:00, 23494.63it/s]
18:09:57 [info     ] Vacuuming database            
Created database: ecoinvent-3.9.1-biosphere
Extracting XML data from 21238 datasets
Extracted 21238 datasets in 49.66 seconds
Applying strategy: normalize_units
Applying strategy: update_ecoinvent_locations
Applying strategy: remove_zero_amount_coproducts
Applying strategy: remove_zero_amount_inputs_with_no_activity
Applying strategy: remove_unnamed_parameters
Applying strategy: es2_assign_only_product_with_amount_as_reference_product
Applying strategy: assign_single_product_as_activity
Applying strategy: create_composite_code
Applying strategy: drop_unspecified_subcategories
Applying strategy: fix_ecoinvent_flows_pre35
Applying strategy: drop_temporary_outdated_biosphere_flows
Applying strategy: link_biosphere_by_flow_uuid
Applying strategy: link_internal_technosphere_by_composite_code
Applying strategy: delete_exchanges_missing_activity
Applying strategy: delete_ghost_exchanges
Applying strategy: remove_uncertainty_from_negative_loss_exchanges
Applying strategy: fix_unreasonably_high_lognormal_uncertainties
Applying strategy: convert_activity_parameters_to_list
Applying strategy: add_cpc_classification_from_single_reference_product
Applying strategy: delete_none_synonyms
Applying strategy: update_social_flows_in_older_consequential
Applying strategy: set_lognormal_loc_value
Applied 22 strategies in 7.22 seconds
21238 datasets
        674593 exchanges
        Links to the following databases:
                ecoinvent-3.9.1-biosphere (407351 exchanges)
                ecoinvent-3.9.1-cutoff (267242 exchanges)
        0 unlinked exchanges (0 types)

18:11:05 [warning  ] Not able to determine geocollections for all datasets. This database is not ready for regionalization.
100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 21238/21238 [00:35<00:00, 591.84it/s]
18:11:41 [info     ] Vacuuming database            
Created database: ecoinvent-3.9.1-cutoff
Databases dictionary with 2 object(s):
        ecoinvent-3.9.1-biosphere
        ecoinvent-3.9.1-cutoff
/home/marsh/miniforge3/envs/bionly/lib/python3.11/site-packages/bw2calc/__init__.py:47: UserWarning: 
It seems like you have an AMD/INTEL x64 architecture, but haven't installed pypardiso:

    https://pypi.org/project/pypardiso/

Installing it could give you much faster calculations.

  warnings.warn(PYPARDISO_WARNING)
0.00018292772209895243
21238
4718
'''

# biosphere only
# bd.projects.set_current("testX2")
# bi.import_ecoinvent_biosphere("3.9.1")
# print(bd.databases)
# print(len(bd.Database("ecoinvent-3.9.1-biosphere")))

# output
'''
Applying strategy: normalize_units
Applying strategy: drop_unspecified_subcategories
Applying strategy: ensure_categories_are_tuples
Applied 3 strategies in 0.00 seconds
4718 datasets
        0 exchanges
        Links to the following databases:

        0 unlinked exchanges (0 types)

100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4718/4718 [00:00<00:00, 19210.42it/s]
18:17:02 [info     ] Vacuuming database            
Created database: ecoinvent-3.9.1-biosphere
Databases dictionary with 1 object(s):
        ecoinvent-3.9.1-biosphere
4718
'''

# adding lcia
bd.projects.set_current("testX2")
bi.import_ecoinvent_release(
    version="3.9.1",
    system_model="cutoff",
    lci=False,
    lcia=True,
)

print(bd.databases)
print(bd.methods)

# output
'''
Databases dictionary with 1 object(s):
        ecoinvent-3.9.1-biosphere
Methods dictionary with 762 objects, including:
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'acidification', 'acidification (incl. fate, average Europe total, A&B)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'climate change', 'global warming potential (GWP100)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'ecotoxicity: freshwater', 'freshwater aquatic ecotoxicity (FAETP inf)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'ecotoxicity: marine', 'marine aquatic ecotoxicity (MAETP inf)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'ecotoxicity: terrestrial', 'terrestrial ecotoxicity (TETP inf)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'energy resources: non-renewable', 'abiotic depletion potential (ADP): fossil fuels')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'eutrophication', 'eutrophication (fate not incl.)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'human toxicity', 'human toxicity (HTP inf)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'material resources: metals/minerals', 'abiotic depletion potential (ADP): elements (ultimate reserves)')
        ('ecoinvent-3.9.1', 'CML v4.8 2016', 'ozone depletion', 'ozone layer depletion (ODP steady state)')
Use `list(this object)` to get the complete list.
'''