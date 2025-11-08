def assign_activity_type(data: list) -> list:
    """creates a field with activity type, which is either transforming activity
    or market activity, based on the code. If starts by M_ is a market.

    Args:
        data (list): _description_

    Returns:
        list: _description_
    """
    for ds in data:

        if ds["code"].startswith("M_"):
            activity_type = "market activity"
        else:
            activity_type = "transforming activity"

        ds["activity type"] = activity_type

    return data


def mapb3():

    # mapping between bonsai 4 biosphere codes and biosphere3 uuids
    # TODO: this should come from the public package on bonsai.
    mapping_b3 = {
        # air emissions
        "Carbon_dioxide__fossil_Air": "349b29d1-3e58-4c66-98b9-9d1a076efd2e",
        "Carbon_dioxide__biogenic_Air": "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7",
        "Methane__fossil_Air": "0795345f-c7ae-410c-ad25-1845784c75f5",
        "Methane__biogenic_Air": "da1157e2-7593-4dfd-80dd-a3449b37a4d8",
        "Dinitrogen_monoxide_Air": "20185046-64bb-4c09-a8e7-e8a9e144ca98",
        "Cadmium_Air": "1c5a7322-9261-4d59-a692-adde6c12de92",
        "Carbon_monoxide__fossil_Air": "ba2f3f82-c93a-47a5-822a-37ec97495275",
        "Ammonia_Air": "87883a4e-1e3e-4c9d-90c0-f1bea36f8014",
        "Arsenic_Air": "7348af7c-e102-4a03-a0df-efce16469eed",
        "Mercury_Air": "71234253-b3a7-4dfe-b166-a484ad15bee7",
        "NMVOC__non_methane_volatile_organic_compounds_Air": "d3260d0e-8203-4cbb-a45a-6a13131a5108",
        "Zinc_Air": "5ce378a0-b48d-471c-977d-79681521efde",
        "Nitrogen_oxides_Air": "c1b91234-6f24-417b-8309-46111d09c457",
        "Sulfur_dioxide_Air": "fd7aa71c-508c-480d-81a6-8052aad92646",
        "Lead_Air": "8e123669-94d3-41d8-9480-a79211fe7c43",
        "Nickel_Air": "a5506f4b-113f-4713-95c3-c819dde6e48b",
        "Copper_Air": "ec8144d6-d123-43b1-9c17-a295422a0498",
        "PAH__polycyclic_aromatic_hydrocarbons_Air": "3e5d7d91-67d7-4afb-91dd-36fab58e4685",
        "PCBs_Air": "c9b97088-efc6-43c1-8d26-f78d6cdbb50b",
        "Particulates____10_um_Air": "66020d27-7ae4-4e59-83a3-89214b72c40a",
        "Particulates____2_5_um_Air": "21e46cb8-6233-4c99-bac3-c41d2ab99498",
        "Selenium_Air": "454c61fd-c52b-4a04-9731-f141bb7b5264",
        "Dioxin__2_3_7_8_Tetrachlorodibenzo_p__Air": "082903e4-45d8-4078-94cb-736b15279277",
        "Benzene__hexachloro__Air": "04f42988-6207-4d09-a91f-155be8d27eb0",
        "Benzo_a__Air": "5e13c2ab-5466-4ff5-816d-702dfdf25f76",
        "Benzo_b__Air": "35357464-0d86-4bbd-940f-7d0dd8e5df57",
        "Benzo_k__Air": "cc5f1637-8aa5-442f-a8c6-43c8739944a0",
        "Polychlorinated_biphenyls_Air": "c9b97088-efc6-43c1-8d26-f78d6cdbb50b",
        "Indeno_1_2_3_cd_pyrene_Air": "76608f33-7127-47f2-9718-383b3efe3b43",
        "Nitrogen__total_Soil": "b748f6f1-7061-4243-89c7-3f2d01dcec07",
        "Nitrogen__total_Water": "dcfe0815-6fa3-4e1d-a55e-155b29904f1d",
        "PCDD_F_Air": "c9b97088-efc6-43c1-8d26-f78d6cdbb50b",
        "Phosphorus__total_Soil": "8b0a4a41-c65c-4d94-b10c-94ddb98abdd2",
        "Phosphorus__total_Water": "2d4b8ec1-8d53-4e62-8a11-ebc45909b02e",
        "TSP_Air": "094310bb-49db-5b2d-ae1b-e7b4ffca1d03",
        # land use (pint may struggle to convert this)
        "Arable_land": "8c173ca1-5f74-4a6e-89e5-dd18e0f18d1a",
        "Pasture_Land": "59ded913-17fe-4b3e-80cb-79b97cdbef9a",
        # ores
        "Asbestos": "c5f5aeb8-7558-4a0c-9594-27621b9cfbc5",
        "Arsenic": "e16fd15c-0ebc-55ba-8d3b-9704f13663cb",
        "Cadmium": "bf377e4f-3a95-4ce2-a9ba-66ee31f00f60",
        "Antimony___associated_ore": "3e0034cd-21d6-4582-9fbf-09c26edd05df",
        "Abrasives__natural__puzzolan__pumice__volcanic_cinder_etc__": "4402f445-984c-4728-be22-6f9aea1146b9",
        "Ball_clay": "f7519ca9-5ffc-41c3-a33e-806da82cfc0e",
        #'Barite':'c13beafb-2aed-4a52-b09a-78d28913b6ce', dissapeared in ecoinvent 3.10
        "Bentonite__sepiolite_and_attapulgite": "93806a54-46f5-409c-99c5-4144a1e73b5d",
        "Bismuth___associated_ore": "0124b342-4bdd-5cbf-ba2a-dce8a259755c",
        "Borate_minerals": "cb7e3e15-ab58-4ef4-8cd0-361511d3b5fe",
        "Bromine": "45d6f26b-596b-5182-8c08-d6d975ff4efe",
        "Cadmium": "3937f5a5-b4a4-434d-9fc3-1d29064fc3f8",
        "Calcite": "99ee393d-4bd1-4cc8-b0a0-d956865fb7bf",
        "Calcium_carbonate": "99ee393d-4bd1-4cc8-b0a0-d956865fb7bf",
        "Chromium___associated_ore": "1c87de06-e58f-4684-a54c-d29f1a251a87",
        "Cobalt___associated_ore": "d0779a5e-6969-4144-954e-ceb81fb83f15",
        "Common_clay__clay_for_bricks_etc_": "f7519ca9-5ffc-41c3-a33e-806da82cfc0e",
        "Copper___associated_ore": "a9ac40a0-9bea-4c48-afa7-66aa6eb90624",
        "Diatomite": "9877ce00-65f8-4c0c-9fcf-92aa53a2c9c0",
        "Dolomite": "c7aee986-b7d8-4ad9-ad45-1ac0d68e6b78",
        "Feldspar": "26296ec9-ff93-41e6-bbbf-6175af04284d",
        "Fluorspar": "0fa4f51e-b0dc-5d11-84d3-b32f0f3c88d5",
        "Gallium": "0878c1c6-4c1d-4f90-a2de-a9383855d5c6",
        "Germanium": "d3e547dc-1a29-5ece-8dbb-bd9c0ad3cc46",
        "Gold___associated_ore": "d080e6a4-42c6-484e-b5d7-d74693aec7d9",
        "Graphite__natural": "5666353e-2db2-41d3-8414-404709151422",
        "Gypsum_and_anhydrite": "11a2a7b1-ab2f-47b8-9e29-6f33d5207fa6",
        "Igneous_rock__basalt__basaltic_lava__diabase__granite__porphyry__etc__": "ac3a8914-35f0-4c34-a956-f26b3a053e4a",
        "Indium": "7aaf1a4e-f72f-5dc6-b999-de4e99948eb8",
        "Industrial_sand": "423ef039-6057-4f63-94bd-e9410d024bd0",
        "Iodine": "36a3d172-7373-507f-85bd-12b8ba31a6d4",
        "Iron___associated_ore": "8ce3ff02-7a1e-48e3-881e-3248b944f28a",
        "Kaolin": "5e86b7ae-1d51-485c-be33-8c12c4ce4d2e",
        "Laterite": "86fb18d4-a425-407a-94bc-194254e4d7d7",
        "Lead___associated_ore": "fbcb9c7a-eea7-4694-ba6c-568e01d28883",
        "Lithium_ore": "7d2c1cdd-a64a-5936-a577-5b82db0c0d1b",
        "Magnesite": "a4bab069-74a9-5b4c-8d6e-5ca984cd9ecd",
        "Magnesium": "9e5823ad-9d9b-4b98-b627-e39611b6a8bd",
        "Manganese___associated_ore": "c2586875-bb56-4b1e-84c5-5ff255a1108b",
        "Manganese_ore": "c2586875-bb56-4b1e-84c5-5ff255a1108b",
        "Mercury___associated_ore": "54b9cbd0-65df-4fd3-8a19-dd3b8eccc619",
        "Molybdenum___associated_ore": "e5a3dff5-72dc-5287-893c-597dd4a19566",
        "Nickel___associated_ore": "974213ef-1ba0-40e5-bc7b-52ef099e9e09",
        "Niobium___associated_ore": "8de8befc-efa2-5d07-a58b-b29ea97a3f41",
        "Palladium___associated_ore": "edc69c63-a776-4dbf-acbf-e0368914980a",
        "Perlite": "09a68c14-01f6-4dee-ba29-8b7f400b72b5",
        "Phosphate_rock": "329fc7d8-4011-4327-84e4-34ff76f0e42d",
        "Platinum___associated_ore": "d13b2665-505d-49e2-8edd-dc966b0342af",
        "Potter_clay": "f7519ca9-5ffc-41c3-a33e-806da82cfc0e",
        "Pyrite": "c73e75dc-c02d-4192-ab43-faf29c119fae",
        "Rhenium": "a2e6fb74-b047-5697-b5dd-e28cc68f29e6",
        "Rhodium___associated_ore": "4803f22f-6950-489b-914d-fa953a8081f6",
        "Sand_and_gravel": "423ef039-6057-4f63-94bd-e9410d024bd0",
        "Selenium": "5f47f918-1c32-5870-b992-db91f843ff34",
        "Silica_Sand": "423ef039-6057-4f63-94bd-e9410d024bd0",
        "Silver___associated_ore": "361a64cb-ab76-4a72-9ea1-c07d6a20c124",
        "Strontium_mineral": "0f1b21d0-2780-4742-87f2-28fb21a44db5",
        "Sulphur_ore": "852281f6-db73-4250-84d3-86b569fce0c1",
        "Talc__steatite__soapstone__pyrophyllite_": "bc97531c-12d8-4113-bcb2-663a47d12d0f",
        "Tantalum___associated_ore": "775fdf03-b0bb-5c25-b14d-107231d5b2f0",
        "Tellurium___associated_ore": "7b6da1f2-e191-5a77-ae06-af96201f5803",
        "Tin___associated_ore": "53d5ef26-66d8-4536-afa2-2f6b114189ba",
        "Titanium___associated_ore": "2f033407-6060-4e1e-868c-9f362d10fdb2",
        "Titanium_ore": "2f033407-6060-4e1e-868c-9f362d10fdb2",
        "Tungsten___associated_ore": "ebcc1f0c-6b19-501d-86a4-629df2a457b5",
        "Uranium___associated_ore": "2ba5e39b-adb6-4767-a51d-90c1cf32fe98",
        "Vanadium___associated_ore": "c9c4b80a-73dd-415a-92fb-f877595651c1",
        "Vermiculite": "bea19217-6a28-4711-8142-2e71090c0b46",
        "Zinc___associated_ore": "be73218b-18af-492e-96e6-addd309d1e32",
        #'Zircon':'fcee6eab-e906-4ddf-bc14-2b131b937893' ecoinvent 3.9.1
        "Zircon": "cd2932c5-a486-4bf1-99b8-815d8a7ce11a",
    }

    return mapping_b3
