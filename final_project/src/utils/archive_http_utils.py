import json
import os
def get_ids_from_path(json_data, target_main_cat, target_sub_cat, target_region):
    """
    Traverses the SMARD-style JSON to find data_ids based on a path string.
    
    Args:
        json_data (dict): The loaded JSON data.
        target_main_cat (string): the main catagory.
        target_sub_cat (string): sub catagory.
        target_region (string): DE, DE-LU, DE-LU-AU

    
    Returns:
        list: A list of found data_ids (integers).
    """
    target_main_cat = f"MM-Name.{target_main_cat}"
    target_sub_cat = f"MM-Name.{target_sub_cat}"

    found_ids = []

    # 2. Start traversing the 'main' list
    main_categories = json_data.get('main', [])
    
    for main in main_categories:
        # Check if the target category is part of the name (e.g. "Stromerzeugung" in "MM-Name.Stromerzeugung")
        if target_main_cat == main.get('name', ''):
            
            # 3. Traverse the 'sub' list
            sub_categories = main.get('sub', [])
            for sub in sub_categories:
                if target_sub_cat == sub.get('name', ''):
                    
                    # 4. Dig into module -> other
                    #    (The data is nested inside 'module' dictionary, under key 'other')

                    module_data = sub.get('module', {})
                    
                    defaults = module_data.get('default', [])
                    others = module_data.get('other', [])
                    
                    # Combine both lists
                    modules = defaults + others                   
                    
                    # 5. Filter the modules by Region
                    for mod in modules:
                        # The 'region' field is a list like ["DE", "AT", "DE-LU"]
                        # We check if our target_region exists in that list.
                        if target_region in mod.get('region', []):
                            
                            # Success! Add the ID to our list.
                            # We use 'data_id' as that is usually the API key, but you can change to 'id'.
                            found_ids.append(mod.get('id'))
                            
                            # Optional: Print details for debugging
                            #print(f"  [Match] Found '{mod.get('name')}' (ID: {mod.get('id')})")

    return found_ids

def create_payload(start_time, end_time, target_main_cat, target_sub_cat, target_region):
    with open(MARKET_DATA_CONFIG_PATH, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    ids = get_ids_from_path(
        json_data=json_data,
        target_main_cat=target_main_cat,
        target_sub_cat=target_sub_cat,
        target_region=target_region
    )
    payload = {
    "request_form": [{
        "format": "CSV",
        "moduleIds": ids,
        "region": "DE-LU",
        "timestamp_from": start_time,
        "timestamp_to": end_time,
        "type": "discrete",
        "language": "de",
        "resolution": ""
    }]
}
    return payload