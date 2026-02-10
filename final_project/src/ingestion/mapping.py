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
    


    found_ids = []

    # 2. Start traversing the 'main' list
    main_categories = json_data.get('main', [])
    
    for main in main_categories:
        # Check if the target category is part of the name (e.g. "Stromerzeugung" in "MM-Name.Stromerzeugung")
        if target_main_cat in main.get('name', ''):
            
            # 3. Traverse the 'sub' list
            sub_categories = main.get('sub', [])
            for sub in sub_categories:
                if target_sub_cat in sub.get('name', ''):
                    
                    # 4. Dig into module -> other
                    #    (The data is nested inside 'module' dictionary, under key 'other')
                    modules = sub.get('module', {}).get('other', [])
                    
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

# --- Main Execution Block ---
if __name__ == "__main__":
    
    filename = 'config/market_data_configuration.json'
    
    # Check if file exists first
    if not os.path.exists(filename):
        print(f"Error: The file '{filename}' was not found.")
    else:
        try:
            # 1. Load the JSON file
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 2. Define your search query
            #    You can change "DE" to "LU" or "DE-LU" depending on what you need.
            
            print("Searching for: 'Stromerzeugung/Realisierte Erzeugung/DE-LU'...\n")
            
            # 3. Run the function
            ids = get_ids_from_path(data, "Stromerzeugung", "Realisierte Erzeugung", "DE-LU")
            
            # 4. Show results
            print("-" * 30)
            print(f"Result List of IDs: {ids}")
            print("-" * 30)

        except json.JSONDecodeError:
            print(f"Error: '{filename}' is not a valid JSON file.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")