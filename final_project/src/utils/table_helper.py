import re
def clean_bq_column_name(name):
    # 1. Handle German Umlauts
    replacements = {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'}
    for char, rep in replacements.items():
        name = name.replace(char, rep)
    
    # 2. Replace everything that isn't a letter, number, or underscore with '_'
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    
    # 3. BigQuery doesn't like double underscores like '__' from ' [MWh]'
    name = re.sub(r'_+', '_', name).strip('_')
    
    return name