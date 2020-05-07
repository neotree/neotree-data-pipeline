# This module extracts the key-value pairs within a raw json file.

def get_key_values(data_raw):
    import json_restructure as jr
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, rows in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        new_entries = {}
        # add uid and ingested_at first
        new_entries['uid'] = index
        new_entries['ingested_at'] = rows['ingested_at']
        # iterate through key, value and add to dict
        for c in rows['entries']:
            # call resturcture function to manage MCL, zero & single values
            k,v = jr.restructure(c)
            new_entries[k] = v
        # for each row add all the keys & values to a list
        data_new.append(new_entries)   
        
    return data_new
