# This module extracts the key-value pairs within a raw json file.
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)

def get_key_values(data_raw):
    
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []

    for index, rows in data_raw.iterrows():
        # dict to store all of the structured key value pairs for each row
        new_entries = {}
        # add uid and ingested_at before restructuring the json data
        new_entries['uid'] = index
        new_entries['ingested_at'] = rows['ingested_at']
        # loop to iterate through key value pairs and add to dict
        for c in rows['entries']:
            # branch to manage MCL
            if len(c['values']) > 1:
                mcl_v={}
                current_v = c['values']
                # code to restructure MCL json file to key value pairs format
                for k,v in [(key,d[key]) for d in current_v for key in d]:
                    if k not in mcl_v: mcl_v[k]=[v]
                    else: mcl_v[k].append(v)
                k = c['key']
                new_entries[k] = mcl_v
            # branch to cater for empty values
            elif len(c['values']) == 0:
                k = c['key']
                zero_v = c['values']
                new_entries[k] = zero_v
            else:
                # branch to extract single entry values
                k = c['key']
                v = c['values'][0]
                new_entries[k] = v
        # for each row add all the key value pairs to a list
        data_new.append(new_entries)    
        
    return data_new

