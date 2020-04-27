# This module extracts the key-value pairs within a raw json file.
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)

def get_key_values(data_raw):
    
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    # n_rows is the number of rows that need to be iterated
    n_rows = len(data_raw) 
    for r in range(n_rows):
        # n_col is the number of key value pairs to iterate through in each row 
        n_col = len(data_raw['entries'][r])
        # dict to store all of the structured key value pairs for each row
        new_entries = {}
        # add uid and ingested_at before restructuring the json data
        new_entries['uid'] = data_raw.index.get_level_values(0).values[r]
        new_entries['ingested_at'] = data_raw.iloc[r]['ingested_at']
        # loop to iterate through key value pairs and add to dict
        for c in range(n_col):
            # branch to manage MCL
            if len(data_raw['entries'][r][c]['values']) > 1:
                mcl_v={}
                current_v = data_raw['entries'][r][c]['values']
                # code to restructure MCL json file to key value pairs format
                for k,v in [(key,d[key]) for d in current_v for key in d]:
                    if k not in mcl_v: mcl_v[k]=[v]
                    else: mcl_v[k].append(v)
                k = data_raw['entries'][r][c]['key']
                new_entries[k] = mcl_v
            # branch to cater for empty values
            elif len(data_raw['entries'][r][c]['values']) == 0:
                k = data_raw['entries'][r][c]['key']
                zero_v = data_raw['entries'][r][c]['values']
                new_entries[k] = zero_v
            else:
                # branch to extract single entry values
                k = data_raw['entries'][r][c]['key']
                v = data_raw['entries'][r][c]['values'][0]
                new_entries[k] = v
        # for each row add all the key value pairs to a list
        data_new.append(new_entries)    
        
    return data_new

