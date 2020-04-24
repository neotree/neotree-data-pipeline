# This module extracts the key-value pairs within a raw json file.
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)

def get_key_values(data_raw):
    
    # Will store the final list of reformed key-value pairs
    data_new = []
    # n_rows is the number of rows that need to be iterated
    n_rows = len(data_raw) 
    # loop to iterate through rows
    for row in range(n_rows):
        # n_col is the number of key value pairs to iterate through in each row 
        n_col = len(data_raw['entries'][row])
        # dict to store all of the key value pairs for each row
        new_entries = {}
        # loop to iterate through key value pairs and add to dict
        for col in range(n_col):
            # branch to extract MCL
            if len(data_raw['entries'][row][col]['values']) > 1:
                new_value={}
                current_value = data_raw['entries'][row][col]['values']
                # code to refactor MCL values
                for k,v in [(key,d[key]) for d in current_value for key in d]:
                    if k not in new_value: new_value[k]=[v]
                    else: new_value[k].append(v)
                new_entries[data_raw['entries'][row][col]['key']] = new_value
            # branch to cater for empty values
            elif len(data_raw['entries'][row][col]['values']) == 0:
                new_entries[data_raw['entries'][row][col]['key']] = data_raw['entries'][row][col]['values']
            else:
                # branch to extract single entry values
                new_entries[data_raw['entries'][row][col]['key']] = data_raw['entries'][row][col]['values'][0]
        # for each row add all the key value pairs
        data_new.append(new_entries)     

    return data_new

