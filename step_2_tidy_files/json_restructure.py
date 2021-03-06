# This module manages different value cases
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)

def restructure(c, mcl):
    # branch to manage MCL
    if len(c['values']) > 1:
        v = {}
        current_v = c['values']
        # code to restructure MCL json file to key value pairs format
        for k, val in [(key, d[key]) for d in current_v for key in d]:
            if k not in v:
                v[k] = [val]
            else:
                v[k].append(val)
        k = c['key']
        mcl.append(k)

    # branch to cater for empty values
    elif len(c['values']) == 0:
        k = c['key']
        v = c['values']

    # branch to extract single entry values
    else:
        k = c['key']
        v = c['values'][0]

    return k, v, mcl
