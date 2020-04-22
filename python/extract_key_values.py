def get_key_values(admin_raw):
    
    # Will store the final list of reformed key-value pairs
    newentrieslist = []
    # m is the number of rows that need to be iterated (5 because of limit statement in sql)
    m = len(admin_raw) # 2623
    # loop to iterate through rows (5)
    for y in range(m):
        # n is the number of key value pairs to iterate through in each row (105)
        n = len(admin_raw['entries'][y])
        # dict to store all of the key value pairs for each row
        newentriesdict = {}
        # loop to iterate through 105 ket value pairs and add to dict
        for x in range(n):
            # new branch to extract MCL
            if len(admin_raw['entries'][y][x]['values']) > 1:
                new_value={}
                current_value = admin_raw['entries'][0][14]['values']
                # code to refactor MCL values
                for k,v in [(key,d[key]) for d in current_value for key in d]:
                    if k not in new_value: new_value[k]=[v]
                    else: new_value[k].append(v)
                newentriesdict[admin_raw['entries'][y][x]['key']] = new_value
            # branch to cater for values with nothing in it
            elif len(admin_raw['entries'][y][x]['values']) == 0:
                newentriesdict[admin_raw['entries'][y][x]['key']] = admin_raw['entries'][y][x]['values']
            else:
                # branch to extract non MCL
                newentriesdict[admin_raw['entries'][y][x]['key']] = admin_raw['entries'][y][x]['values'][0]
        # for each row add all the key value pairs
        newentrieslist.append(newentriesdict)     

    return newentrieslist

