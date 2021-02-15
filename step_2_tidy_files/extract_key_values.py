# This module extracts the key-value pairs within a raw json file.
import step_2_tidy_files.json_restructure as jr


def get_key_values(data_raw):
    mcl = []
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, rows in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        new_entries = {}
        # add uid and ingested_at first
        app_version = rows['appVersion']
        if(app_version!=None and app_version!=''):
            app_version = int(app_version.replace('.', ""))

        new_entries['uid'] = index
        if 'ingested_at_admission' in rows:
            new_entries['ingested_at'] = rows['ingested_at_admission']
        if 'ingested_at_discharge' in rows:
            new_entries['ingested_at'] = rows['ingested_at_discharge']

        # iterate through key, value and add to dict
        for c in rows['entries']:
           
            #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
            if(app_version!='' and app_version!=None and app_version>454):   
                k, v, mcl = jr.restructure_new_format(c,rows['entries'][c], mcl)     
            #ELSE USE THE OLD FORMAT
            else:
               k, v, mcl = jr.restructure(c, mcl)
               
            new_entries[k] = v
        # for each row add all the keys & values to a list
        data_new.append(new_entries)

    return data_new, set(mcl)

