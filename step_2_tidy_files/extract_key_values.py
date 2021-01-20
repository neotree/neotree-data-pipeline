# This module extracts the key-value pairs within a raw json file.
import step_2_tidy_files.json_restructure as jr


def get_key_values_adm(data_raw):
    mcl = []
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, rows in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        new_entries = {}
        # add uid and ingested_at first
        new_entries['uid'] = index
        if 'ingested_at_admission' in rows:
            new_entries['ingested_at'] = rows['ingested_at_admission']
        if 'ingested_at_discharge' in rows:
            new_entries['ingested_at'] = rows['ingested_at_discharge']
        # iterate through key, value and add to dict
        for c in rows['entries']:
            # call resturcture function to manage MCL, zero & single values
            k, v, mcl = jr.restructure_admissions(c, mcl)
            new_entries[k] = v
        # for each row add all the keys & values to a list
        data_new.append(new_entries)

    return data_new, set(mcl)

def get_key_values_disc(data_raw):
    mcl = []
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, rows in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        new_entries = {}
        # add uid and ingested_at first
        new_entries['uid'] = index
        if 'ingested_at_admission' in rows:
            new_entries['ingested_at'] = rows['ingested_at_admission']
        if 'ingested_at_discharge' in rows:
            new_entries['ingested_at'] = rows['ingested_at_discharge']
        # iterate through key, value and add to dict
        for c in rows['entries']:
            # call resturcture function to manage MCL, zero & single values
            k, v, mcl = jr.restructure_discharges(c, mcl)
            new_entries[k] = v
        # for each row add all the keys & values to a list
        data_new.append(new_entries)

    return data_new, set(mcl)

