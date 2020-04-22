def merge_df(df1,df2):
    admissions = df1.drop('entries', 1)
    for x in range(len(df2.columns)):
        admissions[df2.columns[x]] = df2[df2.columns[x]].values
    return admissions