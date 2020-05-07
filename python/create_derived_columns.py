import pandas as pd

def create_columns(jn_adm_dis):
    # All derived features added to join adm dis table.
    # Create derived features based on DAX power bi expressions:
    # e.g. simple IF(<logical_test>,<value_if_true> [,<value_if_false>])
    # e.g. nested IF([Calls]<200,"low",IF([Calls]<300,"medium","high"))  

    # Create DateAdmission  = Admissions[DateTimeAdmission].[Date]
    from datetime import datetime as dt
    # watch out for time zone (tz) issues if you change code (ref: https://github.com/pandas-dev/pandas/issues/25571)
    jn_adm_dis['DateTimeAdmission.value'] =  pd.to_datetime(jn_adm_dis['DateTimeAdmission.value'], format ='%Y-%m-%dT%H:%M:%S' , utc=True)
    jn_adm_dis['DateAdmission.value'] = jn_adm_dis['DateTimeAdmission.value'].dt.date
    #print("1. ", jn_adm_dis['DateAdmission.value'])


    # if you want to show values from ReferredFrom and ReferredFrom2 - not currently done
    # jn_adm_dis['AdmittedFrom.label'] = jn_adm_dis['AdmittedFrom.label'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.label'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.label'])))
    # jn_adm_dis['AdmittedFrom.value'] = jn_adm_dis['AdmittedFrom.value'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.value'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.value'])))
    # print(jn_adm_dis['AdmittedFrom.label'])

    # Create AdmissionSource = IF(ISBLANK(Admissions[AdmittedFrom]); "External Referral"; Admissions[AdmittedFrom])
    jn_adm_dis['AdmittedFrom.value'].fillna("ER", inplace=True)
    jn_adm_dis['AdmittedFrom.label'].fillna("External Referral", inplace=True)
    #print("2. ",jn_adm_dis['AdmittedFrom.value'])
    #print("3. ",jn_adm_dis['AdmittedFrom.label'])

    # Create EXTERNALSOURCE = IF(ISBLANK(Admissions[AdmittedFrom]); 
    # if(isblank(Admissions[ReferredFrom]); Admissions[ReferredFrom2]; Admissions[ReferredFrom]))
    import numpy as np
    # float("nan") used to make sure nan's are set not a string "nan"
    jn_adm_dis['EXTERNALSOURCE.label'] = np.where(jn_adm_dis['AdmittedFrom.label'].isnull(),jn_adm_dis['AdmittedFrom.label'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.label'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.label']))),float('nan'))
    jn_adm_dis['EXTERNALSOURCE.value'] = np.where(jn_adm_dis['AdmittedFrom.value'].isnull(),jn_adm_dis['AdmittedFrom.value'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.value'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.value']))),float('nan'))
    #print("4. ",jn_adm_dis['EXTERNALSOURCE.label'])
    #print("5. ",jn_adm_dis['EXTERNALSOURCE.value'])

    # Create ExtSourceOrder = RELATED(ExternalOrder[Order]) - see if this can be done in tool

    # Create GestGroup = if(isblank(Admissions[Gestation]); "N/A"; 
    # if(Admissions[Gestation]<28; "<28"; 
    # if(Admissions[Gestation]<32; "28-32 wks"; 
    # if(Admissions[Gestation]<34; "32-34 wks";
    # if(Admissions[Gestation]<37; "34-36+6 wks"; "Term")))))

    # order of statements matters
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'].isnull() , 'GestGroup.value'] = float('nan')
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'] >= 37 , 'GestGroup.value'] = "Term"
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'] < 37 , 'GestGroup.value'] = "34-36+6 wks"
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'] < 34 , 'GestGroup.value'] = "32-34 wks"
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'] < 32 , 'GestGroup.value'] = "28-32 wks"
    jn_adm_dis.loc[jn_adm_dis['Gestation.value'] < 28 , 'GestGroup.value'] = "<28"
    #print("6. ", jn_adm_dis['GestGroup.value'])


    # Create GestOrder = RELATED(Gestorder[Order]) - see if this can be done in tool

    # Create BWGroup = if(isblank(Admissions[bw-2]); "Unknown"; if(Admissions[bw-2]<1000; "ELBW"; 
    # if(Admissions[bw-2]<1500; "VLBW"; 
    # if(Admissions[bw-2]<2500; "LBW"; 
    # if(Admissions[bw-2]<4000; "NBW"; "HBW")))))

    # order of statements matters
    jn_adm_dis.loc[jn_adm_dis['BW.value'].isnull() , 'BWGroup.value'] = "Unknown"
    jn_adm_dis.loc[jn_adm_dis['BW.value'] >= 4000 , 'BWGroup.value'] = "HBW"
    jn_adm_dis.loc[jn_adm_dis['BW.value'] < 4000 , 'BWGroup.value'] = "NBW"
    jn_adm_dis.loc[jn_adm_dis['BW.value'] < 2500 , 'BWGroup.value'] = "LBW"
    jn_adm_dis.loc[jn_adm_dis['BW.value'] < 1500 , 'BWGroup.value'] = "VLBW"
    jn_adm_dis.loc[jn_adm_dis['BW.value'] < 1000 , 'BWGroup.value'] = "ELBW"
    #print("7. ", jn_adm_dis['BWGroup.value'])

    # Create BWOrder = RELATED(BWOrder[Order]) - see if this can be done in tool

    # Create AWGroup = if(Admissions[AW]<1000; "<1000g"; 
    # if(Admissions[AW]<1500; "1000-1500g"; 
    # if(Admissions[AW]<2500; "1500-2500g"; 
    # if(Admissions[AW]<4000; "2500-4000g"; ">4000g"))))

    # order of statements matters
    jn_adm_dis.loc[jn_adm_dis['AW.value'] >= 4000 , 'AWGroup.value'] = ">4000g"
    jn_adm_dis.loc[jn_adm_dis['AW.value'] < 4000 , 'AWGroup.value'] = "2500-4000g"
    jn_adm_dis.loc[jn_adm_dis['AW.value'] < 2500 , 'AWGroup.value'] = "1500-2500g"
    jn_adm_dis.loc[jn_adm_dis['AW.value'] < 1500 , 'AWGroup.value'] = "1000-1500g"
    jn_adm_dis.loc[jn_adm_dis['AW.value'] < 1000 , 'AWGroup.value'] = "<1000g"
    #print("8. ", jn_adm_dis['AWGroup.value'])

    # Create AWOrder = RELATED(AWOrder[Order]) - see if this can be done in tool

    # Create AgeCatOrder = RELATED(AgeCatGroup[Order]) - see if this can be done in tool

    # Create TempGroup = if(Admissions[Temperature]<30.5; "<30.5"; 
    # if(Admissions[Temperature]<31.5; "30.5-31.5"; 
    # if(Admissions[Temperature]<32.5; "31.5-32.5"; 
    # IF(Admissions[Temperature]<33.5; "32.5-33.5"; 
    # IF(Admissions[Temperature]<34.5; "33.5-34.5"; 
    # IF(Admissions[Temperature]<35.5; "34.5-35.5"; 
    # IF(Admissions[Temperature]<36.5; "35.5-36.5"; 
    # IF(Admissions[Temperature]<37.5; "36.5-37.5"; 
    # IF(Admissions[Temperature]<38.5; "37.5-38.5"; 
    # IF(Admissions[Temperature]<39.5; "38.5-39.5"; 
    # IF(Admissions[Temperature]<40.5; "39.5-40.5";
    # IF(Admissions[Temperature]<41.5; "40.5-41.5"; ">41.5"))))))))))))

    # order of statements matters
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] >= 41.5 , 'TempGroup.value'] = ">41.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 41.5 , 'TempGroup.value'] = "40.5-41.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 40.5 , 'TempGroup.value'] = "39.5-40.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 39.5 , 'TempGroup.value'] = "38.5-39.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 38.5 , 'TempGroup.value'] = "37.5-38.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 37.5 , 'TempGroup.value'] = "36.5-37.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 36.5 , 'TempGroup.value'] = "35.5-36.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 35.5 , 'TempGroup.value'] = "34.5-35.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 34.5 , 'TempGroup.value'] = "33.5-34.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 33.5 , 'TempGroup.value'] = "32.5-33.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 32.5 , 'TempGroup.value'] = "31.5-32.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 31.5 , 'TempGroup.value'] = "30.5-31.5"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 30.5 , 'TempGroup.value'] = "<30.5"
    #print("9. ", jn_adm_dis['TempGroup.value'])

    # Create TempThermia = IF(Admissions[Temperature]<36.5; "Hypothermia"; 
    # IF(Admissions[Temperature]<37.5; "Normothermia"; "Hyperthermia"))

    # order of statements matters
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] >= 37.5 , 'TempThermia.value'] = "Hyperthermia"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 37.5 , 'TempThermia.value'] = "Normothermia"
    jn_adm_dis.loc[jn_adm_dis['Temperature.value'] < 36.5 , 'TempThermia.value'] = "Hypothermia"
    #print("10. ", jn_adm_dis['TempThermia.value'])

    # Create ThermiaOrder = RELATED(ThermiaOrder[Order]) - see if this can be done in tool

    # Create <28wks/1kg = AND(Admissions[bw-2]<> Blank(); OR(Admissions[bw-2]<1000; Admissions[Gestation]<28))

    jn_adm_dis['<28wks/1kg.value'] = ((jn_adm_dis['BW.value'] > 0) & ((jn_adm_dis['BW.value'] < 1000) | (jn_adm_dis['Gestation.value'] < 28)))
    #print("11. ", jn_adm_dis['<28wks/1kg.value'])

    # Create LBWBinary = AND(Admissions[bw-2]<> Blank();(Admissions[bw-2]<2500))

    jn_adm_dis['LBWBinary'] = ((jn_adm_dis['BW.value'] > 0) & (jn_adm_dis['BW.value'] < 2500))
    #print("12. ", jn_adm_dis['LBWBinary'])

    # Create YearMonth = FORMAT(Admissions[DateTimeAdmission];"yy-mm")
    jn_adm_dis['YearMonth.value'] = jn_adm_dis['DateTimeAdmission.value'].apply(lambda x: pd.Timestamp(x).strftime('%y-%m'))
    #print("13. ", jn_adm_dis['YearMonth.value'])
    
    return jn_adm_dis