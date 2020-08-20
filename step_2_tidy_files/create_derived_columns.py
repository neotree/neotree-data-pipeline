import pandas as pd
import numpy as np
from datetime import datetime as dt

def create_columns(table):
    '''
    This function replicates some of the fields created in power bi.
    Create derived fields based on DAX power bi expressions:
    e.g. simple IF(<logical_test>,<value_if_true> [,<value_if_false>])
    e.g. nested IF([Calls]<200,"low",IF([Calls]<300,"medium","high")) 
    Comments indicate which DAX formulas have been used and which expressession have not be replicated due to complexity issues
    For things that couldn't easily be on here the recommendation is to try and use Metabase
    ''' 

    # if you want to show values from ReferredFrom and ReferredFrom2 - not currently done
    # jn_adm_dis['AdmittedFrom.label'] = jn_adm_dis['AdmittedFrom.label'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.label'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.label'])))
    # jn_adm_dis['AdmittedFrom.value'] = jn_adm_dis['AdmittedFrom.value'].mask(pd.isnull, (jn_adm_dis['ReferredFrom.value'].mask(pd.isnull,jn_adm_dis['ReferredFrom2.value'])))
    # print(jn_adm_dis['AdmittedFrom.label'])

    # Create AdmissionSource = IF(ISBLANK(Admissions[AdmittedFrom]); "External Referral"; Admissions[AdmittedFrom])
    table['AdmittedFrom.value'].fillna("ER", inplace=True)
    table['AdmittedFrom.label'].fillna("External Referral", inplace=True)

    # Create EXTERNALSOURCE = IF(ISBLANK(Admissions[AdmittedFrom]); 
    # if(isblank(Admissions[ReferredFrom]); Admissions[ReferredFrom2]; Admissions[ReferredFrom]))
    
    # float("nan") used to make sure nan's are set not a string "nan"
    table['EXTERNALSOURCE.label'] = np.where(table['AdmittedFrom.label'].isnull(),table['AdmittedFrom.label'].mask(pd.isnull, (table['ReferredFrom.label'].mask(pd.isnull,table['ReferredFrom2.label']))),float('nan'))
    table['EXTERNALSOURCE.value'] = np.where(table['AdmittedFrom.value'].isnull(),table['AdmittedFrom.value'].mask(pd.isnull, (table['ReferredFrom.value'].mask(pd.isnull,table['ReferredFrom2.value']))),float('nan'))
    
    # Create ExtSourceOrder = RELATED(ExternalOrder[Order]) - see if this can be done in tool

    # Create GestGroup = if(isblank(Admissions[Gestation]); "N/A"; 
    # if(Admissions[Gestation]<28; "<28"; 
    # if(Admissions[Gestation]<32; "28-32 wks"; 
    # if(Admissions[Gestation]<34; "32-34 wks";
    # if(Admissions[Gestation]<37; "34-36+6 wks"; "Term")))))

    # order of statements matters
    table.loc[table['Gestation.value'].isnull() , 'GestGroup.value'] = float('nan')
    table.loc[table['Gestation.value'] >= 37 , 'GestGroup.value'] = "Term"
    table.loc[table['Gestation.value'] < 37 , 'GestGroup.value'] = "34-36+6 wks"
    table.loc[table['Gestation.value'] < 34 , 'GestGroup.value'] = "32-34 wks"
    table.loc[table['Gestation.value'] < 32 , 'GestGroup.value'] = "28-32 wks"
    table.loc[table['Gestation.value'] < 28 , 'GestGroup.value'] = "<28"

    # Create GestOrder = RELATED(Gestorder[Order]) - see if this can be done in tool

    # Create BWGroup = if(isblank(Admissions[bw-2]); "Unknown"; if(Admissions[bw-2]<1000; "ELBW"; 
    # if(Admissions[bw-2]<1500; "VLBW"; 
    # if(Admissions[bw-2]<2500; "LBW"; 
    # if(Admissions[bw-2]<4000; "NBW"; "HBW")))))

    # order of statements matters
    table.loc[table['BW.value'].isnull() , 'BWGroup.value'] = "Unknown"
    table.loc[table['BW.value'] >= 4000 , 'BWGroup.value'] = "HBW"
    table.loc[table['BW.value'] < 4000 , 'BWGroup.value'] = "NBW"
    table.loc[table['BW.value'] < 2500 , 'BWGroup.value'] = "LBW"
    table.loc[table['BW.value'] < 1500 , 'BWGroup.value'] = "VLBW"
    table.loc[table['BW.value'] < 1000 , 'BWGroup.value'] = "ELBW"

    # Create BWOrder = RELATED(BWOrder[Order]) - see if this can be done in tool

    # Create AWGroup = if(Admissions[AW]<1000; "<1000g"; 
    # if(Admissions[AW]<1500; "1000-1500g"; 
    # if(Admissions[AW]<2500; "1500-2500g"; 
    # if(Admissions[AW]<4000; "2500-4000g"; ">4000g"))))

    # order of statements matters
    table.loc[table['AW.value'] >= 4000 , 'AWGroup.value'] = ">4000g"
    table.loc[table['AW.value'] < 4000 , 'AWGroup.value'] = "2500-4000g"
    table.loc[table['AW.value'] < 2500 , 'AWGroup.value'] = "1500-2500g"
    table.loc[table['AW.value'] < 1500 , 'AWGroup.value'] = "1000-1500g"
    table.loc[table['AW.value'] < 1000 , 'AWGroup.value'] = "<1000g"

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
    table.loc[table['Temperature.value'] >= 41.5 , 'TempGroup.value'] = ">41.5"
    table.loc[table['Temperature.value'] < 41.5 , 'TempGroup.value'] = "40.5-41.5"
    table.loc[table['Temperature.value'] < 40.5 , 'TempGroup.value'] = "39.5-40.5"
    table.loc[table['Temperature.value'] < 39.5 , 'TempGroup.value'] = "38.5-39.5"
    table.loc[table['Temperature.value'] < 38.5 , 'TempGroup.value'] = "37.5-38.5"
    table.loc[table['Temperature.value'] < 37.5 , 'TempGroup.value'] = "36.5-37.5"
    table.loc[table['Temperature.value'] < 36.5 , 'TempGroup.value'] = "35.5-36.5"
    table.loc[table['Temperature.value'] < 35.5 , 'TempGroup.value'] = "34.5-35.5"
    table.loc[table['Temperature.value'] < 34.5 , 'TempGroup.value'] = "33.5-34.5"
    table.loc[table['Temperature.value'] < 33.5 , 'TempGroup.value'] = "32.5-33.5"
    table.loc[table['Temperature.value'] < 32.5 , 'TempGroup.value'] = "31.5-32.5"
    table.loc[table['Temperature.value'] < 31.5 , 'TempGroup.value'] = "30.5-31.5"
    table.loc[table['Temperature.value'] < 30.5 , 'TempGroup.value'] = "<30.5"

    # Create TempThermia = IF(Admissions[Temperature]<36.5; "Hypothermia"; 
    # IF(Admissions[Temperature]<37.5; "Normothermia"; "Hyperthermia"))

    # order of statements matters
    table.loc[table['Temperature.value'] >= 37.5 , 'TempThermia.value'] = "Hyperthermia"
    table.loc[table['Temperature.value'] < 37.5 , 'TempThermia.value'] = "Normothermia"
    table.loc[table['Temperature.value'] < 36.5 , 'TempThermia.value'] = "Hypothermia"

    # Create ThermiaOrder = RELATED(ThermiaOrder[Order]) - see if this can be done in tool

    # Create <28wks/1kg = AND(Admissions[bw-2]<> Blank(); OR(Admissions[bw-2]<1000; Admissions[Gestation]<28))

    table['<28wks/1kg.value'] = ((table['BW.value'] > 0) & ((table['BW.value'] < 1000) | (table['Gestation.value'] < 28)))

    # Create LBWBinary = AND(Admissions[bw-2]<> Blank();(Admissions[bw-2]<2500))

    table['LBWBinary'] = ((table['BW.value'] > 0) & (table['BW.value'] < 2500))

    # Create YearMonth = FORMAT(Admissions[DateTimeAdmission];"yy-mm")
    #table['YearMonth.value'] = table['DateTimeAdmission.value'].apply(lambda x: pd.Timestamp(x).strftime('%y-%m'))
    #table['DateAdmission.value'] = table['DateTimeAdmission.value'].dt.date
    
    return table