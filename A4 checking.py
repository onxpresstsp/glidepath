# -*- coding: utf-8 -*-
"""
Created on Mon Jan  1 13:09:29 2024

@author: MatthewTsang
"""

##
# finish cleaning up 660 related flow.
# next step:
#     - add cs1_tt clean into the flow
#     - add changing sheet_name into the flow
    

##

# IMPORT LIBRARIES
import os
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from time import sleep


# FUNCTIONS
#============================================================================
#============================================================================
#============================================================================
# READ EXCEL FILE & GET ALL SHEET NAMES
def get_sheet_names_and_equiv(crn_tt, wb_name):
    if crn_tt == '660':
        sheet_names = pd.ExcelFile(wb_name).sheet_names
        sheet_names_660_equi = sheet_names
    if crn_tt != '660': 
        sheet_names = pd.ExcelFile(wb_name).sheet_names
        sheet_names = [x for x in sheet_names if "bound" in x]
        sheet_names_660_equi = []
        # Define replacement mappings
        replacements = {'Outbound': ' OB', 'Oubound': ' OB', 'Inbound': ' IB', 'SV': 'ST', 'MT': 'MI', 'KN': 'KW'}
        # Apply replacements using list comprehension
        sheet_names_660_equi = ["".join(replacements.get(part, part) for part in category.split('-')) for category in sheet_names]
    return sheet_names, sheet_names_660_equi   

# CREATE A EMPTY DICTIONARY ACCORDING TO sheet_names AND wb_name
def dic_creation(sheet_names,wb_name):
    my_dic = dict()
    record_list = ['df','df_form_formby','df_form_formby','df_rev_trn_crn_sta','summary_jt','df_jt','df_freq','df_count','df_ref_t']
    for record in record_list:
        my_dic[record] = dict()
    return my_dic

# GET REFERENCE FROM sht_name
def sht_name_to_ref(sht_name):
    direction = sht_name.split(" ")[1]
        
    ## Get reference point (Union Arr / Dep)
    if direction == 'IB':
        ref_un = 'Arr'
        ref_sta = 'Dep'
    else:
        ref_un = 'Dep'
        ref_sta = 'Arr'   
    return direction, ref_un, ref_sta

# Ia. CLEAN TIMETABLE (FOR 660)
def clean_tt(df,A4_to_660_station_name_excel,sht_name):
    
    
    '''
    CLEAN TIMETABLE
    1. get the row 5/6 to be column name; remove the row above that
    2. fill collumn B to be the cell above if it is empty
    3. aligning naming convention for concerned stations
    4. make the timing data to be the right format for calculation
    5. Get a separate df: "Business ID", "Forms", "Formed by", export
    '''
    ## 1. Get the row 5/6 to be column name; capture "Business ID", "Forms", "Formed by"; Remove useless rows    
    ### Get the row 5/6 to be column name; Remove the row above that
    df.iloc[4,1] = 'sta'
    df.iloc[4,2] = 'arr_dep'
    df.columns = df.iloc[4].copy()
    
    # df = df_bu.copy()
    
    
    ### capture "Business ID", "Forms", "Formed by"
    df_form_formby = df[df['Business ID'].isin(["Business ID", "Forms", "Formed by"])].copy()
    # df_bu = df.copy()
    ### Remove useless rows
    df = df.loc[5:,:].reset_index(drop = True).copy()
    #### rows with no values (no station value & arr/dep time)
    dd =  df.loc[:, ['sta'] + list(df.columns[3:])].isna().all(axis = 1)
    df = df[~dd].copy()
    
    
    crn_col_for_numeric = 'Business ID'
    # print(1)
    
    df[crn_col_for_numeric] = df[crn_col_for_numeric].apply(pd.to_numeric, errors='coerce').copy()
    # print(1.1)
    # df_bu = df.copy()
    df = df.dropna(subset = [crn_col_for_numeric]).reset_index(drop = True)
    

    # df_bu = df.copy()
    
    ## 2. fill collumn B to be the cell above if it is empty
    df = df.fillna({'sta': df['sta'].ffill()})
    # print(2)
    ## 3. aligning naming convention for concerned stations
    df_sta_name_lut = pd.read_excel(A4_to_660_station_name_excel)
    df_sta_name_lut_sub = df_sta_name_lut.drop_duplicates(subset  = '660_name').dropna(subset  = '660_name').reset_index(drop = True)
    df = df.merge(df_sta_name_lut_sub[['660_name','sta']],left_on = 'sta', right_on = '660_name', how = 'left')
    df['sta'] = np.where(df['660_name'].notna(),df['sta_y'], df['sta_x'])
    df['sta_x'] = df['sta'].copy()
    df = df[df.columns[:-3]].copy()
    df = df.rename(columns = {'sta_x': 'sta'})
    
    # print(3)
    #3.5: for UPE only: Further cleaning for UPE
    
    if sht_name.split(" ")[0] == 'UP':
        #### Preparation
        union_sta_name = 'Union Station'
        direction, ref_un, ref_sta = sht_name_to_ref(sht_name)
        ####  Fill back the arr/dep at Union / Pearson
        df.loc[df.sta=='Pearson', 'arr_dep'] = ref_sta
        df.loc[df.sta==union_sta_name, 'arr_dep'] = ref_un
        #### Format timing value for Pearson & Union Station
        row_pearson = list(df[(df.sta=='Pearson')].index)[0]
        row_union = list(df[(df.sta==union_sta_name)].index)[0]        
        df.loc[row_pearson,df.columns[3:]] = [x[:-2] for x in df.loc[row_pearson,df.columns[3:]]]
        df.loc[row_union,df.columns[3:]] = [x[:-4] for x in df.loc[row_union,df.columns[3:]]]
    
    ## 4. make the timing data to be the right format for calculation
    def basic_conversion_datetime(df, desired_format = '%H.%M.%S'):
        for crn_col in df.columns:
            series_temp = pd.to_datetime(df[crn_col], format = desired_format, errors='coerce')
            df.loc[series_temp.notna(),crn_col] = list(series_temp[series_temp.notna()])
        return df
    df = basic_conversion_datetime(df)
    # print(1)
    df = basic_conversion_datetime(df, desired_format = r'%H/%M.%S')
    # print(1)
    ### if the value shall be +1 day, reflect it on the data
    #### if value is '24.00.00', make the date become +1 day
    for crn_col in df.columns:
        df.loc[df[crn_col] == '24.00.00',crn_col] = pd.to_datetime('1900-01-02 00:00:00')
        df.loc[df[crn_col] == r'24/00.00',crn_col] = pd.to_datetime('1900-01-02 00:00:00')
    
    #### change values for trips that starts before next day, but end in next day
    for crn_col in df.columns:
        desired_format = '%Y-%m-%d %H:%M:%S'
        series_temp = df[crn_col].astype('str')
        series_temp2 = pd.to_datetime(series_temp, format = desired_format, errors='coerce')
        series_temp2_df = series_temp2.reset_index().dropna(subset = crn_col).reset_index(drop = True)
        if len(series_temp2_df) > 0:
            series_temp2_df['neg_col'] = series_temp2_df[crn_col] - series_temp2_df[crn_col].shift(1)
            
            # add 1 day for correct values
            ref_index = list(series_temp2_df[series_temp2_df['neg_col'] < timedelta(seconds = 0)]['index'])
            if len(ref_index)>= 1:
                ref_index = ref_index[0]
                series_temp2_df.loc[series_temp2_df['index']>=ref_index,crn_col] += timedelta(days = 1)
                df.loc[series_temp2_df['index'],crn_col] = list(series_temp2_df.loc[:,crn_col])
    # print(4.2)
    #### change values for trips that starts next day (assuming all those trips starts before 3am)
    for crn_col in df.columns:
        desired_format = '%Y-%m-%d %H:%M:%S'
        series_temp = df[crn_col].astype('str')
        series_temp2 = pd.to_datetime(series_temp, format = desired_format, errors='coerce')  
        series_temp2_df = series_temp2.reset_index().dropna(subset = crn_col).reset_index(drop = True)
        if len(series_temp2_df) > 0:
            if series_temp2_df.loc[0,crn_col].hour < 3:
                series_temp2_df.loc[:,crn_col] += timedelta(days=1)
                df.loc[series_temp2_df['index'],crn_col] = list(series_temp2_df.loc[:,crn_col])
    return df, df_form_formby

# Ib. CLEAN TIMETABLE FOR CS1
def clean_tt_cs1(df,A4_to_660_station_name_excel,sht_name):
    
    # use train number row as column value
    # remove first column
    # replace "|      " by '...'
    # replace any values that starts with "(" and end with ")" as "..."
    # change timestamp value format
    # add arr_dep column
    # add two rows
    # keep conccnered stations only
    
    # use train number row as column value
    df.loc[7,df.columns[1:3]] = ['sta', 'arr_dep']
    df.loc[7,:] = df.loc[7,:].astype('str').copy()
    df.columns = df.loc[7,:].copy()
    # remove first column
    df = df.loc[:,df.columns[1:]].copy()
    df['arr_dep'] = "Dep"
    
    # Save the train-configuration
    df_form_formby = df.loc[8,:].copy()
    
    
    # replace "|      " by '...'
    df.replace("|      ", '…', inplace = True)
    df.replace("  ", np.nan, inplace = True)
    # replace any values that starts with "(" and end with ")" as "..."
    for col in list(df.columns)[2:]:
        con1 = (df.loc[:,col].str[:1] == "(") & (df.loc[:,col].str[-3:] == ")  ")
        df.loc[con1, col] = '…'
        
    # change timestamp value format
    def basic_conversion_datetime(df, desired_format = '%H:%M  '):
        for crn_col in df.columns:
            series_temp = pd.to_datetime(df[crn_col], format = desired_format, errors='coerce')
            df.loc[series_temp.notna(),crn_col] = list(series_temp[series_temp.notna()])
        for crn_col in df.columns:
            series_temp = pd.to_datetime(df[crn_col].astype('str'), format = '%H:%M:%S', errors='coerce')
            df.loc[series_temp.notna(),crn_col] = list(series_temp[series_temp.notna()])
            
        for crn_col in df.columns:
            series_temp = pd.to_datetime(df[crn_col].astype('str'), format = "o %H:%M  ", errors='coerce')
            df.loc[series_temp.notna(),crn_col] = list(series_temp[series_temp.notna()])            
        return df
    
    # df = basic_conversion_datetime(df)
    df = basic_conversion_datetime(df)
    
    
    # keep conccnered stations only
    df = df.loc[10:,:].copy()
    
    ## 2. fill collumn B to be the cell above if it is empty
    df = df.fillna({'sta': df['sta'].ffill()})
    # print(2)
    ## 3. aligning naming convention for concerned stations
    df_sta_name_lut = pd.read_excel(A4_to_660_station_name_excel)
    df_sta_name_lut_sub = df_sta_name_lut.drop_duplicates(subset  = 'cs1_name').dropna(subset  = 'cs1_name').reset_index(drop = True)
    df = df.merge(df_sta_name_lut_sub[['cs1_name','sta']],left_on = 'sta', right_on = 'cs1_name', how = 'left')
    df['sta'] = np.where(df['cs1_name'].notna(),df['sta_y'], df['sta_x'])
    df['sta_x'] = df['sta'].copy()
    df = df[df.columns[:-3]].copy()
    df = df.rename(columns = {'sta_x': 'sta'})
    
    # remove rows with no data - arrival / departure
    trips = [x for x in list(df.columns) if x not in ['sta','arr_dep']]
    df = df.loc[df[trips].notna().sum(axis = 1)>0,:].copy().reset_index(drop = True)
    # For particular station (Hamilton), if it is "...", make it np.nan so that we know we are not skipping the station because it is an express train
    particular_station = ['Hamilton GO Centre']
    mask = df['sta'].isin(particular_station)
    df.loc[mask, trips] = np.where(df.loc[mask, trips] == '…', np.nan, df.loc[mask, trips])
    
            
    # Add arr / dep rows: 
    rowno = 1
    while rowno < len(df):
        # print(rowno)
        if rowno != len(df)-1:
            # print("not last row")
            if df.loc[rowno,'sta'] != df.loc[rowno+1,'sta']:
                # print('this row station is not the same as next rows station')
                df.loc[rowno - 0.5,:] = df.loc[rowno,:].copy()
                df.loc[rowno - 0.5,'arr_dep'] = 'Arr'
                df = df.sort_index()
                are_dt = df.loc[rowno, :].apply(pd.api.types.is_datetime64_any_dtype)
                
                trip_crn = [trip for trip in trips if isinstance(df.loc[rowno, trip], pd._libs.tslibs.timestamps.Timestamp)]
                df.loc[rowno - 0.5,trip_crn] = df.loc[rowno - 0.5,trip_crn] - timedelta(minutes=1)
                df = df.reset_index(drop = True)
                rowno +=2
            else:
                # print('this row station same as next row station')
                df.loc[rowno,'arr_dep'] = 'Arr'
                rowno += 2
        else:    
            df.loc[rowno,'arr_dep'] = 'Arr'
            # print('last row')
            rowno += 2
    
    ### if the value shall be +1 day, reflect it on the data
    #### if value is '24.00.00', make the date become +1 day
    for crn_col in df.columns:
        df.loc[df[crn_col] == '24.00.00',crn_col] = pd.to_datetime('1900-01-02 00:00:00')
        df.loc[df[crn_col] == r'24/00.00',crn_col] = pd.to_datetime('1900-01-02 00:00:00')
    
    #### change values for trips that starts before next day, but end in next day
    for crn_col in df.columns:
        desired_format = '%Y-%m-%d %H:%M:%S'
        series_temp = df[crn_col].astype('str')
        series_temp2 = pd.to_datetime(series_temp, format = desired_format, errors='coerce')
        series_temp2_df = series_temp2.reset_index().dropna(subset = crn_col).reset_index(drop = True)
        if len(series_temp2_df) > 0:
            series_temp2_df['neg_col'] = series_temp2_df[crn_col] - series_temp2_df[crn_col].shift(1)
            
            # add 1 day for correct values
            ref_index = list(series_temp2_df[series_temp2_df['neg_col'] < timedelta(seconds = 0)]['index'])
            if len(ref_index)>= 1:
                ref_index = ref_index[0]
                series_temp2_df.loc[series_temp2_df['index']>=ref_index,crn_col] += timedelta(days = 1)
                df.loc[series_temp2_df['index'],crn_col] = list(series_temp2_df.loc[:,crn_col])
    # print(4.2)
    #### change values for trips that starts next day (assuming all those trips starts before 3am)
    for crn_col in df.columns:
        desired_format = '%Y-%m-%d %H:%M:%S'
        series_temp = df[crn_col].astype('str')
        series_temp2 = pd.to_datetime(series_temp, format = desired_format, errors='coerce')  
        series_temp2_df = series_temp2.reset_index().dropna(subset = crn_col).reset_index(drop = True)
        if len(series_temp2_df) > 0:
            if series_temp2_df.loc[0,crn_col].hour < 3:
                series_temp2_df.loc[:,crn_col] += timedelta(days=1)
                df.loc[series_temp2_df['index'],crn_col] = list(series_temp2_df.loc[:,crn_col])
    return df, df_form_formby

# II. FILTER REV TRAINS & CONCERNED STATIONS
def filter_rev_trn_crn_sta(df, df_corridor_lut,sht_name):
    # 1. Filter: ONLY CONCERN REVENUE TRAINS 
    filtered_list = [item for item in df.columns if (item[0] != 'E') and (item[:3] != 'VIA')]
    df = df[filtered_list]
    
    # 2. Filter station: only leaving / arriving concerned station
    def crn_sta(A4_to_660_station_name_excel,df_corridor_lut, sht_name, union_sta_name = 'Union Station'):
        df_sta_name_lut = pd.read_excel(A4_to_660_station_name_excel)
        df_sta_name_lut  = df_sta_name_lut.merge(df_corridor_lut, left_on = 'corridor', right_on = 'A4', how = 'left')
        df_sta_name_lut = df_sta_name_lut.dropna(subset = '660_name')
        crn_cols = ['660','sta']
        df_sta_name_lut = df_sta_name_lut[crn_cols]
        df_sta_name_lut.columns = ['corridor', 'sta']
        list_crn_sta = list(df_sta_name_lut.loc[df_sta_name_lut.corridor == sht_name.split(' ')[0],'sta'])
        list_crn_sta = [union_sta_name] + list_crn_sta
        return list_crn_sta
    list_crn_sta = crn_sta(A4_to_660_station_name_excel,df_corridor_lut, sht_name, union_sta_name = 'Union Station')
    
    df = df.loc[df.sta.isin(list_crn_sta),:].reset_index()
    return df

# III. CALCULATE JT
def cal_jt(df, df_corridor_lut,sht_name):
    '''
    MAX JT
    3. Calculate JT for each trip
    4a. Separation: Check if a train is local / express
    5a. Calculate Max JT
    '''    
    # 3. Calculate JT for each trip
    def jt_calculation(df, sht_name, union_sta_name = 'Union Station'):
        # direction = sht_name.split(" ")[1]
        
        # ## Get reference point (Union Arr / Dep)
        # if direction == 'IB':
        #     ref_un = 'Arr'
        #     ref_sta = 'Dep'
        # else:
        #     ref_un = 'Dep'
        #     ref_sta = 'Arr'
        direction, ref_un, ref_sta = sht_name_to_ref(sht_name)
        
        trips = [item for item in list(df.columns) if item not in ["index",'Business ID', 'sta', 'arr_dep']]
        con1 = (df.sta == union_sta_name)
        con2 = (df.arr_dep == ref_un)
        df_ref = df.loc[con1&con2,trips].reset_index(drop = True)
        
        
        # rowno = 11
        # trip = '1004'
        ## Do minus
        df_jt = df[['sta', 'arr_dep']].copy()
        for trip in trips:
            for rowno in range(len(df)):
                # print(trip + ", " + str(rowno))
                try:
                    if ref_un == "Arr":
                        df_jt.loc[rowno,trip] =  (df_ref.loc[0,trip] - df.loc[rowno,trip]).total_seconds()/60
                    else:
                        df_jt.loc[rowno,trip] =  (df.loc[rowno,trip] - df_ref.loc[0,trip]).total_seconds()/60
                except:
                    continue
        return df_jt
    df_jt = jt_calculation(df, sht_name, union_sta_name = 'Union Station')
    
    
    # 4a. Separation: Check if a train is local / express
    # if a trip contains "…" for more than 1 times, then it is a local train
    skip_val = '…'
    express_threshold = 2 # if there are >= x number of station skipped, the trip is considered as express
    trips = [item for item in list(df.columns) if item not in ['index','Business ID', 'sta', 'arr_dep']]
    count_skip = df[trips].apply(lambda col: col.value_counts().get(skip_val, 0))
    df_local = pd.DataFrame([trips,count_skip]).T
    df_local.columns = ['trip','count_skip']
    df_local['local'] = np.where(df_local.count_skip >= express_threshold, 'express', 'local')
    
    # 5a. Calculate Max JT
    '''
    ## Loop local / express
    ## Filter Station for Arr/Dep for IB/OB
    ## Calculate Min / Max
    ## Export Max JT
    '''
    
    ## Loop local / express
    # localornot = 'local'
    summary_jt = pd.DataFrame()
    summary_method = 'max'
    for localornot in ['local','express']:
        crn_trip = list(df_local.loc[df_local.local==localornot,'trip'])
        crn_cols = ['sta','arr_dep'] + crn_trip
        df_jt_sub = df_jt[crn_cols]
            
        ## Filter Station for Arr/Dep for IB/OB
        direction, ref_un, ref_sta = sht_name_to_ref(sht_name)
        df_jt_sub = df_jt_sub.loc[df_jt_sub.arr_dep == ref_sta,:].reset_index(drop = True)

        if summary_method == 'max':   
            ## Calculate Min / Max
            df_jt_sub['{}_{}_jt'.format(localornot,summary_method)] = df_jt_sub[crn_trip].max(axis = 1)
            df_jt_sub2 = df_jt_sub[['sta','{}_{}_jt'.format(localornot,summary_method)]]
        
            try:
                summary_jt = pd.merge(summary_jt,df_jt_sub2, how = 'outer')
            except:
                summary_jt = df_jt_sub2

    ## Export Max JT
    summary_jt['corridor'] = sht_name.split(' ')[0]
    return summary_jt, df_jt

# IV. FREQUENCY
def cal_freq(df_rev_trn_crn_sta,time_range_excel, sht_name):
    '''
    ## 1. Get the logic to separate the group
    ## 2. Count trip and calculate frequency
    '''
    ## 1. Get the logic to separate the group
    ### Define 
    union_sta_name = 'Union Station'
    direction = sht_name.split(" ")[1]
    df_time_range = pd.read_excel(time_range_excel, '{}_A4'.format(direction))
    
    ### Get reference point (Union Arr / Dep)
    direction, ref_un, ref_sta = sht_name_to_ref(sht_name)
    df_temp = df_rev_trn_crn_sta.copy()
    ###
    # change the df_temp to df_rev_trn_crn_sta in the future
    ### 
    
    ### Get the Union Station's arr/dep time
    trips = [item for item in list(df_temp.columns) if item not in ["index",'Business ID', 'sta', 'arr_dep']]
    con1 = (df_temp.sta == union_sta_name)
    con2 = (df_temp.arr_dep == ref_un)
    df_ref = df_temp.loc[con1&con2,trips].reset_index(drop = True)
    
    ### Get the "range" for each trip (e.g. Peak1, Peak2)
    for trip in trips:
        for x in range(len(df_time_range)):
            # print(trip, x)
            con1 = df_ref.loc[0,trip].time()>=df_time_range.st_time[x]
            con2 = df_ref.loc[0,trip].time()<= df_time_range.end_time[x]
            if con1&con2:
                df_ref.loc[1,trip] = df_time_range.cat[x]
                # print('yes')
                break
    # Store the trip & their union arr/dep time + period category in df_ref_t
    df_ref_t = df_ref.T.reset_index()
    df_ref_t.columns = ['trip','time','cat']
    
    
    ## 2. Count trip and calculate frequency
    ### Calculate no of hours covered by each period category
    df_time_range['no_hr'] = np.round((df_time_range['end_time'].apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second) - df_time_range['st_time'].apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second) + 1)/3600,1)
    pt_df_time_range = pd.pivot_table(df_time_range,values = 'no_hr',index = 'cat', aggfunc= np.sum).reset_index()

    
    df_count = pd.DataFrame()
    df_freq = pd.DataFrame()
    
    x=0
    for x in range(len(pt_df_time_range)):
        cat = pt_df_time_range['cat'][x]
        crn_trips = list(df_ref_t.loc[df_ref_t.cat == cat,'trip'])
        
        crn_rows = (df_rev_trn_crn_sta.arr_dep == ref_sta)
        # crn_cols = np.where(df_rev_trn_crn_sta.columns.isin(crn_trips), 1, 0)
        
        df_rev_trn_crn_sta_sub = df_rev_trn_crn_sta.loc[crn_rows, ['sta','arr_dep'] + crn_trips].reset_index(drop = True)    
        def count_datetime_values(row):
            return sum(isinstance(value, datetime) for value in row)
        
        # Apply the function to each row
        df_rev_trn_crn_sta_sub['{}_count'.format(cat)] = df_rev_trn_crn_sta_sub.apply(count_datetime_values, axis=1)
        df_rev_trn_crn_sta_sub['{}_freq'.format(cat)] = df_rev_trn_crn_sta_sub['{}_count'.format(cat)] / list(pt_df_time_range[pt_df_time_range.cat == cat]['no_hr'])[0]
        try:
            df_count = df_count.merge(df_rev_trn_crn_sta_sub[['sta','{}_count'.format(cat)]], how = 'outer')
            df_freq = df_freq.merge(df_rev_trn_crn_sta_sub[['sta','{}_freq'.format(cat)]], how = 'outer')
        except:
            df_count = df_rev_trn_crn_sta_sub[['sta','{}_count'.format(cat)]].copy()
            df_freq = df_rev_trn_crn_sta_sub[['sta','{}_freq'.format(cat)]].copy()
    ### Export frequency summary
    df_freq['corridor'] = sht_name.split(" ")[0]
    df_count['corridor'] = sht_name.split(" ")[0]
    return df_freq, df_count, df_ref_t

# CALCULATE MERGE ALL ['df_freq', 'df_count', 'summary_jt']
def cal_df_all(my_dic):
    crn_df = ['df_freq', 'df_count', 'summary_jt']
    
    for df_str in crn_df:
        my_dic['{}_all'.format(df_str)] = dict()
        my_dic['{}_all'.format(df_str)]['{}_all_IB'.format(df_str)] = pd.DataFrame()
        my_dic['{}_all'.format(df_str)]['{}_all_OB'.format(df_str)] = pd.DataFrame()
        for key in my_dic[df_str]:
            df = my_dic[df_str][key].set_index(['corridor','sta'])
            con_IB = (key.split(' ')[1] == 'IB')
            
            if con_IB:
                my_dic['{}_all'.format(df_str)]['{}_all_IB'.format(df_str)] = pd.concat([my_dic['{}_all'.format(df_str)]['{}_all_IB'.format(df_str)], df])
            else:
                my_dic['{}_all'.format(df_str)]['{}_all_OB'.format(df_str)] = pd.concat([my_dic['{}_all'.format(df_str)]['{}_all_OB'.format(df_str)], df])
    return my_dic

#============================================================================
#============================================================================
#============================================================================


# CHANGE MIGHT BE REQUIRED
#**********************************************************************
# SETTING ENVIRONMENT
crn_tt = '660'
wkdir = r"C:\Users\MatthewTsang\OneDrive - ONxpress\Desktop\Reference\CS1 and Glidepath\glidepath\(DRAFT) CS1 - Form 660"
# os.chdir(wkdir)
# RECEIVE wkdir AND wb_name
def wkdir_wbname(crn_tt):
    if crn_tt == '660':
        # wkdir = r"C:\Users\MatthewTsang\OneDrive - ONxpress\Desktop\Reference\CS1 and Glidepath\glidepath\(DRAFT) CS1 - Form 660"
        wb_name = "(DRAFT) CS1 - Form 660 - WeekDAY.xlsx"
    else:
        # wkdir = r"C:\Users\MatthewTsang\OneDrive - ONxpress\Desktop\Reference\CS1 and Glidepath\CS1_TT"
        wb_name = "ONC-TCC-5090-SWS-PWS-TSP-LST-00055_CS1_BD_Customer_Timetables_P01.xlsx"
    return wkdir, wb_name
# LOOKUP TABLES
## LOOKUP TABLE FOR CONVERTING 660 STATION NAME TO A4 STATION NAMES
A4_to_660_station_name_excel = "A4_to_660_station_name.xlsx"
## LOOKUP TABLE FOR TIME RANGES
time_range_excel = r"time_range.xlsx"
#**********************************************************************


# LOOKUP TABLE FOR CORRIDOR NAME
df_corridor_lut = pd.DataFrame([['LSW', 'LSE', 'MIL', 'KIT', 'BAR', 'RH', 'STF', 'UPE'],
                                ['LW','LE','MI','KW','BA','RH','ST','UP']]).T
df_corridor_lut.columns = ['A4','660']


# START WORKING
# Get and change working directory, wb_name, sheet_name; create dictionary for storage
def clean_tt_and_get_stat(crn_tt,A4_to_660_station_name_excel,time_range_excel,df_corridor_lut):
    wkdir, wb_name = wkdir_wbname(crn_tt)   
    os.chdir(wkdir)
    sheet_names, sheet_names_660_equi = get_sheet_names_and_equiv(crn_tt, wb_name)
    my_dic = dic_creation(sheet_names,wb_name)
    
    # CLEAN TIMETABLE AND GET STATISTICS
    # sht_no = 0
    for sht_no in range(len(sheet_names)):
        sht_name = sheet_names[sht_no]
        df = pd.read_excel(wb_name, sheet_name = sht_name)
        sht_name = sheet_names_660_equi[sht_no]
    
        # I. CLEAN TIMETABLE
        if crn_tt == '660':
            # Ia. CLEAN TIMETABLE
            df, df_form_formby = clean_tt(df,A4_to_660_station_name_excel,sht_name)
        else:
            # Ib.
            df, df_form_formby = clean_tt_cs1(df,A4_to_660_station_name_excel,sht_name)
            
        # II. FILTER REV TRAINS & CONCERNED STATIONS
        df_rev_trn_crn_sta = filter_rev_trn_crn_sta(df, df_corridor_lut,sht_name)
        
        # III. CALCULATE JT
        df_bu = df.copy()
        df = df_bu.copy()
        df = df_rev_trn_crn_sta.copy()
        
        summary_jt,df_jt = cal_jt(df_rev_trn_crn_sta, df_corridor_lut,sht_name)
        
        # IV. FREQUENCY
        df_freq, df_count, df_ref_t = cal_freq(df_rev_trn_crn_sta,time_range_excel, sht_name)
        
        # Store values in dictionary
        my_dic['df'][sht_name] = df
        my_dic['df_rev_trn_crn_sta'][sht_name] = df_rev_trn_crn_sta
        my_dic['summary_jt'][sht_name] = summary_jt
        my_dic['df_jt'][sht_name] = df_jt
        my_dic['df_freq'][sht_name] = df_freq
        my_dic['df_count'][sht_name] = df_count
        my_dic['df_ref_t'][sht_name] = df_ref_t
        my_dic['df_form_formby'][sht_name] = df_form_formby
        
    # CALCULATE MERGE ALL ['df_freq', 'df_count', 'summary_jt']
    my_dic = cal_df_all(my_dic)
    return my_dic


my_dic_cs1 = clean_tt_and_get_stat('cs1',A4_to_660_station_name_excel,time_range_excel,df_corridor_lut)
my_dic_660 = clean_tt_and_get_stat('660',A4_to_660_station_name_excel,time_range_excel,df_corridor_lut)


# EXPORTING SUB_DIC AS EXCEL FILES WITH MULTIPLE SHEETS (SINCE SUB_DIC HAS MULTIPLE DATAFRAME)
def export_sub_dic(dic):
    '''
    # Assume you have a dictionary named 'data_dict' with sub-dictionaries containing dataframes
    
    # Example data_dict structure:
    # data_dict = {
    #     'sub_dict1': {'df1': pd.DataFrame(...), 'df2': pd.DataFrame(...), ...},
    #     'sub_dict2': {'df3': pd.DataFrame(...), 'df4': pd.DataFrame(...), ...},
    #     ...
    # }
    '''
    # Loop through each sub-dictionary
    for sub_dict_name, sub_dict_data in dic.items():
        # Create an Excel writer for the sub-dictionary
        excel_writer = pd.ExcelWriter(f'{sub_dict_name}.xlsx', engine='xlsxwriter')
    
        # Loop through each dataframe in the sub-dictionary and write to Excel
        for df_name, dataframe in sub_dict_data.items():
            dataframe.to_excel(excel_writer, sheet_name=df_name, index=True)
    
        # Save the Excel file for the current sub-dictionary
        excel_writer.save()
    return 

# export_sub_dic(my_dic_660)
# export_sub_dic(my_dic_cs1)
# The above code will create separate Excel files for each sub-dictionary with sheets for each dataframe within it.



# FURTHER CLEANING in the future
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
## clean 660 useless first / last trip for cs1 - function 1a (matching the format of 1b result perfectly)
## clean cs1 useless first / last trip for cs1 - function 1b (matching the format of 1a result perfectly)
## adjust time range table
## adjust the algo to be more generic to cater weekend timetable
## do the post-processing with pandas
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################