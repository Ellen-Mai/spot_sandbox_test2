#Importing necessary modules
import numpy as np
import pandas as pd
from datetime import datetime
import math
from sqlalchemy import create_engine
import psycopg2 as p
import sys

#Database credentials will be captured at runtime
my_list = sys.argv[1].split(':')
user = my_list[0]
pwd = my_list[1]

#PostgreSQL database connection details
con = p.connect(database="scheduling", user="ctcadmin@mpm-spot-db", password=pwd, host="mpm-spot-db.mysql.database.azure.com", port="3306")
cur = con.cursor()

engine = create_engine('postgresql://' + user + ':' + pwd + '@host/scheduling')

#Captures deals that need to be worked on
df2 = pd.read_sql('''Select * from public."DEAL_FULL_INFO_VW" where "Deal_prefix" not in ('0536', '0552') ''', con = engine)

#Delete unassigned POG info before scheduling run
cur.execute(''' DELETE FROM public."UNASSIGNED_DATASET" ''')
cur.execute('''COMMIT''')

#Reading in unassigned dataset for appending unassigned POGs during current scheduling run
unassigned_df = pd.read_sql('''Select * from public."UNASSIGNED_DATASET"''', con=engine)

#Get list of SPS - this is in order of least to most active department, and SPS' in the department from the last 30 calendar days.
cat_owners = pd.read_sql('''Select "SPS_ID" from public."BUSIEST_DEPT_SPS_VW"''', con = engine)
owners_list = cat_owners['SPS_ID'].to_list()

#This will be used to find category owner of the POG.
all_pog_owners = pd.read_sql('''Select * from public."POG_CAT_OWNERS_VW"''', con = engine)

#To integrate vacation into the schedule we will be resetting the calendar from holidays view as it has the same table structure.
sps_info_dataset = pd.read_sql('''Select * from public."HOLIDAYS"''', con=engine)
sps_short_info = pd.read_sql('''Select * from public."HOLIDAYS"''', con=engine)

#Resets happen every Thursday.
import datetime
thursday_dt = datetime.date.today()
while thursday_dt.weekday() != 3:
    thursday_dt += datetime.timedelta(1)
int_dt = int(thursday_dt.strftime('%Y%m%d'))
#Only reset from next Thurday into the future.
sps_info_dataset = sps_info_dataset.loc[sps_info_dataset['DAY_ID'] >= int_dt]
sps_short_info = sps_short_info.loc[sps_short_info['DAY_ID'] >= int_dt]

#Reading in finished POG builds.
sps_dataset = pd.read_sql(''' Select * from public."SPS_DATASET" WHERE "DAY" < %s ''', params = [int_dt], con = engine)
sps_dataset_freeze = pd.read_sql(''' Select * from public."SPS_DATASET" WHERE "DAY" >= %s and "FREEZE" = 'Y' ''', params = [int_dt], con = engine)
sps_dataset = pd.concat([sps_dataset, sps_dataset_freeze])
#Adjusting target store setup for V build window.
df2['MERCH_RELEASE'] = df2['MERCH_RELEASE'].fillna(df2['Target_store_setup'])
df2['MERCH_RELEASE'] = df2['MERCH_RELEASE'].apply(lambda x: x + pd.DateOffset(-21))
#Converting to upper case
df2['LR_Deal'] = df2['LR_Deal'].str.upper()

def get_pog_info(deal, pog):
    info = pd.read_sql(''' Select distinct "POG_DESC", "MERCH_RELEASE", "SPS" from public."DEAL_FULL_INFO_VW" where "POG_NUM" = %s and  "Deal_prefix" = %s ''', engine, params=[pog, deal])
    return (info['POG_DESC'][0], info['MERCH_RELEASE'][0], info['SPS'][0])

#Below dictionary to capture all SPS by department.
sps_dicts = {}

cat_owners_dep = pd.read_sql('''Select * from public."BUSIEST_DEPT_SPS_VW"''', con=engine)

dict_vw = cat_owners_dep[['SPS_ID', 'Department']]

#Dictionary of SPS' as values and departments as keys.
for i, j in dict_vw.iterrows():
    key = j['Department']
    if key not in sps_dicts:
        sps_dicts[key] = []
    sps_dicts[key].append(j['SPS_ID'])

dept_order_qry = pd.read_sql('''Select "Department" from public."BUSIEST_DEPT_SPS_VW" ''', con = engine)
all_depts = list(dept_order_qry['Department'].unique())

#Filling in with 0s for values we don't have from JDA
df2['POG_SKU_Count'] = df2['POG_SKU_Count'].fillna(0)

df_2 = df2

#For POGs that don't require ESL, we set value as 0
df_2['ESL_DIFF_INDEX'] = df_2['ESL_DIFF_INDEX'].fillna(0)

df_2['V_Build_days'] = df_2['Recco_V'] + df_2['ESL_DIFF_INDEX'] + df_2['EXTD_BUILD']

df_2['O_Build_days'] = df_2['Recco_O']  

#Adjusting the V build start date to a week for cut off.
df_2['V_Build_start_date'] = df_2['Dealer_mail_date'].apply(lambda x: x + pd.DateOffset(days=5))

#Reading in holidays
sps_holidays_sched = pd.read_sql('''Select * from public."HOLIDAYS"''', con = engine)

#This function checks availability of an SPS whether they're already assigned a tast or if they're on holiday
def check_sps_holiday_and_schedule_calendar(cat_owner, comp_date):
    global sps_dataset
    global sps_holidays_sched
    val1 = sps_holidays_sched[cat_owner].loc[sps_holidays_sched['DAY_ID'] == comp_date].values[0] == 'x' # Check is SPS is on vacation
    val2 = sps_dataset.loc[(sps_dataset['DAY'] == comp_date) & (sps_dataset['SPS'] ==  cat_owner)]
    a = val1
    b = len(val2) > 0
    return a | b 

#The below function is a slight variation of the above one. An SPS can build 2 ordering/PPK POGs on a single day.
def check_sps_holiday_ordering_ppk(cat_owner, comp_date):
    global sps_dataset
    global sps_holidays_sched
    comp_date = comp_date
    val1 = sps_holidays_sched[cat_owner].loc[sps_holidays_sched['DAY_ID'] == comp_date].values[0] == 'x' # Check is SPS is on vacation
    #val2 = sps_dataset.loc[(sps_dataset['DAY'] == comp_date) & (sps_dataset['ORDERING_PPK'] == np.nan) & (sps_dataset['SPS'] ==  cat_owner)]
    val2 = sps_dataset.loc[(sps_dataset['DAY'] == comp_date) & (sps_dataset['BUILD_TYP'] != 'ORDERING_PPK') & (sps_dataset['SPS'] ==  cat_owner)]
    count_var = sps_dataset.loc[(sps_dataset['SPS'] == cat_owner) & (sps_dataset['DAY'] == comp_date) & (sps_dataset['BUILD_TYP'] ==  'ORDERING_PPK')]
    a = val1
    b = len(val2) > 0
    c = len(count_var) > 1
    return a | b | c

#For every assigned day/task, a row is added to this dataset.
def append_row(cat_owner, comp_date, deal_id, pog_num, build_typ):
    global sps_dataset
    append_df = pd.DataFrame([[cat_owner, comp_date, deal_id, pog_num, build_typ, '']], columns = ['SPS', 'DAY', 'DEAL', 'POG', 'BUILD_TYP', 'FREEZE'])
    sps_dataset = sps_dataset.append(append_df, ignore_index=True)

#Reading in miscellaneous/non regular tasks. These tasks will be assigned first.
df_misc = pd.read_sql('''Select * from public."SPS_MISC_SCHED" WHERE "END_DATE" >= %(thur)s ''',params={"thur":thursday_dt}, con = engine)

#If a task is currently happening (started prior to Thursday reset and unfinished) but not finished, we reset the start date to thursday for the purpose of schedule generation.
df_misc['START_DATE']= df_misc['START_DATE'].apply(lambda x: max(x, thursday_dt))

#Function to assign these non regular tasks.
def misc_schedule():
    global df_misc
    for i, j in df_misc.iterrows():
        datelist = [y for y in pd.date_range(j['START_DATE'], j['END_DATE']).tolist() if y.strftime("%A") not in ('Saturday', 'Sunday')] #Weekends are excluded
        for dt in datelist: #Looping through all the dates
            comp_date = int(dt.strftime('%Y%m%d')) #Convert date to integer
            sched_holiday_check = check_sps_holiday_and_schedule_calendar(j['SPS'], comp_date) #check for holiday
            if sched_holiday_check == False: #Assign day to SPS for that task only if they're available.
                info_str = 'Deal: ' + j['DEAL'] + ' ' + 'MISC ' + 'Desc: ' + j['DESC'] #Calendar view info
                short_info = j['TASK'] + '-'+'M:' + 'MISC' #Calendar view info
                sps_info_dataset.at[sps_info_dataset['DAY_ID'] == comp_date, j['SPS']] = info_str
                sps_short_info.at[sps_short_info['DAY_ID'] == comp_date, j['SPS']] = short_info
                append_row(cat_owner = j['SPS'], comp_date = comp_date, deal_id = j['DEAL'], pog_num = j['TASK'], build_typ = 'MISC') #Append row is day is assigned for that task

#For a POG build, given the required build days, this function will determine which SPS can build the POG. First preference is given to category followed by SPS from their department.
#If no one from their department is available, task will be assigned to someone from a different department based on availability.
def pog_owner_dates(cat_owner, dept, start_date, final_dt, O_build_days, build_typ):
    global sps_dicts
    #{Next 6 lines in code} - Category owner is given preference for the build. If not available, then the program looks for another person in their department. If no 
    #availability within department, the program looks for a resource in another department.
    orig_cat_owner = [cat_owner]
    dept_list_ext = [x for x in sps_dicts[dept] if x!=cat_owner]
    other_depts = [dep for dep in all_depts if dep!= dept]
    for dep in other_depts:
        dept_list_ext.extend(sps_dicts[dep])
    full_dept_list = orig_cat_owner + dept_list_ext
    datelist = [y for y in pd.date_range(start_date, final_dt).tolist() if y.strftime("%A") not in ('Saturday', 'Sunday')]  
    if build_typ=='OPOG': #O builds should start Thursday
        datelist = datelist[1:]
    for owner in full_dept_list:
        dates_final = []
        day_count = 0        
        for dates in datelist:
            comp_date = int(dates.strftime('%Y%m%d')) #Getting the date in integer format.
            if build_typ != 'ORDERING_PPK':
                sched_holiday_check = check_sps_holiday_and_schedule_calendar(owner, comp_date)
            else:
                sched_holiday_check = check_sps_holiday_ordering_ppk(owner, comp_date)                
            if sched_holiday_check == False:
                dates_final.append(comp_date)
                day_count+=1
                if day_count == O_build_days: #If the required days are met, then function returns the resource and dates that need to be allocated.
                    finalized_cat_owner = owner
                    final_dates = dates_final
                    return (finalized_cat_owner,final_dates)


#Once we determine which SPS can build the POG, we assign those days to the SPS
def assign_schedule_sps(deal_id, pog_num, pog_desc, cat_owner, dept, build_typ, start_date, build_days, final_dt, esl_days, merch_release, v_builder, extd_days):
    global sps_dataset #Global because we want to fill an existing dataset
    global unassigned_df
    global all_pog_owners
    owner = all_pog_owners['SPS'].loc[all_pog_owners['POG_NUM'] == pog_num].iloc[0]
    info_str = 'Deal: ' + deal_id[0:4] + '  |  ' + 'POG: ' + pog_num + '  |  ' + 'POG Desc: ' + pog_desc +  '  |  ' + 'Cat Owner: ' + owner +  '  |  ' + 'Build: ' + build_typ + '  |  ' + 'Merch Release: ' + str(merch_release)[:10] #Calendar view info
    short_info = build_typ[0] + '-'+'D:' + deal_id[1:4] + '-' + pog_num  #Calendar view info
    #Datelist below builds out a list of days for the POG build.
    sps_and_dates = pog_owner_dates(cat_owner = cat_owner, dept = dept, start_date = start_date, final_dt = final_dt, O_build_days = build_days, build_typ = build_typ) #Find resource for POG build.
    vb = build_days - esl_days - extd_days #To keep count of V build and ESL days separately.
    day_counter = 0
    if sps_and_dates is not None:
        sps = sps_and_dates[0]
        dates = sps_and_dates[1]
        for dt in dates:
            if build_typ != 'ORDERING_PPK':
                if build_typ == 'OPOG':
                    sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = info_str
                    sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = short_info
                else: #Build type is V build.
                    day_counter+=1 #we increment day counter
                    if day_counter <= vb: #Regular V build
                        sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = info_str
                        sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = short_info
                    elif day_counter <= (vb + esl_days): #ESL build
                        sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = 'Deal: ' + deal_id[0:4] + '  |  ' + 'POG: ' + '2' + pog_num[1:] + '  |  ' + 'POG Desc: ' + pog_desc + '  |  ' + 'Cat Owner: ' + owner +  '  |  ' + 'Build: ' + 'ESL'+ '  |  ' + 'Merch Release: ' + str(merch_release)[:10]
                        sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = 'E' + '-'+'D:' + deal_id[1:4] + '-' + '2' + pog_num[1:]
                    else: #Build type is "EXTD" --> We can use this for extended POG library or for Extended assortment POGs.
                        sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = 'Deal: ' + deal_id[0:4] + '  |  ' + 'POG: ' + '9' + pog_num[1:] + '  |  ' + 'POG Desc: ' + pog_desc + '  |  ' + 'Cat Owner: ' + owner + '  |  ' + 'Build: ' + 'EXTD' + '  |  ' + 'Merch Release: ' + str(merch_release)[:10]
                        sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = 'EX' + '-' + 'D:' + deal_id[1:4] + '-' + '9' + pog_num[1:]
            else: #Build type is ordering/PPK
                prev = sps_info_dataset[sps].loc[sps_info_dataset['DAY_ID'] == dt].to_list()
                if len(prev) == 0:
                    prev = ''
                else:
                    prev = prev[0]
                if prev == '':
                    sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = info_str
                    sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = short_info
                else: #If SPS is doing two ordering/PPK POGs in a single day, then the calendar view should include that.
                    sps_info_dataset.at[sps_info_dataset['DAY_ID'] == dt, sps] = prev + ' ;' +info_str
                    sps_short_info.at[sps_short_info['DAY_ID'] == dt, sps] = short_info
            if build_typ!= 'VPOG':
                append_row(sps, dt, deal_id, pog_num, build_typ)
            else: #Record ESL build days separately.
                if day_counter<=vb:
                    append_row(sps, dt, deal_id, pog_num, build_typ)
                elif day_counter <= (vb + esl_days):
                    append_row(sps, dt, deal_id, pog_num, 'ESL')
                else:
                    append_row(sps, dt, deal_id, pog_num, 'EXTD')
    else: #This part of the code is activated if the tool cannot find a resource for a POG build.      
        append_no_assign = pd.DataFrame([[deal_id, pog_num, cat_owner, build_typ, build_days, start_date, final_dt, dept]],columns = ['DEAL', 'POG', 'CAT_OWNER', 'BUILD_TYP', 'BUILD_DAYS' ,'START_DT', 'END_DT', 'DEPT'])
        unassigned_df = unassigned_df.append(append_no_assign)

def loop_and_assign(df, start_dt, end_dt, build_typ, reset_flag=0):
    if build_typ == 'OPOG':
        data_set_begin = df.sort_values(['O_Build_days'], ascending =False) #Most difficult POGs are assigned first
    else:
        data_set_begin = df.sort_values(['Dealer_mail_date','LR_Deal','V_Build_days','POG_NUM'], ascending = [True, True, False, True])
    for i, j in data_set_begin.iterrows():
        if build_typ == 'OPOG':
            assign_schedule_sps(deal_id = j['LR_Deal'], pog_num = j['POG_NUM'], pog_desc = j['POG_DESC'], cat_owner = j['CAT_OWNER'], dept = j['Department'], build_typ = build_typ, start_date = j[start_dt], build_days = j['O_Build_days'], final_dt  = j[end_dt], esl_days = j['ESL_DIFF_INDEX'], merch_release = j['MERCH_RELEASE'], v_builder = j['V_BUILDER'], extd_days = j['EXTD_BUILD'])
        else:
            assign_schedule_sps(deal_id = j['LR_Deal'], pog_num = j['POG_NUM'], pog_desc = j['POG_DESC'], cat_owner = j['SPS_EXST'], dept = j['Department'], build_typ = build_typ, start_date = j[start_dt], build_days = j['Rem_days'], final_dt  = j[end_dt], esl_days = j['ESL_DIFF_INDEX'], merch_release = j['MERCH_RELEASE'],v_builder = j['V_BUILDER'],extd_days = j['EXTD_BUILD'])
            #assign_v_sps(deal_id = j['LR_Deal'], pog_num = j['POG_NUM'], pog_desc  = j['POG_DESC'], cat_owner = j['CAT_OWNER'], start_date = j[start_dt], end_date = j[end_dt], build_days = j['V_Build_days'],dept = j['Department'], build_typ = 'VPOG')

#Assign non regular tasks
misc_schedule()

#Assign POG builds that are not ordering/PPK POGs
df_now = df_2.loc[df_2['ORDERING_PPK_POG']=='N']
df_now = df_now.loc[df_now['FREEZE']!='O']
#Reading upcoming O builds.
df_now_o_builds = df_now.loc[~(df_now['O_end_date']< thursday_dt)]
df_now_o_builds_1 = df_now_o_builds.loc[df_now_o_builds['PRIORITY'] == 'Y']
df_now_o_builds_2 = df_now_o_builds.loc[df_now_o_builds['PRIORITY'] != 'Y']

#Assign O builds with priority flag
if len(df_now_o_builds_1) >0:
    loop_and_assign(df_now_o_builds_1, start_dt = 'O_start_date', end_dt = 'O_end_date', build_typ = 'OPOG')

#Assign ordering/PPK builds.
df_ppk = df_2.loc[(df_2['ORDERING_PPK_POG'] == 'Y') & (df_2['O_end_date'] > thursday_dt)]
df_ppk = df_ppk.loc[df_ppk['FREEZE']!='O']
#Start date should be Thursday
df_ppk['O_start_date'] = df_ppk['O_start_date'] + pd.DateOffset(days=1)

df_ppk_1 = df_ppk.loc[df_ppk['PRIORITY'] == 'Y']
df_ppk_2 = df_ppk.loc[~(df_ppk['PRIORITY'] == 'Y')]

#Assign priority ordering/ppk pogs
if len(df_ppk_1)>0:
    for i, j in df_ppk_1.iterrows():
        assign_schedule_sps(deal_id = j['LR_Deal'], pog_num = j['POG_NUM'], pog_desc  = j['POG_DESC'], cat_owner = j['CAT_OWNER'], dept = j['Department'], build_typ = 'ORDERING_PPK', start_date = j['O_start_date'], build_days = 1, final_dt = j['O_end_date'], esl_days = j[['ESL_DIFF_INDEX']], merch_release = j['MERCH_RELEASE'], v_builder = j['V_BUILDER'], extd_days = j['EXTD_BUILD'])

#Assign remainder O builds
loop_and_assign(df_now_o_builds_2, start_dt = 'O_start_date', end_dt = 'O_end_date', build_typ = 'OPOG')

#Assigning schedule for remaining Ordering/PPK POGs
for i, j in df_ppk_2.iterrows():
    assign_schedule_sps(deal_id = j['LR_Deal'], pog_num = j['POG_NUM'], pog_desc  = j['POG_DESC'], cat_owner = j['CAT_OWNER'], dept = j['Department'], build_typ = 'ORDERING_PPK', start_date = j['O_start_date'], build_days = 1, final_dt = j['O_end_date'], esl_days = j[['ESL_DIFF_INDEX']], merch_release = j['MERCH_RELEASE'], v_builder = j['V_BUILDER'], extd_days = j['EXTD_BUILD'])

#Future and unfinished V/ESL builds.
df_now_v_pogs= df_now.loc[df_now['MERCH_RELEASE'] >= thursday_dt]
df_now_v_pogs = df_now_v_pogs.loc[df_now_v_pogs['FREEZE']!='V']
#If we are in the midst of a V/ESL build, we count how many days have already been assigned for that POG.
v_pog_days_completed = pd.DataFrame(sps_dataset.loc[(sps_dataset['BUILD_TYP'] == 'VPOG') | (sps_dataset['BUILD_TYP'] == 'ESL') | (sps_dataset['BUILD_TYP'] == 'EXTD')].groupby(['DEAL', 'POG', 'SPS'])['DAY'].count())

v_pog_days_completed = v_pog_days_completed.reset_index()

#V builds that have already begun when the schedule is generated weekly are given priority first.
v_pog_days_completed['priority'] = 'Y'     
                                 
#Renaming columns. SPS_EXT variable identifies who started the V/ESL builds. When rescheduling, that same resource is given prioriy to continue the V/ESL build.
v_pog_days_completed.columns = ['LR_Deal', 'POG_NUM', 'SPS_EXST', 'DAYS', 'priority']

#Merging with our main dataset
df_now_v_pogs_final= df_now_v_pogs.merge(v_pog_days_completed, how = 'left', on = ['LR_Deal', 'POG_NUM'])

#For V builds yet to begin, we set number of completed days to 0
df_now_v_pogs_final['DAYS']= df_now_v_pogs_final['DAYS'].fillna(0)

#Rem days variable will be used to assign V builds. For ongoing V/ESL builds, the remainder of the days are calculated. For upcoming V/ESL builds, it will be the actual number of build days required.
df_now_v_pogs_final['Rem_days']= df_now_v_pogs_final['V_Build_days'] - df_now_v_pogs_final['DAYS']

#For V/ESL builds that haven't begun, the category owner is prioritized.
df_now_v_pogs_final['SPS_EXST'] = df_now_v_pogs_final['SPS_EXST'].fillna(df_now_v_pogs_final['V_BUILDER'])

df_now_v_pogs_final = df_now_v_pogs_final.drop(['CAT_OWNER'], axis=1)

from datetime import date
from datetime import datetime

#Reset V build start date. For ongoing builds, this is tset to Thursday reset date. For upcoming, it will be original start date.
df_now_v_pogs_final['V_Build_start_date']= df_now_v_pogs_final['V_Build_start_date'].apply(lambda x: max(x, datetime.combine(thursday_dt, datetime.min.time())))

#Excluding finished V/ESL builds.
df_now_v_pogs_final = df_now_v_pogs_final.loc[df_now_v_pogs_final['Rem_days']!=0]

df_now_v_pogs_final_1 = df_now_v_pogs_final.loc[df_now_v_pogs_final['priority'] == 'Y']
df_now_v_pogs_final_2 = df_now_v_pogs_final.loc[~(df_now_v_pogs_final['priority'] == 'Y')]                                                                                       
#Assign V/ESL builds.
if len(df_now_v_pogs_final_1) > 0:
    loop_and_assign(df_now_v_pogs_final_1, start_dt = 'V_Build_start_date', end_dt = 'MERCH_RELEASE', build_typ = 'VPOG')
    
loop_and_assign(df_now_v_pogs_final_2, start_dt = 'V_Build_start_date', end_dt = 'MERCH_RELEASE', build_typ = 'VPOG')

for i, j in sps_dataset_freeze.iterrows():
    deal_id = j['DEAL'][:4]
    pog_num = j['POG']
    deal_pog_info = get_pog_info(deal_id, pog_num)
    pog_desc = deal_pog_info[0]
    merch_release = deal_pog_info[1]
    owner = deal_pog_info[2]
    build_typ = j['BUILD_TYP']
    info_str = 'Deal: ' + deal_id[0:4] + '  |  ' + 'POG: ' + pog_num + '  |  ' + 'POG Desc: ' + pog_desc +  '  |  ' + 'Cat Owner: ' + owner +  '  |  ' + 'Build: ' + build_typ + '  |  ' + 'Merch Release: ' + str(merch_release)[:10] #Calendar view info
    short_info = build_typ[0] + '-'+'D:' + deal_id[1:4] + '-' + pog_num
    sps_info_dataset.at[sps_info_dataset['DAY_ID'] == j['DAY'], j['SPS']] = info_str
    sps_short_info.at[sps_short_info['DAY_ID'] == j['DAY'], j['SPS']] = short_info
    

#Delete calendar and records from Thursday into the future. They are then input with the new rescheduled data.
cur.execute(''' DELETE FROM public."SPS_SCHED_WITH_INFO" WHERE "DAY_ID" >= %s  ''', [int_dt])
cur.execute('''COMMIT''')

sps_info_dataset.to_sql('SPS_SCHED_WITH_INFO', engine, if_exists='append', index=False)

cur.execute(''' DELETE FROM public."SPS_DATASET" WHERE "DAY" >= %s ''',[int_dt])
cur.execute('''COMMIT''')

sps_dataset = sps_dataset.loc[sps_dataset['DAY'] >= int_dt]
sps_dataset.to_sql('SPS_DATASET', engine, if_exists='append', index=False)

cur.execute(''' DELETE FROM public."SCHED_SHORT_INFO" WHERE "DAY_ID" >= %s  ''', [int_dt])
cur.execute('''COMMIT''')

sps_short_info.to_sql('SCHED_SHORT_INFO', engine, if_exists='append', index=False)

#Unassigned POGs are written back to the database.
unassigned_df.to_sql('UNASSIGNED_DATASET', engine, if_exists='append', index=False)
