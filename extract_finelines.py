import numpy as np
import pandas as pd
import psycopg2 as p
from sqlalchemy import create_engine

con = p.connect(database="Scheduling", user="ctcadmin@mpm-spot-db", password="test", host="mpm-spot-db.mysql.database.azure.com", port="5432")
cur = con.cursor()
engine = create_engine('mysql://postgres:test@localhost/Scheduling')

df = pd.read_excel('CPR_2021.xlsx')

df = df.loc[(df['Deal Type'] == 'Seasonal Set Up') | (df['Deal Type'] == 'Line Review Deal') |  (df['Deal Type'] == 'Order to Tab') |  (df['Deal Type'] == 'Placing')]

df2 = df.loc[df['Finelines'].notnull()]

#df_full_info = df2[['LR Name', 'Fineline IDs', 'Ordering POG Production Baseline Start Date', 'Ordering POG Production Baseline End Date', 'Cut Off', 'Target Store Set Up', 'Ordering POG Production (Pilot) Baseline Start Date', 'Ordering POG Production (Pilot) Baseline End Date']].drop_duplicates()

df_full_info = df2[['LR #', 'LR Name', 'Fineline IDs', 'Cross Functional Teams Review All Info ', 'Cut Off', 'Target Store Set Up']].drop_duplicates()

df_full_info['pref'] = df_full_info['LR Name'].apply(lambda x: x[:2])

df_full_info = df_full_info.loc[df_full_info['pref'] == '08']

df_full_info = df_full_info.drop(['pref'], axis=1)

df_full_info['O_start_date'] = df_full_info['Cross Functional Teams Review All Info ']

df_full_info['O_end_date'] = df_full_info['Cross Functional Teams Review All Info '] + pd.DateOffset(days=7)

df_full_info = df_full_info.drop(['Cross Functional Teams Review All Info '], axis=1)

df_full_info = df_full_info[['LR #', 'LR Name', 'Fineline IDs', 'O_start_date', 'O_end_date', 'Cut Off', 'Target Store Set Up']]

df_full_info['CPR'] = 'CPR_2021'

df_full_info.columns = ['LR_ID', 'LR_Deal', 'FINELINE', 'O_start_date', 'O_end_date', 'Dealer_mail_date', 'Target_store_setup', 'CPR']

cur.execute(''' DELETE FROM public."CPR_RAW" WHERE "CPR" = 'CPR_2020' ''')
cur.execute('''COMMIT''')

df_full_info.to_sql('CPR_RAW', engine, if_exists='append', index=False)

df = pd.read_sql('Select * FROM public."CPR_RAW"', engine )

df2 = df.loc[df['FINELINE'].notnull()]

master_dict = {}

for i, j in df2.iterrows():
    fnl_desc = j['FINELINE'].split(';')
    lr_key = j['LR_ID']
    master_dict[lr_key] = list(set(fnl_desc)) 

df_t1 = pd.DataFrame.from_dict(master_dict, orient='index')
df_t1 = df_t1.reset_index()

final_df1 = pd.melt(df_t1, id_vars = ['index'])

final_df11 = final_df1.drop(['variable'], axis = 1)

final_df11 = final_df11.drop_duplicates()

final_df11.columns = ['LR_ID', 'FINELINE']

df_full_info = df_full_info.drop(['FINELINE'], axis=1)

final_set = final_df11.merge(df_full_info, how = 'inner', on = ['LR_ID']).drop_duplicates()

final_set = final_set[final_set['FINELINE'].notnull()]

final_set['FINELINE'] = final_set['FINELINE'].apply(lambda x: x[-5:])

final_set_2  = final_set.drop_duplicates()

cur.execute(''' DELETE FROM public."CPR_RAW_FNLN" WHERE "CPR" = 'CPR_2020' ''')
cur.execute('''COMMIT''')

final_set_2.to_sql('CPR_RAW_FNLN', engine, if_exists='append', index=False)