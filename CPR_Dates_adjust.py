#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as p
import os
import sys

my_list = sys.argv[1].split(':')
user = my_list[0]
pwd = my_list[1]

#221115: change port Azure mysql port3306
con = p.connect(database="Scheduling", user="ctcadmin@mpm-spot-db", password=pwd, host="mpm-spot-db.mysql.database.azure.com", port="3306")
cur = con.cursor()

engine = create_engine('mysql://' + user + ':' + pwd + '@localhost/Scheduling')

df = pd.read_sql('''Select * from public."CPR_DEAL_POG_INFO" where "Target_store_setup" > CURRENT_DATE''', con=engine)

df_const = df.loc[df['CHANGE_IND'] == 'Y']

df_change = df.loc[~(df['CHANGE_IND'] == 'Y')]

folder = 'C:\\CPR_Adjust\\'

deals = os.listdir(folder)

df_master = pd.DataFrame(columns = ['LR_ID_1', 'LR_Deal_1', 'Dealer_mail_date_1', 'Target_store_setup_1', 'O_start_date_1', 'O_end_date_1'])

for deal in deals:
    df_cpr = pd.read_excel(folder +  deal)
    df_cpr = df_cpr.loc[(df_cpr['Deal Type'] =='Seasonal Set Up')| (df_cpr['Deal Type'] =='Order to Tab') | (df_cpr['Deal Type'] =='Placing') | (df_cpr['Deal Type'] =='Line Review Deal')]
    df_cp = df_cpr[['LR #', 'LR Name', 'Cross Functional Teams Review All Info ', 'Ordering POG Production Baseline Start Date', 'Ordering POG Production Baseline End Date', 'Cut Off', 'Target Store Set Up']]
    df_cp['Cross Functional Teams Review All Info '] = df_cp['Cross Functional Teams Review All Info '].fillna(df_cp['Ordering POG Production Baseline Start Date'] )
    df_cp = df_cp.drop(['Ordering POG Production Baseline Start Date','Ordering POG Production Baseline End Date'], axis=1)
    df_cp['O_start_date'] = df_cp['Cross Functional Teams Review All Info ']
    df_cp['O_end_date'] = df_cp['O_start_date'] + pd.DateOffset(days=7)
    df_cp = df_cp.drop(['Cross Functional Teams Review All Info '], axis=1)
    df_cp.columns = ['LR_ID_1', 'LR_Deal_1', 'Dealer_mail_date_1', 'Target_store_setup_1', 'O_start_date_1', 'O_end_date_1']
    df_master = df_master.append(df_cp)

df_2 = df_change.merge(df_master, how = 'left', left_on = ['LR_ID', 'LR_Deal'], right_on = ['LR_ID_1', 'LR_Deal_1'])

df_2['O_start_date_1'] = df_2['O_start_date_1'].fillna(df_2['O_start_date'])
df_2['O_end_date_1'] = df_2['O_end_date_1'].fillna(df_2['O_end_date'])
df_2['Dealer_mail_date_1'] = df_2['Dealer_mail_date_1'].fillna(df_2['Dealer_mail_date'])
df_2['Target_store_setup_1'] = df_2['Target_store_setup_1'].fillna(df_2['Target_store_setup'])

df_2['O_start_date'] = df_2['O_start_date_1']
df_2['O_end_date'] = df_2['O_end_date_1']
df_2['Dealer_mail_date'] = df_2['Dealer_mail_date_1']
df_2['Target_store_setup'] = df_2['Target_store_setup_1']

df_2 = df_2.drop(['O_start_date_1', 'O_end_date_1', 'Dealer_mail_date_1', 'Target_store_setup_1', 'LR_ID_1', 'LR_Deal_1'], axis=1)

df_final = pd.concat([df_2, df_const])

cur.execute(''' DELETE FROM public."CPR_DEAL_POG_INFO" WHERE "Target_store_setup" > CURRENT_DATE''')
cur.execute('''COMMIT''')

df_final.to_sql('CPR_DEAL_POG_INFO', engine, if_exists='append', index=False)

success_facor = 1

