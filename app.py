#Importing all required modules.
from flask import Flask, render_template, request, redirect, flash, session
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from flask_wtf import FlaskForm
from wtforms import SelectField
from sqlalchemy import create_engine
from waitress import serve
from flask_session import Session
from functools import wraps
from flask import send_file
import psycopg2 as p
import pandas as pd
import subprocess
import os

flash_msg ='You must be logged in to continue.'
no_perm = 'You are not allowed to perform this action.'

#Setting app and db configs.
app = Flask(__name__)
app._static_folder = 'static/'
app.config['SECRET_KEY'] = 'KNYdSLvYkUsVvHuPJfsgIQ'
db = SQLAlchemy(app)

#   @fgrf50&!~0lqpt7320@

#Classes of all tables existing in SPOT DB.
class CPR_RAW(db.Model):
    LR_ID = db.Column(db.String(), primary_key=True)
    LR_Deal = db.Column(db.String())
    FINELINE = db.Column(db.String())
    O_start_date = db.Column(db.Date())
    O_end_date = db.Column(db.Date())
    Dealer_mail_date = db.Column(db.Date())
    Target_store_setup = db.Column(db.Date())
    CPR = db.Column(db.String())


class DEALS_MISSED_DATES(db.Model):
    DEAL = db.Column(db.String(50), primary_key=True)
    EFF_DT = db.Column(db.Date(), primary_key=True)
    REASON = db.Column(db.String(50), primary_key=True)
    NEW_SKUS = db.Column(db.Integer())
    EA_SKUS = db.Column(db.Integer())
    PPK_SKUS = db.Column(db.Integer())
    ACTUAL_SKUS = db.Column(db.Integer())
    MOCK_SKUS = db.Column(db.Integer())
    SAMPLES_PROVIDED = db.Column(db.Integer())
    TOTAL_SKUS = db.Column(db.Integer())
    TENTATIVE_GATE3_DT = db.Column(db.Date())

class POG_SKU_INFO(db.Model):
    POG_NUM = db.Column(db.String(), primary_key=True)
    POG_SKU_Count = db.Column(db.Integer())
    Current_SKU_Count = db.Column(db.Integer())
    SKU_Churn = db.Column(db.Integer())

class HOLIDAYS(db.Model):
    DAY_ID = db.Column(db.Integer(), primary_key=True)
    DAY_DATE = db.Column(db.Date())
    DAY_WKDAY_SHNM = db.Column(db.String())
    CLNDR_MTH_NUM = db.Column(db.Integer())
    CLNDR_MTH_SHNM = db.Column(db.String())
    CLNDR_YR_NUM = db.Column(db.Integer())
    CLNDR_WK_NUM = db.Column(db.Integer())
    WEEK_OF_MNTH = db.Column(db.Integer())
    SPS_001 = db.Column(db.String())
    SPS_002 = db.Column(db.String())
    SPS_003 = db.Column(db.String())
    SPS_004 = db.Column(db.String())
    SPS_005 = db.Column(db.String())
    SPS_007 = db.Column(db.String())
    SPS_008 = db.Column(db.String())
    SPS_009 = db.Column(db.String())
    SPS_010 = db.Column(db.String())
    SPS_011 = db.Column(db.String())
    SPS_012 = db.Column(db.String())
    SPS_013 = db.Column(db.String())
    SPS_014 = db.Column(db.String())
    SPS_015 = db.Column(db.String())
    SPS_016 = db.Column(db.String())
    SPS_017 = db.Column(db.String())
    SPS_018 = db.Column(db.String())
    SPS_019 = db.Column(db.String())
    SPS_020 = db.Column(db.String())
    SPS_021 = db.Column(db.String())
    SPS_006 = db.Column(db.String())
    SPS_022 = db.Column(db.String())
    SPS_023 = db.Column(db.String())
    SPS_024 = db.Column(db.String())
    SPS_025 = db.Column(db.String())

class DEAL_SCHED_VW(db.Model):
    Deal_Num = db.Column(db.String(), primary_key=True)
    DAY_DATE = db.Column(db.Date(), primary_key=True)
    MERCH_RELEASE = db.Column(db.Date(), primary_key=True)
    POG = db.Column(db.String(), primary_key=True)
    POG_DESC = db.Column(db.String())
    BUILD_TYP = db.Column(db.String(), primary_key=True)
    SPS = db.Column(db.String(), primary_key=True)

class SPS_SCHED_WITH_INFO(db.Model):
    DAY_ID = db.Column(db.Integer(), primary_key=True)
    DAY_DATE = db.Column(db.Date())
    DAY_WKDAY_SHNM = db.Column(db.String())
    CLNDR_MTH_NUM = db.Column(db.Integer())
    CLNDR_MTH_SHNM = db.Column(db.String())
    CLNDR_YR_NUM = db.Column(db.Integer())
    CLNDR_WK_NUM = db.Column(db.Integer())
    WEEK_OF_MNTH = db.Column(db.Integer())
    SPS_001 = db.Column(db.String())
    SPS_002 = db.Column(db.String())
    SPS_003 = db.Column(db.String())
    SPS_004 = db.Column(db.String())
    SPS_005 = db.Column(db.String())
    SPS_007 = db.Column(db.String())
    SPS_008 = db.Column(db.String())
    SPS_009 = db.Column(db.String())
    SPS_010 = db.Column(db.String())
    SPS_011 = db.Column(db.String())
    SPS_012 = db.Column(db.String())
    SPS_013 = db.Column(db.String())
    SPS_014 = db.Column(db.String())
    SPS_015 = db.Column(db.String())
    SPS_016 = db.Column(db.String())
    SPS_017 = db.Column(db.String())
    SPS_018 = db.Column(db.String())
    SPS_019 = db.Column(db.String())
    SPS_020 = db.Column(db.String())
    SPS_021 = db.Column(db.String())
    SPS_006 = db.Column(db.String())
    SPS_022 = db.Column(db.String())
    SPS_023 = db.Column(db.String())
    SPS_024 = db.Column(db.String())
    SPS_025 = db.Column(db.String())

class SCHED_SHORT_INFO(db.Model):
    DAY_ID = db.Column(db.Integer(), primary_key=True)
    DAY_DATE = db.Column(db.Date())
    DAY_WKDAY_SHNM = db.Column(db.String())
    CLNDR_MTH_NUM = db.Column(db.Integer())
    CLNDR_MTH_SHNM = db.Column(db.String())
    CLNDR_YR_NUM = db.Column(db.Integer())
    CLNDR_WK_NUM = db.Column(db.Integer())
    WEEK_OF_MNTH = db.Column(db.Integer())
    SPS_001 = db.Column(db.String())
    SPS_002 = db.Column(db.String())
    SPS_003 = db.Column(db.String())
    SPS_004 = db.Column(db.String())
    SPS_005 = db.Column(db.String())
    SPS_007 = db.Column(db.String())
    SPS_008 = db.Column(db.String())
    SPS_009 = db.Column(db.String())
    SPS_010 = db.Column(db.String())
    SPS_011 = db.Column(db.String())
    SPS_012 = db.Column(db.String())
    SPS_013 = db.Column(db.String())
    SPS_014 = db.Column(db.String())
    SPS_015 = db.Column(db.String())
    SPS_016 = db.Column(db.String())
    SPS_017 = db.Column(db.String())
    SPS_018 = db.Column(db.String())
    SPS_019 = db.Column(db.String())
    SPS_020 = db.Column(db.String())
    SPS_021 = db.Column(db.String())
    SPS_006 = db.Column(db.String())
    SPS_022 = db.Column(db.String())
    SPS_023 = db.Column(db.String())
    SPS_024 = db.Column(db.String())
    SPS_025 = db.Column(db.String())

class ADI_SPS_CLNDR_VW(db.Model):
    DAY_ID =  db.Column(db.Integer())
    DAY_DATE = db.Column(db.Date(), primary_key=True) 
    DAY_WKDAY_SHNM =  db.Column(db.String())
    sps = db.Column(db.String(), primary_key=True) 
    LR_Deal = db.Column(db.String(), primary_key=True) 
    MERCH_RELEASE =  db.Column(db.Date())
    POG_NUM = db.Column(db.String(), primary_key=True) 
    POG_DESC = db.Column(db.String()) 
    BUILD_TYPE = db.Column(db.String()) 
    POG_BUILDER = db.Column(db.String()) 
    CAT_OWNER  =db.Column(db.String())


class CPR_DEAL_POG_INFO(db.Model):
    CPR = db.Column(db.String(50))
    LR_ID = db.Column(db.String(50), primary_key=True)
    LR_Deal = db.Column(db.String(50), primary_key=True)
    POG_NUM = db.Column(db.String(50), primary_key=True)
    O_start_date = db.Column(db.Date())
    O_end_date = db.Column(db.Date())
    Dealer_mail_date = db.Column(db.Date())
    Target_store_setup = db.Column(db.Date())
    ORDERING_PPK_POG = db.Column(db.String())
    Deal_prefix = db.Column(db.String())
    CHANGE_IND = db.Column(db.String())
    MERCH_RELEASE = db.Column(db.String())
    PRIORITY = db.Column(db.String())
    FREEZE = db.Column(db.String())
    def __repr__(self):
        return 'Deal_ID ' + str(self.DEAL_ID)

class DEAL_FULL_INFO_VW(db.Model):
    CPR = db.Column(db.String(), primary_key=True)
    LR_ID = db.Column(db.String(), primary_key=True)
    LR_Deal = db.Column(db.String(), primary_key=True)
    Dealer_mail_date = db.Column(db.Date(), primary_key=True)
    MERCH_RELEASE = db.Column(db.Date(), primary_key=True)
    Target_store_setup = db.Column(db.Date(), primary_key=True)
    Deal_prefix = db.Column(db.String(), primary_key=True)
    POG_NUM = db.Column(db.String(), primary_key=True)
    POG_DESC = db.Column(db.String())
    CAT_OWNER = db.Column(db.String())
    O_start_date = db.Column(db.Date())
    O_end_date = db.Column(db.Date())
    V_BUILDER = db.Column(db.String())
    Department = db.Column(db.String())
    SPS = db.Column(db.String())
    PRIORITY = db.Column(db.String())
    FREEZE = db.Column(db.String())

class DEAL_VW_SUMM(db.Model):
    Deal_Num = db.Column(db.String(), primary_key=True)
    MERCH_RELEASE = db.Column(db.Date(), primary_key=True)
    POG = db.Column(db.String(), primary_key=True)
    POG_DESC = db.Column(db.String())
    BUILD_TYP = db.Column(db.String(), primary_key=True)
    SPS = db.Column(db.String(), primary_key=True)
    START_DT = db.Column(db.Date(), primary_key=True)
    END_DT = db.Column(db.Date())
    BUILD_DAYS = db.Column(db.Integer())

class DEAL_VW_SUMM_SHOW(db.Model):
    Deal_Num = db.Column(db.String(), primary_key=True)
    MERCH_RELEASE = db.Column(db.Date(), primary_key=True)
    POG = db.Column(db.String(), primary_key=True)
    POG_DESC = db.Column(db.String())
    BUILD_TYP = db.Column(db.String(), primary_key=True)
    SPS = db.Column(db.String(), primary_key=True)
    CAT_OWNER = db.Column(db.String(), primary_key=True)
    START_DT = db.Column(db.Date(), primary_key=True)
    END_DT = db.Column(db.Date())
    BUILD_DAYS = db.Column(db.Integer())

class SPS_CURRENT(db.Model):
    Department = db.Column(db.String())
    SPS_ID = db.Column(db.String(), primary_key=True)
    SPS = db.Column(db.String())

class UNASSIGNED_DATASET(db.Model):
    DEAL = db.Column(db.String(), primary_key=True)
    POG = db.Column(db.String(), primary_key=True)
    CAT_OWNER = db.Column(db.String(), primary_key=True)
    BUILD_DAYS = db.Column(db.Integer())
    START_DT = db.Column(db.Date())
    END_DT = db.Column(db.Date())
    DEPT = db.Column(db.String())

class unassigned_vw(db.Model):
    DEAL = db.Column(db.String(), primary_key=True)
    POG = db.Column(db.String(), primary_key=True)
    CAT_OWNER = db.Column(db.String(), primary_key=True)
    BUILD_DAYS = db.Column(db.Integer())
    START_DT = db.Column(db.Date())
    END_DT = db.Column(db.Date())
    DEPT = db.Column(db.String())

class SPS_MISC_SCHED(db.Model):
    DEAL = db.Column(db.String(),primary_key=True)
    START_DATE = db.Column(db.Date(),primary_key=True)
    END_DATE = db.Column(db.Date())
    SPS = db.Column(db.String(),primary_key=True)
    TASK = db.Column(db.String())
    DESC = db.Column(db.String(), primary_key=True)

class MISC_VW(db.Model):
    DEAL = db.Column(db.String(),primary_key=True)
    SPS = db.Column(db.String(), primary_key=True)
    START_DATE = db.Column(db.Date(),primary_key=True)
    END_DATE = db.Column(db.Date())
    TASK = db.Column(db.String())
    DESC = db.Column(db.String(), primary_key=True)
    Id = db.Column(db.String())

class SPS_DEPT_INFO(db.Model):
    Department = db.Column(db.String(),nullable=False)
    SPS_ID = db.Column(db.String(), primary_key=True,nullable=False)
    SPS = db.Column(db.String(),nullable=False)
    EFF_DT = db.Column(db.Date(), primary_key=True,nullable=False)

    def __repr__(self):
        return 'SPS_ID ' + str(self.SPS_ID)

class POG_CAT_OWNERS(db.Model):
    POG_NUM = db.Column(db.String(50), primary_key=True)
    POG_DESC = db.Column(db.String(100))
    SPS_ID= db.Column(db.String(50))
    Recco_O = db.Column(db.Integer())
    Recco_V = db.Column(db.Integer())
    ESL_DIFF_INDEX = db.Column(db.Integer())
    Difficulty = db.Column(db.Integer())
    V_BUILDER = db.Column(db.String(50))
    EXTD_BUILD = db.Column(db.Integer())

    def __repr__(self):
        return 'POG_NUM ' + str(self.POG_NUM)

class POG_CAT_OWNERS_VW(db.Model):
    POG_NUM = db.Column(db.String(50), primary_key=True)
    POG_DESC = db.Column(db.String(100))
    SPS_ID= db.Column(db.String(50))
    SPS = db.Column(db.String(50))
    Recco_O = db.Column(db.Integer())
    Recco_V = db.Column(db.Integer())
    ESL_DIFF_INDEX = db.Column(db.Integer())
    Difficulty = db.Column(db.Integer())
    V_BUILDER = db.Column(db.String(50))
    V_SPS = db.Column(db.String(50))
    EXTD_BUILD = db.Column(db.Integer())

    def __repr__(self):
        return 'POG_NUM ' + str(self.POG_NUM)

class DUP_V_BUILDS_VW(db.Model):
    DEAL = db.Column(db.String(50), primary_key=True)
    POG_NUM = db.Column(db.String(50), primary_key=True)
    POG_DESC = db.Column(db.String(100), primary_key=True)
    Department = db.Column(db.String(50), primary_key=True)
    MERCH_RELEASE = db.Column(db.Date(), primary_key=True)
    SPS_Count = db.Column(db.Integer(), primary_key=True)

class PLACEHOLDER_VW(db.Model):
    DEAL = db.Column(db.String(50), primary_key=True)
    POG = db.Column(db.String(50), primary_key=True)
    POG_DESC = db.Column(db.String(100), primary_key=True)
    SPS = db.Column(db.String(50), primary_key=True)
    BUILD_TYP = db.Column(db.String(50), primary_key=True)
    START_DT = db.Column(db.Date(), primary_key=True)
    END_DT = db.Column(db.Date(), primary_key=True)

#Flask form for imput choices as drop down.
class Form(FlaskForm):
    SPS  = SelectField('SPS', choices = [])
    V = SelectField('V', choices = [])

#With this method (to be applied to all urls in SPOT), users will only be able to visit a page if they're logged in. If a user is atrying to access a page that needs login confirmation,
#they will be redirected to the login page.
def login_required(f):
    @wraps(f)
    def decorated_function():
        if session.get('username') is None or session.get('username') == '':
            flash('You must login to continue.', 'warning')
            return redirect('/')
        return f()
    return decorated_function

#Login page for SPOT. With the user entered credentials, a connection attempt will be made with the SPOT db. If successful, users are logged in and will be directed to the home page.
@app.route('/',  methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        try:
            con = p.connect(database="scheduling", user=request.form['username'].replace('-', '').replace("'", '').lower(), password=request.form['pass'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
            session['username']  = request.form['username'].replace('-', '').replace("'", '').lower()
            session['password'] = request.form['pass']
            con.close()
            return redirect('/home')
        except:
            flash('Invalid credentials. Please try again.', 'danger')
            return redirect('/')
    if session.get('username'):
        return redirect('/home')
    return render_template('login.html')

#Home page of SPOT.
@app.route('/home')
@login_required
def index():
    return render_template('index.html')

@app.route('/getfinelines', methods=['GET', 'POST'])
@login_required
def test_py():
    print(session.keys())
    if request.method == 'POST':
        #subprocess.call(["python", "extract_finelines.py"])
        creds = session['username'] + ':' + session['password']
        x = subprocess.call(["python", "TEst.py", creds],  shell=True)
        print(x)
        return redirect('/home')
    return render_template('getfinelines.html')


@app.route('/adjust_cpr_dates', methods=['GET', 'POST'])
@login_required
def adjust_dates_py():
    if request.method == 'POST':
        print('Here now!')
        x = subprocess.call(["python", "CPR_Dates_adjust.py", session['username'] + ':' + session['password']], shell=True)
        if x == 0:
            flash('Dates adjusted successfully', 'success')
            return redirect('/home')
        return redirect('/home')
    return render_template('adjust_cpr_dates.html')

#This url navigates to the schedule generation page.
@app.route('/generate_schedule', methods=['GET', 'POST'])
@login_required
def generate_schedule():
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)
    
    super_users = df1['user_role_name'].to_list()
    engine.dispose()
    super_users = super_users + ['postgres']
    if session['username'] not in super_users:
        flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
        return redirect('/home')
    if request.method == 'POST':
        #x = subprocess.call(["python", "C:\\Users\\adi.jakka\\Desktop\\Flask\\Scheduling_prd.py", session['username'] + ':' + session['password']], shell=True)
        x = subprocess.call(["python", "C:\\Users\\#svcbc.pp\\SPOT\\Flask\\Scheduling_prd.py", session['username'] + ':' + session['password']], shell=True)
        if x == 0:
            engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
            unassign_deals = pd.read_sql('''Select * from public."UNASSIGNED_DATASET"''', con=engine)
            if len(unassign_deals) == 0:
                engine.dispose()
                flash('Program ran successfully!', 'success')
                return redirect('/home')
            engine.dispose()
            flash('The schedule was generated but with some exceptions.', 'warning')
            return redirect('/unassigned')
        else:
            flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
            return redirect('/home')
    return render_template('run_schedule.html')

#All current SPS' information is visible here.
@app.route('/spsdepts', methods=['GET', 'POST'])
@login_required
def current_sps():
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)
    
    super_users = df1['user_role_name'].to_list()
    engine.dispose()
    super_users = super_users + ['postgres']
    if session['username'] not in super_users:
        flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
        return redirect('/home')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    cols = SPS_CURRENT.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('sps_current.html', all_sps=all_sps, cols=cols)

#When we click the edit button on the SPS' current info page, users will be redirected to this page. Here, we can edit SPS names or move them to a different department.
@app.route('/spsdepts/edit/<string:SPS_ID>', methods=['GET', 'POST'])
def edit_current_sps(SPS_ID):
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        sps_id = request.form['SPS_ID']
        sps = request.form['SPS'].lstrip().rstrip()
        dept = request.form['Department']
        new_dt = request.form['EFF_DT']
        new_info = SPS_DEPT_INFO(SPS_ID=sps_id, Department=dept, SPS = sps, EFF_DT=new_dt)
        db.session.add(new_info)
        db.session.commit()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'

        engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
        con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
        cur = con.cursor()
        eff_dt = int(request.form['EFF_DT'].replace("-", ""))
        holiday_cal = pd.read_sql('SELECT * FROM public."HOLIDAYS"', engine)
        holiday_cal['DAY_DATE'] = pd.to_datetime(holiday_cal['DAY_DATE'])
        hol_list = pd.read_sql('''Select * from public."NAT_HOLIDAY_FCT"''', con=engine)
        nat_hols = hol_list['DAY_ID'].to_list()
        for i, j in holiday_cal.iterrows():
            if j['DAY_ID'] >= eff_dt and j['DAY_ID'] not in nat_hols:
                holiday_cal.at[holiday_cal['DAY_ID'] == j['DAY_ID'], sps_id] = ''
        cur.execute(''' DELETE FROM public."HOLIDAYS" ''')
        cur.execute('''COMMIT''')
        holiday_cal.to_sql('HOLIDAYS', engine, if_exists='append', index=False)
        cur.close()
        con.close()
        engine.dispose()
        return redirect('/spsdepts')
    else:
        dept_list = ['Seasonal_Auto', 'Playing_Kids', 'Living_Fixing', 'Party_City']
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        all_posts = SPS_CURRENT.query.filter_by(SPS_ID=SPS_ID)
        current_dept = SPS_CURRENT.query.filter_by(SPS_ID=SPS_ID).with_entities(SPS_CURRENT.Department).all()[0][0]
        dep_list = [current_dept] + [x for x in dept_list if x!=current_dept]
        dt_tdy = datetime.today().strftime('%Y-%m-%d')
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('edit_sps.html', info=all_posts, dt = dt_tdy, dl = dep_list)

#View holiday calendar by individual SPS.
@app.route('/holidays/singlesps', methods = ['GET', 'POST'])
@login_required
def view_sps_holiday():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
            sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
        sps_names_list = [sps_names[x] for x in sps_names.keys()]

    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        sps_name = request.form['SPS']
        sps = [x for x in sps_names if sps_names[x] == sps_name][0]
        end_dt = request.form['END_DATE']
        start_dt = request.form['START_DATE']
        if end_dt < start_dt:
            flash('Invalid date range. Please enter a valid date range and try again', 'danger')
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return redirect('/holidays/singlesps')
        tests = HOLIDAYS.query.filter(HOLIDAYS.DAY_DATE >= start_dt, HOLIDAYS.DAY_DATE <= end_dt).order_by(HOLIDAYS.DAY_ID)
        base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM', 'CLNDR_MTH_SHNM', sps]
        sudo_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM', 'CLNDR_MTH_SHNM', sps_name]
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('tests_2.html', cols = base_cols, tests = tests, sudo_cols = sudo_cols)
    dt_tdy = datetime.today().strftime('%Y-%m-%d')
    return render_template('sps_holiday.html', sps_names_list = sps_names_list, dt = dt_tdy)

#View individual sps schedule 
@app.route('/schedule/sps', methods = ['GET', 'POST'])
@login_required
def view_sps_schedule():
    global sps_name_print
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        sps_name = request.form['SPS']
        sps_name_print = sps_name
        sps_nm = [x for x in sps_names if sps_names[x] == sps_name][0]
        end_dt = request.form['END_DATE']
        start_dt = request.form['START_DATE']
        tests = ADI_SPS_CLNDR_VW.query.filter(ADI_SPS_CLNDR_VW.DAY_DATE >= start_dt, ADI_SPS_CLNDR_VW.DAY_DATE <= end_dt, ADI_SPS_CLNDR_VW.sps == sps_nm).order_by(ADI_SPS_CLNDR_VW.DAY_ID)
        base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM', 'LR_Deal','MERCH_RELEASE','POG_NUM','POG_DESC','BUILD_TYPE','CAT_OWNER']
        sudo_cols = ['DAY', 'WKDAY', 'Deal (Hyperlinks below)', 'Merch Release', 'POG', 'POG Desc', 'Build type', 'Cat Owner']
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('tests_3.html', cols = base_cols, sps_name = sps_name, tests = tests, sudo_cols = sudo_cols)
    dt_tdy = datetime.today().strftime('%Y-%m-%d')
    return render_template('sps_schedule.html', sps_names_list = sps_names_list, dt = dt_tdy)

#View schedule by department
@app.route('/schedule/sps_dept', methods = ['GET', 'POST'])
@login_required
def view_dept_schedule():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    if request.method == 'POST':
        start_dt = request.form['START_DATE']
        end_dt = request.form['END_DATE']
        if end_dt < start_dt:
            flash('Invalid date range! End date entered is greater than start date.', 'danger')
            return redirect('/schedule/sps_dept')
        if request.form['action'] == 'Get Department Schedule':
            app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
            dept = request.form['Dept']
            sps_list = sps_dict[dept]
            tests = SCHED_SHORT_INFO.query.filter(SCHED_SHORT_INFO.DAY_DATE >= start_dt,SCHED_SHORT_INFO.DAY_DATE <= end_dt).order_by(SCHED_SHORT_INFO.DAY_ID)   
            #tests = SPS_SCHED_WITH_INFO.query.filter(SPS_SCHED_WITH_INFO.DAY_DATE >= start_dt,SPS_SCHED_WITH_INFO.DAY_DATE <= end_dt).order_by(SPS_SCHED_WITH_INFO.DAY_ID)            
            base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM']
            sudo_base_col_names = ['DAY', 'WKDY']
            sudo_sps_names = [sps_names[x] for x in sps_list]
            cols = base_cols + sps_list
            sudo_cols = sudo_base_col_names + sudo_sps_names
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return render_template('sps_dept_schedule.html', tests=tests, cols=cols, sudo_cols=sudo_cols)
        else:
            app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
            tests = SCHED_SHORT_INFO.query.filter(SCHED_SHORT_INFO.DAY_DATE >= start_dt, SCHED_SHORT_INFO.DAY_DATE <= end_dt).order_by(SCHED_SHORT_INFO.DAY_ID)
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM']
            sudo_base_col_names = ['DAY', 'WKDY']
            dept = ['Seasonal_Auto', 'Playing_Kids','Living_Fixing', 'Party_City']
            sps_list = []
            for dep in dept:
                sps_list = sps_list + sps_dict[dep]
            sudo_sps_names = [sps_names[x] for x in sps_list]
            cols = base_cols + sps_list
            sudo_cols = sudo_base_col_names + sudo_sps_names
            return render_template('sps_dept_full_schedule.html', tests=tests,  sudo_cols=sudo_cols, cols=cols)
    dt_tdy = datetime.today().strftime('%Y-%m-%d')
    return render_template('find_dept_schedule.html', dt=dt_tdy)

#View SPS holiday by department
@app.route('/holidays', methods=['GET', 'POST'])
@login_required
def get_holidays():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        start_dt = request.form['START_DATE']
        end_dt = request.form['END_DATE']
        if end_dt < start_dt:
            flash('Invalid date range! End date entered is greater than start date.', 'danger')
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return redirect('/holidays')
        dept = request.form['Dept']
        sps_list = sps_dict[dept]
        tests = HOLIDAYS.query.filter(HOLIDAYS.DAY_DATE >= start_dt, HOLIDAYS.DAY_DATE <= end_dt).order_by(HOLIDAYS.DAY_ID)
        base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM', 'CLNDR_MTH_SHNM']
        sudo_base_col_names = ['DAY', 'WKDY', 'MNTH']
        sudo_sps_names = [sps_names[x] for x in sps_list]
        cols = base_cols + sps_list
        sudo_cols = sudo_base_col_names + sudo_sps_names
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('tests.html', tests=tests, cols=cols, sudo_cols = sudo_cols)
    dt_tdy = datetime.today().strftime('%Y-%m-%d')
    return render_template('find_holidays.html', dt = dt_tdy)

#Add miscellaneous task for SPS.
@app.route('/schedule/misc', methods=['GET', 'POST'])
@login_required
def update_misc_schedule():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]

    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        start_dt = request.form['START_DATE']
        end_dt = request.form['END_DATE']
        task = request.form['TASK']
        desc = request.form['DESC']
        if end_dt < start_dt:
            flash('Invalid date range! End date entered is greater than start date.', 'danger')
            return redirect('/schedule/misc')
        sps_name = request.form['SPS']
        sps_id = [x for x in sps_names if sps_names[x] == sps_name][0]
        deal = request.form['DEAL']
        new_task = SPS_MISC_SCHED(DEAL = deal, START_DATE = start_dt, END_DATE = end_dt, SPS = sps_id, TASK = task, DESC = desc)
        db.session.add(new_task)
        db.session.commit()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        flash('Misc record added.', 'success')
        return redirect('/schedule/misc')
    else:
        flash('While adding a new Misc record, please ensure there are no special characters like "/". Additionally, please ensure to have a unique combination of DEAL, TASK type and Task description. This will enable smooth edits for non regular builds.', 'warning')
        return render_template('sps_misc_sched.html', sps_names_list  = sps_names_list)

#Update SPS vacation
@app.route('/holidays/add', methods=['GET', 'POST'])
@login_required
def update_sps_holidays():
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)
    
    super_users = df1['user_role_name'].to_list()
    engine.dispose()
    super_users = super_users + ['postgres']
    if session['username'] not in super_users:
        flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
        return redirect('/home')
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    if request.method == 'POST':        
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        sps_name = request.form['SPS']
        if request.form['options'] == 'Holiday':
            val = 'x'
        else:
            val = ''
        sps =[x for x in sps_names if sps_names[x] == sps_name][0]
        start_dt = int(request.form['START_DATE'].replace("-", ""))
        end_dt = int(request.form['END_DATE'].replace("-", ""))
        if end_dt < start_dt:
            flash('Invalid date range. End date entered is greater than start date', 'danger')
            return redirect('/holidays/add')
        holiday_cal = pd.read_sql('SELECT * FROM public."HOLIDAYS"', engine)
        holiday_cal['DAY_DATE'] = pd.to_datetime(holiday_cal['DAY_DATE'])
        hol_list = pd.read_sql('''Select * from public."NAT_HOLIDAY_FCT"''', con =  engine)

        nat_hols = hol_list['DAY_ID'].to_list()
        for i, j in holiday_cal.iterrows():
            if j['DAY_ID'] >= start_dt and j['DAY_ID'] <= end_dt and j['DAY_ID'] not in nat_hols:
                holiday_cal.at[holiday_cal['DAY_ID'] == j['DAY_ID'], sps] = val
                
        cur.execute(''' DELETE FROM public."HOLIDAYS" ''')
        cur.execute('''COMMIT''')
        holiday_cal.to_sql('HOLIDAYS', engine, if_exists='append', index=False)  
        
        cur.close()
        con.close()
        engine.dispose()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        flash('Holidays have been updated for ' + sps_name + ' from ' + str(request.form['START_DATE']) + ' to ' + str(request.form['END_DATE']) + '!', 'success')
        return redirect('/holidays/add')
    return render_template('update_sps_holidays.html', sps_names_list = sps_names_list)

#Deal information page url.
@app.route('/deals', methods=['GET', 'POST'])
@login_required
def deals():
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal_id = request.form['DEAL_NUM']
        if request.form['action'] == 'Detailed info':
            query = DEAL_FULL_INFO_VW.query.filter_by(Deal_prefix=deal_id).all()  # .with_entities(DEAL_FULL_INFO_VW.Deal_prefix, DEAL_FULL_INFO_VW.LR_ID, DEAL_FULL_INFO_VW.LR_Deal, DEAL_FULL_INFO_VW.POG_NUM, DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.Dealer_mail_date, DEAL_FULL_INFO_VW.Target_store_setup).all()
            if len(query) == 0:
                flash('Deal Id: ' + deal_id + ' is not valid!', 'danger')
                app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
                return redirect('/deals')
            query_2 = DEAL_VW_SUMM_SHOW.query.filter_by(Deal_Num = deal_id).all()
            cols = DEAL_VW_SUMM_SHOW.__table__.columns.keys()
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return render_template('deal_sched_vw.html', cols = cols, data = query_2)
        elif request.form['action'] == 'View':
            engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
            df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)    
            super_users = df1['user_role_name'].to_list()
            engine.dispose()
            super_users = super_users + ['postgres']
            if session['username'] not in super_users:
                flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
                return redirect('/deals')
            query = DEAL_FULL_INFO_VW.query.filter_by(Deal_prefix=deal_id).all()  # .with_entities(DEAL_FULL_INFO_VW.Deal_prefix, DEAL_FULL_INFO_VW.LR_ID, DEAL_FULL_INFO_VW.LR_Deal, DEAL_FULL_INFO_VW.POG_NUM, DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.Dealer_mail_date, DEAL_FULL_INFO_VW.Target_store_setup).all()
            if len(query) == 0:
                flash('Deal Id: ' + deal_id + ' is not valid!', 'danger')
                app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
                return redirect('/deals')
            cols = ['LR_ID', 'LR_Deal', 'POG_NUM', 'POG_DESC', 'O_start_date', 'Dealer_mail_date', 'MERCH_RELEASE']
            data = query
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return render_template('deal_info.html', cols = cols, data=data)
        elif request.form['action'] == 'Placeholder':
            engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
            df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)    
            super_users = df1['user_role_name'].to_list()
            engine.dispose()
            super_users = super_users + ['postgres']
            if session['username'] not in super_users:
                flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
                return redirect('/deals')
            query = DEAL_FULL_INFO_VW.query.filter_by(Deal_prefix=deal_id).all()  # .with_entities(DEAL_FULL_INFO_VW.Deal_prefix, DEAL_FULL_INFO_VW.LR_ID, DEAL_FULL_INFO_VW.LR_Deal, DEAL_FULL_INFO_VW.POG_NUM, DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.Dealer_mail_date, DEAL_FULL_INFO_VW.Target_store_setup).all()
            if len(query) == 0:
                flash('Deal Id: ' + deal_id + ' is not valid!', 'danger')
                app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
                return redirect('/deals')
            query_2 = DEAL_VW_SUMM_SHOW.query.filter_by(Deal_Num = deal_id).all()
            cols = DEAL_VW_SUMM_SHOW.__table__.columns.keys()
            cols = [x for x in cols if x not in ['MERCH_RELEASE', 'BUILD_DAYS']]
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return render_template('deal_pog_hold.html', cols = cols, data = query_2)
        else:
            start_dt = request.form['START_DATE']
            end_dt = request.form['END_DATE']
            query_2 = DEAL_FULL_INFO_VW.query.filter(DEAL_FULL_INFO_VW.O_start_date>=start_dt, DEAL_FULL_INFO_VW.O_start_date<=end_dt).order_by(DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.LR_Deal) 
            cols = ['LR_Deal', 'Department', 'POG_NUM' ,'POG_DESC', 'SPS', 'O_start_date', 'MERCH_RELEASE']
            return render_template('deal_info_detailed.html', cols=cols, data=query_2)
    return render_template('find_deals.html')

#When we want to prioritize a POG in a deal. That is, if we want certain POGs to be scheduled before others, this method ensures that these POGs get priority.
@app.route('/deals/priority/<string:LR_Deal>/<string:POG_NUM>', methods = ['GET', 'POST'])
def priority_pog(LR_Deal, POG_NUM):
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "PRIORITY" = 'Y' where "LR_Deal" = %s and "POG_NUM" = %s ''',[LR_Deal, POG_NUM])
    cur.execute('''COMMIT''')
    flash('POG ' + POG_NUM + ' has been prioritized.', 'success')
    return redirect('/deals')

#Add a new deal in SPOT
@app.route('/add_deal', methods = ['GET', 'POST'])
def add_deal():
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)
    
    super_users = df1['user_role_name'].to_list()
    engine.dispose()
    super_users = super_users + ['postgres']
    if session['username'] not in super_users:
        flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
        return redirect('/deals')
    if request.method == 'POST':
        cpr = request.form['CPR']
        deal_id = request.form['DEAL_ID']
        lr_deal = request.form['DEAL_num']
        O_start_dt = request.form['O_START_DATE']
        O_end_dt = request.form['O_END_DATE']
        dealer_mail_dt = request.form['DEALER_MAIL_DATE']
        target_setup_dt = request.form['Target_store_setup']
        merch_dt = request.form['MERCH_RELEASE']
        pog_num = request.form['POG']
        deal_pref = lr_deal[:4]
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        new_deal_added = CPR_DEAL_POG_INFO(CPR=cpr, LR_ID=deal_id, LR_Deal=lr_deal, O_start_date=O_start_dt,
                                          O_end_date=O_end_dt, Dealer_mail_date=dealer_mail_dt,
                                          Target_store_setup=target_setup_dt, POG_NUM=pog_num,
                                          Deal_prefix=deal_pref, MERCH_RELEASE=merch_dt)
        db.session.add(new_deal_added)
        db.session.commit()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('new_deal.html')
    return render_template('new_deal.html')

#Shows deal level summary
@app.route('/detailed_tab/<string:LR_Deal>')
def get_deal_schedule(LR_Deal):
    deal_id = LR_Deal[:4]
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    query_2 = DEAL_VW_SUMM_SHOW.query.filter_by(Deal_Num = deal_id).all()
    cols = DEAL_VW_SUMM_SHOW.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('deal_sched_vw.html', cols = cols, data = query_2)

@app.route('/missed_deals_tab/<string:LR_Deal>')
def get_missed_deal_info(LR_Deal):
    deal_id = LR_Deal.replace('/', '_')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    query_2 = DEALS_MISSED_DATES.query.filter_by(DEAL = deal_id).all()
    cols = DEALS_MISSED_DATES.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('missed_deals_upd.html', cols = cols, data = query_2)
    
 #Add a new POG   
@app.route('/pogs/new', methods=['GET', 'POST'])
@login_required
def new_pogs():
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    df1 = pd.read_sql('''select a.oid as user_role_id , a.rolname as user_role_name, b.roleid as other_role_id , c.rolname as other_role_name from pg_roles a inner join pg_auth_members b on a.oid=b.member inner join pg_roles c on b.roleid=c.oid where c.rolname = 'super_user' ''', con=engine)
    
    super_users = df1['user_role_name'].to_list()
    engine.dispose()
    super_users = super_users + ['postgres']
    if session['username'] not in super_users:
        flash('You are either not allowed to perform this operation or the system encountered an error. Please contact your system admin if this problem persists.', 'danger')
        return redirect('/pogs/find')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        pog_id = request.form['POG_NUM']
        pog_desc = request.form['POG_DESC']
        pog_owner = request.form['SPS']
        v_owner = request.form['SPS_V']
        SPS_ID =  [x for x in sps_names if sps_names[x] == pog_owner][0]
        V_BUILDER = [x for x in sps_names if sps_names[x] == v_owner][0]
        recco_O = request.form['Recco_O']
        recco_V = request.form['Recco_V']
        ESL = request.form['ESL_DIFF_INDEX']
        Difficulty = request.form['Difficulty']
        extd_build = request.form['Extd_build']
        new_pog = POG_CAT_OWNERS(POG_NUM=pog_id, POG_DESC=pog_desc, SPS_ID =SPS_ID, Recco_O = recco_O, Recco_V = recco_V, ESL_DIFF_INDEX = ESL, Difficulty = Difficulty, V_BUILDER = V_BUILDER, EXTD_BUILD = extd_build)
        db.session.add(new_pog)
        db.session.commit()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        flash('POG ' + pog_id + ' has been successfully added!', 'success')
        return redirect('/pogs/find')
    else:
        return render_template('new_pogs.html', sps_names_list  =sps_names_list)

#Edit deal information. We can add POGs to a deal here.
@app.route('/deals/edit/<string:LR_ID>', methods=['GET', 'POST'])
#@login_required
def edit_deal(LR_ID):
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal_id = request.form['LR_ID'].rstrip().lstrip()
        lr_deal = request.form['LR_Deal']
        pog_num = request.form['POG']
        o_start_dt = request.form['O_start_date']
        o_end_dt = request.form['O_end_date']
        dealer_mail_dt = request.form['Dealer_mail_date']
        target_store_setup = request.form['Target_store_setup']
        ordering_ppk = request.form['ORD_PPK'].rstrip().lstrip()
        change_ind = request.form['CHANGE_IND'].rstrip().lstrip()
        merch_release = request.form['MERCH_RELEASE']
        CPR = pd.read_sql('''SELECT distinct "CPR" FROM public."CPR_DEAL_POG_INFO" where "LR_ID" =%s ''',  engine, params=[deal_id])
        CPR = CPR['CPR'][0]
        deal_pref = lr_deal[:4]
        new_pog_added = CPR_DEAL_POG_INFO(CPR=CPR, LR_ID=deal_id, LR_Deal=lr_deal, O_start_date=o_start_dt, O_end_date=o_end_dt, Dealer_mail_date=dealer_mail_dt, Target_store_setup=target_store_setup, POG_NUM=pog_num, ORDERING_PPK_POG=ordering_ppk, Deal_prefix=deal_pref, CHANGE_IND = change_ind, MERCH_RELEASE=merch_release)
        db.session.add(new_pog_added)
        db.session.commit()
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "CHANGE_IND" = 'Y' WHERE "LR_ID" = %s''', [deal_id])
        cur.execute('''COMMIT''')
        cur.close()
        con.close()
        engine.dispose()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return redirect('/deals')
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal_info = CPR_DEAL_POG_INFO.query.filter_by(LR_ID=LR_ID).with_entities(CPR_DEAL_POG_INFO.LR_ID, CPR_DEAL_POG_INFO.LR_Deal, CPR_DEAL_POG_INFO.O_start_date, CPR_DEAL_POG_INFO.O_end_date, CPR_DEAL_POG_INFO.Dealer_mail_date, CPR_DEAL_POG_INFO.Target_store_setup, CPR_DEAL_POG_INFO.CHANGE_IND, CPR_DEAL_POG_INFO.MERCH_RELEASE).distinct().all()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('edit_deal.html', info=deal_info)

#Edit deal information - deal dates.
@app.route('/deals/edit_dates/<string:LR_ID>', methods=['GET', 'POST'])
#@login_required
def edit_deal_dates(LR_ID):
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    if request.method == 'POST':
        deal_id = request.form['LR_ID'].rstrip().lstrip()
        lr_deal = request.form['LR_Deal']
        o_start_dt = request.form['O_start_date']
        o_end_dt = request.form['O_end_date']
        dealer_mail_dt = request.form['Dealer_mail_date']
        target_store_setup = request.form['Target_store_setup']
        merch_release = request.form['MERCH_RELEASE']
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "O_start_date" = %s WHERE "LR_ID" = %s''', [o_start_dt, deal_id])
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "O_end_date" = %s WHERE "LR_ID" = %s''', [o_end_dt, deal_id])
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "Dealer_mail_date" = %s WHERE "LR_ID" = %s''', [dealer_mail_dt, deal_id])
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "MERCH_RELEASE" = %s WHERE "LR_ID" = %s''', [merch_release, deal_id])
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "Target_store_setup" = %s WHERE "LR_ID" = %s''', [target_store_setup, deal_id])
        cur.execute(''' UPDATE public."CPR_DEAL_POG_INFO" SET "CHANGE_IND" = 'Y' WHERE "LR_ID" = %s''', [deal_id])
        cur.execute('''COMMIT''')
        cur.close()
        con.close()
        return redirect('/deals')
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal_info = CPR_DEAL_POG_INFO.query.filter_by(LR_ID=LR_ID).with_entities(CPR_DEAL_POG_INFO.LR_ID, CPR_DEAL_POG_INFO.LR_Deal, CPR_DEAL_POG_INFO.O_start_date, CPR_DEAL_POG_INFO.O_end_date, CPR_DEAL_POG_INFO.Dealer_mail_date, CPR_DEAL_POG_INFO.MERCH_RELEASE, CPR_DEAL_POG_INFO.Target_store_setup).distinct().all()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('edit_deal_dates.html', info=deal_info)

#Lets us delete POGs from a deal.
@app.route('/deals/delete/<string:LR_ID>/<string:POG_NUM>', methods=['GET', 'POST'])
#@login_required
def del_deal_pog(LR_ID, POG_NUM):
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    cur.execute(''' DELETE FROM public."CPR_DEAL_POG_INFO" where "LR_ID" = %s and "POG_NUM" = %s''',[LR_ID, POG_NUM])
    cur.execute('''COMMIT''')
    cur.close()
    con.close()
    return redirect('/deals')

#View miscellaneous tasks assigned to SPS'
@app.route('/view_misc', methods = ['GET', 'POST'])
@login_required
def view_misc_deals():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    dt_tdy = datetime.today().strftime('%Y-%m-%d')
    qry = MISC_VW.query.all()
    cols = MISC_VW.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('misc_deals_vw.html', cols = cols, tests = qry)

#Find POGs by POG# entered.
@app.route('/pogs/find', methods=['GET', 'POST'])
@login_required
def search_page():
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        if request.form['action'] == 'Submit':
            try:
                pog_id = request.form['POG_NUM']
                pog_info = POG_CAT_OWNERS_VW.query.get_or_404(pog_id)
                app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
                return render_template('pogs_found_results.html', pog_info=pog_info)
            except:
                app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
                flash(pog_id + ' is not a valid POG. Please enter a valid POG number.', 'danger')
                return redirect('/pogs/find')
        else: #Get list of all POGs owned by an SPS.
            sps_name = request.form['SPS']
            all_sps = SPS_CURRENT.query.all()
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            sps_dict = {}
            for row in all_sps:
                if row.Department not in sps_dict:
                    sps_dict[row.Department] = []
                    sps_dict[row.Department].append(row.SPS_ID)
            sps_names = {}
            for row in all_sps:
                sps_names[row.SPS_ID] = row.SPS
                sps_names_list = [sps_names[x] for x in sps_names.keys()]
            sps = [x for x in sps_names if sps_names[x] == sps_name][0]
            all_pogs = POG_CAT_OWNERS_VW.query.filter(POG_CAT_OWNERS_VW.SPS==sps_name)
            cols = POG_CAT_OWNERS_VW.__table__.columns.keys()
            cols = [x for x in cols if x not in ['Difficulty', 'V_BUILDER', 'SPS_ID']]
            app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
            return render_template('pog_cat_owners.html', cols = cols, tests = all_pogs)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
            sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
        sps_names_list = [sps_names[x] for x in sps_names.keys()]
    return render_template('find_pogs.html', sps_names_list = sps_names_list)

#Fetches POG info
@app.route('/pog_info/<string:POG_NUM>')
def view_pog_info(POG_NUM):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    pog_id = POG_NUM
    pog_info = POG_CAT_OWNERS_VW.query.get_or_404(pog_id)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('pogs_found_results.html', pog_info=pog_info)

#Delete POG from SPOT db.
@app.route('/pogs/delete/<string:POG_NUM>', methods=['GET', 'POST'])
#@login_required
def delete_pog(POG_NUM):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    pog = POG_CAT_OWNERS.query.get_or_404(POG_NUM)
    db.session.delete(pog)
    db.session.commit()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return redirect('/pogs/find')

#Edit POG info.
@app.route('/pogs/edit/<string:POG_NUM>', methods=['GET', 'POST'])
#@login_required
def edit_pogs(POG_NUM):
    form = Form()
    form1 = Form()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    pog = POG_CAT_OWNERS.query.get_or_404(POG_NUM)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    prior_sps_id = pog.SPS_ID
    prior_vbuilder = pog.V_BUILDER
    prior_ESL = pog.ESL_DIFF_INDEX
    prior_diff = pog.Difficulty
    ESL_list = [0,1,2,3]
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        pog.POG_NUM = request.form['POG_NUM']
        pog.POG_DESC = request.form['POG_DESC']
        pog.Recco_O = request.form['Recco_O']
        pog.Recco_V = request.form['Recco_V']
        pog.ESL_DIFF_INDEX = request.form['ESL_DIFF_INDEX']
        pog.SPS_ID = form.SPS.data
        pog.V_BUILDER = form1.V.data
        pog.Difficulty = request.form['Difficulty']
        pog.EXTD_BUILD = request.form['Extd_build']
        db.session.commit()
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        flash('POG ' + pog.POG_NUM + ' has been updated successfully.', 'success')
        return redirect('/pogs/find')
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        orig_owner = [(x.SPS_ID, x.SPS) for x in SPS_CURRENT.query.all() if x.SPS_ID==prior_sps_id]
        v_owner = [(x.SPS_ID, x.SPS) for x in SPS_CURRENT.query.all() if x.SPS_ID==prior_vbuilder]
        form.SPS.choices = orig_owner + [(x.SPS_ID, x.SPS) for x in SPS_CURRENT.query.all() if x.SPS_ID!=prior_sps_id]
        form1.V.choices = v_owner + [(x.SPS_ID, x.SPS) for x in SPS_CURRENT.query.all() if x.SPS_ID != prior_vbuilder]
        ESL_drop_down_list = [prior_ESL] + [x for x in ESL_list if x!=prior_ESL]
        diff_drop_down = [prior_diff] +  [x for x in ESL_list if x!=prior_diff]
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('edit_pog_info.html', pog=pog, form=form, form1=form1, d_list = ESL_drop_down_list, diff_drop_down = diff_drop_down)

#Change user password.
@app.route('/pswd_chg', methods = ['GET', 'POST'])
@login_required
def pswd_chg():
    if request.method == 'POST':
        username = session['username']
        pass_1 = request.form['pass_1']
        pass_2 = request.form['pass_2']
        if pass_1 != pass_2:
            flash("Passwords don't match. Please try again", 'danger')
            return redirect('/pswd_chg')
        con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
        cur = con.cursor()
        statement = '''ALTER USER {} WITH PASSWORD %s '''.format(session['username'])
        cur.execute(statement, [pass_1])
        cur.execute('''COMMIT''')
        session['password'] = pass_2
        cur.close()
        con.close()
        flash('Password successfully reset!', 'success')
        return redirect('/pswd_chg')
    username = session['username']
    return render_template('pswd_change.html', username=username)

#User manual link to doc.
@app.route('/manual')
@login_required
def show_static_pdf():
    return send_file('SPOT.pdf')

#View all unassigned POGs for the generated schedule.
@app.route('/unassigned', methods = ['GET', 'POST'])
@login_required
def get_unassigned_builds():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_unassigned = unassigned_vw.query.all()
    cols =  unassigned_vw.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('unassigned_builds.html', cols = cols, all_sps = all_unassigned)

#View schedule by department 
@app.route('/calendar_view/<string:START_DT>/<string:DEPT>', methods = ['GET', 'POST'])
def view_cal(START_DT, DEPT):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_sps = SPS_CURRENT.query.all()
    sps_dict = {}
    for row in all_sps:
        if row.Department not in sps_dict:
            sps_dict[row.Department] = []
        sps_dict[row.Department].append(row.SPS_ID)
    sps_names = {}
    for row in all_sps:
        sps_names[row.SPS_ID] = row.SPS
    sps_names_list = [sps_names[x] for x in sps_names.keys()]
    START_DT = START_DT.replace('-', '')
    start_dte = datetime(year=int(START_DT[0:4]), month=int(START_DT[4:6]), day=int(START_DT[6:8]))
    end_dt = start_dte + pd.DateOffset(days=21)
    sps_list = sps_dict[DEPT]
    tests = SCHED_SHORT_INFO.query.filter(SCHED_SHORT_INFO.DAY_DATE >= start_dte, SCHED_SHORT_INFO.DAY_DATE <= end_dt).order_by(SCHED_SHORT_INFO.DAY_ID)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    base_cols = ['DAY_DATE', 'DAY_WKDAY_SHNM']
    sudo_base_col_names = ['DAY', 'WKDY']
    sudo_sps_names = [sps_names[x] for x in sps_list]
    cols = base_cols + sps_list
    sudo_cols = sudo_base_col_names + sudo_sps_names
    return render_template('sps_dept_schedule.html', tests=tests, cols=cols, sudo_cols=sudo_cols)

#Delete miscellaneous deals.
@app.route('/misc/delete/<string:DEAL>/<string:TASK>/<string:DESC>/<string:Id>',methods = ['GET', 'POST'])
def del_misc(DEAL, TASK, DESC, Id):
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    statement = '''Delete from public."SPS_MISC_SCHED" where "DEAL"= %s and "TASK" = %s and "DESC"  = %s and "SPS" = %s '''
    cur.execute(statement, [DEAL, TASK, DESC, Id])
    cur.execute('''COMMIT''')
    cur.close()
    con.close()
    return redirect('/view_misc')

#Edit miscellaneous deal.
@app.route('/misc/edit/<string:DEAL>/<string:TASK>/<string:DESC>/<string:Id>', methods = ['GET', 'POST'])
def edit_misc_deal(DEAL, TASK, DESC, Id):
    if request.method == 'POST':
        start_dt = request.form['START_DATE']
        end_dt = request.form['END_DATE']
        DEAL = request.form['DEAL']
        TASK = request.form['TASK']
        DESC = request.form['DESC']
        Id = request.form['Id']
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        all_sps = SPS_CURRENT.query.all()
        app.config['SQLALCHEMY_DATABASE_URI'] =  'reset'
        sps_names = {}
        for row in all_sps:
            sps_names[row.SPS_ID] = row.SPS
        SPS = [x for x in sps_names if sps_names[x] == request.form['SPS']][0]
        con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
        cur = con.cursor()
        cur.execute('''UPDATE public."SPS_MISC_SCHED" set "START_DATE" = %s where "DEAL" = %s and "TASK" = %s and "DESC" = %s''', [start_dt, DEAL, TASK, DESC])
        cur.execute('''UPDATE public."SPS_MISC_SCHED" set "END_DATE" = %s where "DEAL" = %s and "TASK" = %s and "DESC" = %s''', [end_dt, DEAL, TASK, DESC])
        cur.execute('''UPDATE public."SPS_MISC_SCHED" set "SPS" = %s where "DEAL" = %s and "TASK" = %s and "DESC" = %s''', [SPS, DEAL, TASK, DESC])
        cur.execute('''COMMIT''')
        cur.close()
        con.close()
        return redirect('/view_misc')
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        all_sps = SPS_CURRENT.query.all()
        sps_names = {}
        for row in all_sps:
            sps_names[row.SPS_ID] = row.SPS
        sps_names_list = [sps_names[x] for x in sps_names.keys() if x!=Id]
        sps_names_final = [sps_names[Id]] + sps_names_list
        info = MISC_VW.query.filter(MISC_VW.DEAL == DEAL, MISC_VW.TASK == TASK, MISC_VW.DESC == DESC, MISC_VW.Id == Id)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
        return render_template('sps_misc_edit.html', info = info, sps_names = sps_names_final)

@app.route('/dup_vbuilds',methods = ['GET', 'POST'])
@login_required
def view_dup_v_builds():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_dup_builds = DUP_V_BUILDS_VW.query.all()
    cols =  DUP_V_BUILDS_VW.__table__.columns.keys()
    return render_template('dup_v_builds.html', cols = cols, all_dup_builds = all_dup_builds)


@app.route('/missed_deals', methods = ['GET','POST'])
@login_required
def get_missed_deals():
    if request.method == 'POST':
        start_dt = request.form['START_DATE']
        end_dt = request.form['END_DATE']
        
        session['m_start_dt'] = start_dt
        session['m_end_dt'] = end_dt
        
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        query = DEAL_FULL_INFO_VW.query.filter(DEAL_FULL_INFO_VW.O_start_date>=start_dt, DEAL_FULL_INFO_VW.O_start_date<=end_dt).order_by(DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.LR_Deal).with_entities(DEAL_FULL_INFO_VW.LR_Deal)
        query = query.distinct()
        cols = ['LR_Deal']
        return render_template('missed_deals_tbl.html', cols = cols, query = query)
    return render_template('missed_deals.html')

  
@app.route('/option_2/<string:LR_Deal>', methods=['GET', 'POST'])
def option2(LR_Deal):
    deal = LR_Deal
    reason = 'Proceed with caution'
    return render_template('missed_deals_more_info.html', deal = deal, reason = reason)     

@app.route('/option_3/<string:LR_Deal>', methods=['GET', 'POST'])
def option3(LR_Deal):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    deal = LR_Deal
    reason = 'Gate3 delayed'
    return render_template('missed_deals_gate3_info.html', deal = deal, reason = reason)    

@app.route('/update_missed_skus', methods = ['GET', 'POST'])
def update():
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal = request.form['deal'].replace('/', '_')
        ea_skus = int(float(request.form['ea_skus']))
        ppk_skus = int(float(request.form['ppk_skus']))
        new_skus = int(float(request.form['new_skus']))
        mock_skus = int(float(request.form['mock_skus']))
        samples_provided = int(float(request.form['samples_provided']))
        actual_skus = int(float(request.form['actual_skus']))
        total = int(float(request.form['total']))
        eff_dt = datetime.today().strftime('%Y-%m-%d')
        reason = request.form['reason']
        new_entry = DEALS_MISSED_DATES(DEAL=deal, EFF_DT=eff_dt, REASON=reason,NEW_SKUS = new_skus, EA_SKUS = ea_skus, PPK_SKUS = ppk_skus, ACTUAL_SKUS = actual_skus, MOCK_SKUS = mock_skus, SAMPLES_PROVIDED = samples_provided, TOTAL_SKUS = total)
        db.session.add(new_entry)
        db.session.commit()
        query = DEAL_FULL_INFO_VW.query.filter(DEAL_FULL_INFO_VW.O_start_date>=session['m_start_dt'], DEAL_FULL_INFO_VW.O_start_date<=session['m_end_dt']).order_by(DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.LR_Deal).with_entities(DEAL_FULL_INFO_VW.LR_Deal)
        query = query.distinct()
        cols = ['LR_Deal']
        flash(deal + ' successfully recorded.', 'success')
        return render_template('missed_deals_tbl.html', cols = cols, query = query)


@app.route('/update_gate3_dt', methods = ['GET', 'POST'])
def update_gate3_dt():
    if request.method == 'POST':
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
        deal = request.form['deal'].replace('/', '_')
        eff_dt = datetime.today().strftime('%Y-%m-%d')
        reason = request.form['reason']
        gate3_dt = request.form['gate3_dt']
        new_entry = DEALS_MISSED_DATES(DEAL=deal, EFF_DT=eff_dt, REASON=reason,TENTATIVE_GATE3_DT = gate3_dt)
        db.session.add(new_entry)
        db.session.commit()
        query = DEAL_FULL_INFO_VW.query.filter(DEAL_FULL_INFO_VW.O_start_date>=session['m_start_dt'], DEAL_FULL_INFO_VW.O_start_date<=session['m_end_dt']).order_by(DEAL_FULL_INFO_VW.O_start_date, DEAL_FULL_INFO_VW.LR_Deal).with_entities(DEAL_FULL_INFO_VW.LR_Deal)
        query = query.distinct()
        cols = ['LR_Deal']
        flash(deal + ' successfully recorded.', 'success')
        return render_template('missed_deals_tbl.html', cols = cols, query = query)

@app.route('/option_4/<string:LR_Deal>', methods=['GET', 'POST'])
def option4(LR_Deal):
    deal = LR_Deal
    reason = 'Deal on time'
    return render_template('missed_deals_more_info.html', deal = deal, reason = reason) 

@app.route('/pog_hold/<string:Deal_Num>/<string:POG>/<string:BUILD_TYP>/<string:START_DT>/<string:END_DT>', methods=['GET', 'POST'])
def pog_hold(Deal_Num, POG, BUILD_TYP, START_DT, END_DT):
    import datetime as dt
    if BUILD_TYP not in ['OPOG', 'VPOG', 'ORDERING_PPK']:
        flash('Can only freeze O and V builds. Freezing a V build will also freeze ESLs for the POG', 'danger')
        return redirect('/deals')
    START_DT = START_DT.replace('-', '')
    start_dte = datetime(year=int(START_DT[0:4]), month=int(START_DT[4:6]), day=int(START_DT[6:8]))
    int_strt = int(START_DT.replace('-', ''))
    int_end = int(END_DT.replace('-', ''))
    thursday_dt = dt.date.today()
    while thursday_dt.weekday() != 3:
        thursday_dt += dt.timedelta(1)
    last_dt = thursday_dt + dt.timedelta(6)
    datelist = [y for y in pd.date_range(thursday_dt, last_dt).tolist()]
    if start_dte not in datelist:
        flash("Can't freeze this POG. Please try freezing a POG with an earlier start date.", 'danger')
        return redirect('/deals')
    freeze_flg = BUILD_TYP[0]
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    cur.execute('''UPDATE public."CPR_DEAL_POG_INFO" set "FREEZE" = %s where "Deal_prefix" = %s and "POG_NUM" = %s''', [freeze_flg, Deal_Num, POG])
    if freeze_flg == 'O':
        cur.execute('''UPDATE public."SPS_DATASET" set "FREEZE" = 'Y' where substring("DEAL",1,4) = %s and "POG" = %s and "BUILD_TYP" = %s and "DAY" >= %s and "DAY" <= %s''', [Deal_Num, POG, BUILD_TYP, int_strt, int_end])
        cur.execute('''COMMIT''')
    else:
        cur.execute('''UPDATE public."SPS_DATASET" set "FREEZE" = 'Y' where substring("DEAL",1,4) = %s and "POG" = %s and "BUILD_TYP" in ('VPOG', 'ESL', 'EXTD')and "DAY" >= %s ''', [Deal_Num, POG, int_strt])
        cur.execute('''COMMIT''')
    cur.close()
    con.close()    
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')            
    query_2 = DEAL_VW_SUMM_SHOW.query.filter_by(Deal_Num = Deal_Num[:4]).all()
    cols = DEAL_VW_SUMM_SHOW.__table__.columns.keys()
    cols = [x for x in cols if x not in ['MERCH_RELEASE', 'BUILD_DAYS']]
    engine.dispose()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    flash(freeze_flg + ' build of POG ' + POG + ' belonging to deal ' + Deal_Num + ' has been frozen. Please check that POG builder is not on vacation or been assigned a MISC task.', 'success')
    return render_template('deal_pog_hold.html', cols = cols, data = query_2)

@app.route('/pog_release/<string:Deal_Num>/<string:POG>/<string:BUILD_TYP>/<string:START_DT>', methods=['GET', 'POST'])
def pog_release(Deal_Num, POG, BUILD_TYP, START_DT):
    import datetime as dt
    if BUILD_TYP not in ['OPOG', 'VPOG', 'ORDERING_PPK']:
        flash('Can only release O and V builds.', 'danger')
        return redirect('/deals')
    START_DT = START_DT.replace('-', '')
    start_dte = datetime(year=int(START_DT[0:4]), month=int(START_DT[4:6]), day=int(START_DT[6:8]))
    int_strt = int(START_DT.replace('-', ''))
    con = p.connect(database="scheduling", user=session['username'], password=session['password'], host="mpm-spot-db.mysql.database.azure.com", port="3306")
    cur = con.cursor()
    cur.execute('''UPDATE public."CPR_DEAL_POG_INFO" set "FREEZE" = '' where "Deal_prefix" = %s and "POG_NUM" = %s''', [Deal_Num, POG])
    cur.execute('''UPDATE public."SPS_DATASET" set "FREEZE" = '' where substring("DEAL",1,4) = %s and "POG" = %s and "DAY" >= %s ''', [Deal_Num, POG, int_strt])
    cur.execute('''COMMIT''')
    cur.close()
    con.close()    
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    engine = create_engine('postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling')            
    query_2 = DEAL_VW_SUMM_SHOW.query.filter_by(Deal_Num = Deal_Num[:4]).all()
    cols = DEAL_VW_SUMM_SHOW.__table__.columns.keys()
    cols = [x for x in cols if x not in ['MERCH_RELEASE', 'BUILD_DAYS']]
    engine.dispose()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    flash('POG #' + POG + ' hold' + ' belonging to deal ' + Deal_Num + ' has been released.', 'success')
    return render_template('deal_pog_hold.html', cols = cols, data = query_2)

#View all unassigned POGs for the generated schedule.
@app.route('/placeholders', methods = ['GET', 'POST'])
@login_required
def get_placeholders():
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + session['username'] + ':' + session['password'] + '@host/scheduling'
    all_placeholders = PLACEHOLDER_VW.query.all()
    cols =  PLACEHOLDER_VW.__table__.columns.keys()
    app.config['SQLALCHEMY_DATABASE_URI'] = 'reset'
    return render_template('placeholders.html', cols = cols, all_sps = all_placeholders)

#Logout from SPOT.
@app.route('/logout')
@login_required
def logout():
    for k in ['username', 'password']:
        session.pop(k)
    flash('Successfully logged out!','success')
    return redirect('/')

#Logs out user if no activity for 30 mins.
@app.before_request
def make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=30)

#Default message when an error is encountered.
@app.errorhandler(500)
def internal_error(error):
    flash('You either do not have the permission to perform this operation or the system encountered an error. If this problem persists, please contact the system administrator', 'danger')
    return redirect('/home')

if __name__ == "__main__":
    serve(app, host='localhost', port = 8000)
    #app.run('localhost', port = 8000,  debug=True)
