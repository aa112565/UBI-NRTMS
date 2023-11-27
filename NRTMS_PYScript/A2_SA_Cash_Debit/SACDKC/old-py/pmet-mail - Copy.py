#closeby date null,
import json
#import pandas as pd
from datetime import datetime
import cx_Oracle
import warnings
from sqlalchemy import create_engine 
from kafka import KafkaConsumer
#import subroutines
import logging
from jproperties import Properties
configs = Properties()
with open('application.properties','rb') as config_file:
    configs.load(config_file)

warnings.filterwarnings('ignore')
user = configs.get('user').data    
pwd = configs.get('pwd').data    
host = configs.get('host').data    
port = configs.get('port').data    
sid = configs.get('sid').data    
topic = configs.get('topic').data    
AlRefId = int(configs.get('alertlibrefewiid').data) 
freq = configs.get('freq').data    
alertnum = configs.get('alertnum').data    
closurestage = configs.get('closurestage').data    
vstage = configs.get('vstage').data    
ewiid = int(configs.get('ewiid').data)
consumer = KafkaConsumer(topic,bootstrap_servers=[configs.get('kafka_bootstrap_server').data], auto_offset_reset=configs.get('topic_offset_reset').data, group_id=configs.get('groupid').data)
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
#cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_9')
engine = create_engine(oracle_connection_string.format(username=configs.get("user").data,password=configs.get("pwd").data,hostname=configs.get("host").data,port=configs.get("port").data,service_name=configs.get("sid").data))
logging.basicConfig(filename='app.log',filemode='a',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')    

def mst_cust_info(cif):
    with engine.connect() as conn:
        rslt = conn.execute('select cust_name,cust_exposure,cust_classification,vertical_code,cust_risk_factor from stg_custmaster where cust_id = :1',[cif]).fetchone()
        if not rslt:
            print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        return dict(rslt)
        

def get_branchname(sol_id):
    with engine.connect() as conn:
        rslt = conn.execute('select v_branch_name from mst_branch where v_branch_code = :1',[sol_id]).fetchone()
        if not rslt:
            return 'Branch-not in MST'
        return rslt[0]

def get_alert_threshold(alertid):
    with engine.connect() as conn:
        rslt = conn.execute('select n_amount_fixed from cnfg_alert_threshold where n_param_id = :p1 and n_rule_id = 1', [alertid]).fetchone()
        if rslt:
            return rslt[0]
        else:
            return 0.0

def get_alert_info(alertid):
    with engine.connect() as conn:
        rslt = conn.execute('select v_desc,v_display_area,v_risk_mitigation,v_significance,v_severity,v_source,v_type from cnfg_alert_params where n_param_id = :p1', [alertid]).fetchone()
        return dict(rslt)     

def logic(rec,amt_thres):
    if float(rec['tran_amt']) >= amt_thres:
        return True
    else:
        return False
    
def to_wf(rec,amt_thres):
    cust_info = mst_cust_info(rec['cif_id'])
    branchname = get_branchname(rec['sol_id'])
    alertinfo = get_alert_info(AlRefId)    
    msg = 'On '+rec['txn_dtm']+', Cash Withdrawal transaction in Account Number: '+rec['foracid']+' of Rs.'+rec['tran_amt']+' with transaction ID:'+rec['tran_id']+' have taken place beyond the threshold of Rs.'+amt_thres    
    with engine.connect() as conn:
        alertrefid = conn.execute('select mst_wf_alert_ref_id.nextval from dual').fetchone()[0]
        rundate = datetime.now()
        reclst = [alertrefid,alertinfo['v_type'], ewiid, alertinfo['v_desc'], alertinfo['v_severity'], alertinfo['v_source'], freq, rec['cif_id'], cust_info['cust_name'], cust_info['cust_exposure'], cust_info['cust_classification'], cust_info['vertical_code'], rec['sol_id'], branchname, msg, datetime.strptime(rec['txn_dtm'],'%Y-%m-%d %H:%M:%S'), vstage, alertinfo['v_display_area'], alertinfo['v_significance'], alertinfo['v_risk_mitigation'], cust_info['cust_risk_factor'], alertnum, closurestage, rundate]    
        conn.execute('insert into mst_wf_cust_alert_dtl(V_ALERT_ID,V_TYPE,N_EWI_ID,V_EWI_DESC,V_SEVERITY,V_SOURCE,V_FREQUENCY,V_CUSTOMER_ID,V_CUSTOMER_NAME,N_CUST_EXPOSURE,V_CUST_CLASSIFICATION,V_CUST_VERTICAL,V_BRANCH_ID,V_BRANCH_NAME,V_MESSAGE,D_DATE,V_STAGE,V_PAGE,V_SIGNIFICANCE,V_RISK_MITIGATION,V_CUST_RISK,V_ALERT_NUM,V_CLOSURE_STAGE,D_RUN_DATE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24)', reclst)
    print('To WF:', rec['cif_id'], rec['foracid'])
    logging.info('To WF: '+rec['cif_id']+'   '+rec['foracid'])
    #subroutines.send2mailtopic(msg,alertnum,ewiid,rec['sol_id'],branchname,cust_info['cust_name'],alertinfo['v_desc'],alertrefid,datetime.strftime(rundate,'%Y-%m-%d %H:%M:%S'))
    
def to_wf_rej(rec,amt_thres):
    cust_info = mst_cust_info(rec['cif_id'])  
    branchname = get_branchname(rec['sol_id'])
    alertinfo = get_alert_info(AlRefId) 
    msg = 'On '+rec['txn_dtm']+', Cash Withdrawal transaction in Account Number: '+rec['foracid']+' of Rs.'+rec['tran_amt']+' with transaction ID:'+rec['tran_id']+' have taken place beyond the threshold of Rs.'+amt_thres
    reclst = [alertinfo['v_type'], ewiid, alertinfo['v_desc'], alertinfo['v_severity'], alertinfo['v_source'], freq, rec['cif_id'], cust_info['cust_name'], cust_info['cust_exposure'], cust_info['cust_classification'], cust_info['vertical_code'], rec['sol_id'], branchname, msg, datetime.strptime(rec['txn_dtm'],'%Y-%m-%d %H:%M:%S'), vstage, alertinfo['v_display_area'], alertinfo['v_significance'], alertinfo['v_risk_mitigation'], cust_info['cust_risk_factor'], alertnum, closurestage, datetime.now()]    
    with engine.connect() as conn:
        conn.execute('insert into wf_rt_rjts(V_TYPE,N_EWI_ID,V_EWI_DESC,V_SEVERITY,V_SOURCE,V_FREQUENCY,V_CUSTOMER_ID,V_CUSTOMER_NAME,N_CUST_EXPOSURE,V_CUST_CLASSIFICATION,V_CUST_VERTICAL,V_BRANCH_ID,V_BRANCH_NAME,V_MESSAGE,D_DATE,V_STAGE,V_PAGE,V_SIGNIFICANCE,V_RISK_MITIGATION,V_CUST_RISK,V_ALERT_NUM,V_CLOSURE_STAGE,D_RUN_DATE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23)', reclst)
    print('Rejected: ', rec['cif_id'], rec['foracid'])    
    logging.info('To WF Rejects: '+rec['cif_id']+'   '+rec['foracid'])

print('Consumer started....')
for msg in consumer:
    logging.info('------------------------------------')   
    rec = json.loads(msg[6])
    print(rec)
    amt_thres = get_alert_threshold(AlRefId)
    if logic(rec,amt_thres):
        to_wf(rec,str(amt_thres))
    else:
        to_wf_rej(rec,str(amt_thres))


#consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=False)