#rotloggin
#cust_id subquery, restart unlike 1k custs will happen only on th start_str which will be the exception time, the time the bot was interrupted, make sure the time lapse is not extensive to avoid bot getting stuck in time
#21aug, added extime to msg for consumer to save in the mstwf table 
import json
from datetime import datetime
import cx_Oracle
import warnings
import sqlalchemy
from sqlalchemy import create_engine 
from jproperties import Properties
from kafka import KafkaProducer
import logging
from logging.handlers import RotatingFileHandler
warnings.filterwarnings('ignore')
configs = Properties()
with open('application.properties','rb') as config_file:
    configs.load(config_file)
logger = logging.getLogger("CTETL")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('./logs/CTKP.log', maxBytes=50*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
AlRefId = int(configs.get('alertlibrefewiid').data) 
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_6')
#producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=40960)
producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=1048576)
#engine1 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))
engine1 = create_engine(oracle_connection_string.format(username=configs.get('odg_luser').data,password=configs.get('odg_lpwd').data,hostname=configs.get('odg_lhost').data,port=configs.get('odg_lport').data,service_name=configs.get('odg_lsid').data))

#NRTMS_UAT
engine2 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))

#############



################### Retrieving the start_str from DB #######################

sysTime=datetime.now()
currentTime=sysTime.strftime('%d-%m-%Y %H:%M:%S')
currentDate=sysTime.strftime('%d-%m-%Y')
date_query = '''select to_char(a.ACTIVATION_DATE,'dd-mm-yyyy hh24:mi:ss') tran_date from stg_dormant_account a order by a.ACTIVATION_DATE desc fetch first 1 rows only'''


with engine2.connect() as adminconn_2:    
    with adminconn_2.begin():
        start_str = adminconn_2.execute(date_query)
        print('Start dateTime:',list(start_str))
        if len(list(start_str))>0:
            start_str = list(start_str)
            start_str = start_str[0][0]
            start_date=currentDate
        else:
            start_str=currentTime
            start_date=datetime.strftime(datetime.now(),'%d-%m-%Y')
        print('Start dateTime:',start_str)
        print('Start Date:',start_date)

def get_alert_threshold(AlRefId):
    with engine2.connect() as adminconn_2:
        with adminconn_2.begin():
            rslt = adminconn_2.execute('select n_amount_fixed from cnfg_alert_threshold where n_param_id = :p1 and n_rule_id = 1', [AlRefId]).fetchone()
            if rslt:
                return rslt[0]
            else:
                return 0.0

alertThreshold = get_alert_threshold(AlRefId)       
        
################### END Of Retrieving the 
with engine1.connect() as adminconn_1:    
    with adminconn_1.begin():
        
################### END Of Retrieving the start_str from DB ################
        topic = configs.get('topic').data  
     #   start_str = configs.get('start_str').data 
     #   start_str = datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S')
     #   print('start_date ',start_str)
#custidsason = configs.get('custidsason').data  #taking all 2k custs

        qry = '''SELECT distinct /*+parallel(64)*/ gam.sol_id,
                        sol.BR_CODE,
                        sol.SOL_DESC,
                        gam.foracid,
                        gam.cif_id,
                        gam.acct_name,
                        gam.schm_code,
                        gam.schm_type,
                       to_char(gam.clr_bal_amt) clr_bal_amt,
                        smt.acct_status,
                        smt.lchg_time txn_dtm,
                       to_char( (SELECT eab.tran_date_bal
                         FROM tbaadm.eab
                         WHERE eab.acid = gam.acid
                         AND   eab.eod_date <= TO_DATE(smt.lchg_time)
                         AND   eab.end_eod_date >= TO_DATE(smt.lchg_time))) bal_on_activ_date
                        FROM tbaadm.gam,
                        tbaadm.smt,
                        tbaadm.sol
                        WHERE gam.acid = smt.acid
                        AND   acct_ownership <> 'O'
                        AND   gam.del_flg <> 'Y'
                        AND   gam.acct_cls_flg = 'N'
                        AND   gam.sol_id = sol.sol_id
                        AND   gam.entity_cre_flg = 'Y'
                        AND   gam.bank_id = '01'
                        AND   smt.bank_id = '01'
                        AND   smt.acct_status = 'A'
                        AND   acct_opn_date < TO_DATE(:dateFilter,'dd-mm-yyyy')
                        AND   smt.lchg_time > TO_DATE(:dateTimefilter,'dd-mm-yyyy HH24:MI:SS')
                        AND   (SELECT eab.tran_date_bal
                               FROM tbaadm.eab
                               WHERE eab.acid = gam.acid
                               AND   eab.eod_date <= TO_DATE(smt.lchg_time)
                               AND   eab.end_eod_date >= TO_DATE(smt.lchg_time))>=:alertThreshold
                        AND   EXISTS (SELECT 1
                                      FROM tbaadm.adt
                                      WHERE TABLE_NAME = 'SMT'
                                      AND   adt.acid = gam.acid
                                      AND   AUDIT_BOD_DATE = TO_DATE(:dateFilter,'dd-mm-yyyy')
                                      AND   MODIFIED_FIELDS_DATA LIKE '%acct_status|D|A%')'''
 
qry = sqlalchemy.text(qry) 
maxtime = datetime.strptime('2022-01-01','%Y-%m-%d') ####just for comparison

#logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + configs.get('start_str').data)
logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%d-%m-%Y %H:%M:%S') + ' FOR Time Since: ' + str(start_str))     

try:   
    while(True):
        #logger.info('--------------------------------sq----')        
        #print('---------------------------------sq----') 
        with engine1.connect() as adminconn:    
            with adminconn.begin():
                print('Query start_str',start_str,'-->')
                rslttemp = adminconn.execute(qry,{'dateFilter':start_date,'dateTimefilter':start_str,'alertThreshold':alertThreshold,'dateFilter':start_date}).fetchall()
                #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
        with engine2.connect() as adminconn1:    
            with adminconn1.begin():
                if rslttemp:
                    print('---------------------------------sq----')
                    logger.info('--------------------------------sq----')        
                    #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                    print(start_str,'-->',len(rslttemp))
                    #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                   #logger.info(start_str+' Count Rsltemp: '+str(len(rslttemp)))
                    for r in rslttemp:  
                        logger.info(r)
                        xason = datetime.now()
                        adminconn1.execute('insert into stg_dormant_account(SOL_ID,BRANCH_CODE,v_branch_name,V_ACCOUNT_NO,V_CUSTOMER_ID,V_CUSTOMER_NAME,SCHM_CODE,SCHM_TYPE,CLR_BAL_AMT,V_ACCOUNT_STATUS,ACTIVATION_DATE,BAL_ON_ACTIV_DATE,ASON) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],xason])
                        ##
                        rec = dict(r)
                        last_tran_time=rec['txn_dtm'].strftime('%d-%m-%Y %H:%M:%S')                        
                       #rec['tran_date'] = rec['tran_date'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['txn_dtm'] = rec['txn_dtm'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['xason'] = xason.strftime('%Y-%m-%d %H:%M:%S')
                        print('transaction Time ',rec['txn_dtm'])
                        print(rec)
                        alrec = json.dumps(rec).encode('utf-8')            
                        producer.send(topic, value=alrec)
                        producer.flush()
                        if datetime.strptime(last_tran_time,'%d-%m-%Y %H:%M:%S')>datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S'):
                            start_str = last_tran_time            
                            print('start_str ',start_str)
                       
except Exception as e:
    print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'d-%m-%Y %H:%M:%S'))  
    print(str(e))      
    logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'d-%m-%Y %H:%M:%S'))
    logger.info(str(e))
    
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')