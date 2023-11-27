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
engine1 = create_engine(oracle_connection_string.format(username=configs.get('odg_luser').data,password=configs.get('odg_lpwd').data,hostname=configs.get('odg_lhost').data,port=configs.get('odg_lport').data,service_name=configs.get('odg_lsid').data))
#############
#NRTMS_UAT
engine2 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))

topic = configs.get('topic').data  

################### Retrieving the start_str from DB #######################
sysTime=datetime.now()
currentTime=sysTime.strftime('%d-%m-%Y %H:%M:%S')
start_str=None
date_query = '''select to_char(a.txn_dtm,'dd-mm-yyyy hh24:mi:ss') txn_dtm from STG_SA_CASH_DEBIT a order by a.txn_dtm desc fetch first 1 rows only'''

with engine2.connect() as adminconn_2:    
    with adminconn_2.begin():
        start_str = adminconn_2.execute(date_query)
        if start_str is None:
            start_str = list(start_str)
            start_str = start_str[0][0]
        else:
            start_str=currentTime
        print('Start Time:',start_str)
        
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

#start_str = configs.get('start_str').data 
#start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
#start_str = datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S')
#custidsason = configs.get('custidsason').data  #taking all 2k custs

        qry = ''' WITH main_acct AS
            (
            SELECT gam.sol_id,
                    gam.cif_id,
                    foracid,
                    acct_name,
                    tran_type,
                    tran_id,
                    tran_date,
                    to_char(tran_amt) tran_amt,
                    NVL(vfd_date,dtd.pstd_date) txn_dtm
            FROM  tbaadm.gam,
                    tbaadm.dtd dtd
            WHERE gam.acid = dtd.acid
            AND   gam.bacid = '82500540'
            AND   dtd.pstd_date>TO_DATE(:dateFilter,'dd-mm-yyyy HH24:MI:SS')
            AND   part_tran_type = 'D'
            AND   tran_amt >=:alertThreshold
            AND   gam.del_flg = 'N'
            AND   dtd.del_flg = 'N'
            AND   acct_cls_date IS NULL
            )
            SELECT main_acct.sol_id sol_id,
                (SELECT br_code FROM  tbaadm.sol WHERE sol_id = main_acct.sol_id) branch_code,
                 gam.cif_id,
                main_acct.foracid,
                main_acct.acct_name,
                gam.foracid cr_Acct_No,
                gam.acct_name cr_Acct_Name,
                main_acct.tran_type,
                main_acct.tran_id,
                to_char(main_acct.tran_amt) tran_amt,
                main_acct.tran_date,
                main_acct.txn_dtm
            FROM  tbaadm.gam,
                tbaadm.dtd dtd,
                main_acct
            WHERE gam.acid = dtd.acid
            AND   dtd.tran_id = main_acct.tran_id
            AND   dtd.tran_date = main_acct.tran_date
            AND   dtd.part_tran_type = 'C'
            AND   dtd.del_flg = 'N'
            AND   gam.del_flg = 'N' '''
 
qry = sqlalchemy.text(qry) 
maxtime = datetime.strptime('2022-01-01','%Y-%m-%d') ####just for comparison

#logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + configs.get('start_str').data)
logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + str(start_str))     

try:   
    while(True):
        #logger.info('--------------------------------sq----')        
        #print('---------------------------------sq----') 
        with engine1.connect() as adminconn:    
            with adminconn.begin():
                rslttemp = adminconn.execute(qry,{'dateFilter':start_str,'alertThreshold':alertThreshold}).fetchall()
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
                        adminconn1.execute('insert into STG_SA_CASH_DEBIT(SOL_ID,BRANCH_CODE,CIF_ID,FORACID,ACCT_NAME,CR_ACCT_NO,CR_ACCT_NAME,TRAN_TYPE,TRAN_ID,TRAN_AMT,TRAN_DATE,TXN_DTM,ASON) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],xason])
                        ##
                        rec = dict(r)
                        last_tran_time=rec['txn_dtm'].strftime('%d-%m-%Y %H:%M:%S')                        
                        rec['tran_date'] = rec['tran_date'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['txn_dtm'] = rec['txn_dtm'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['xason'] = xason.strftime('%Y-%m-%d %H:%M:%S')
                        print(rec)
                        alrec = json.dumps(rec).encode('utf-8')            
                        producer.send(topic, value=alrec)
                        producer.flush()
                        if datetime.strptime(last_tran_time,'%d-%m-%Y %H:%M:%S')>datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S'):
                            start_str = last_tran_time            
                            print('start_str ',start_str)
                        
                       
except Exception as e:
    print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))  
    print(str(e))      
    logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))
    logger.info(str(e))
    
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')