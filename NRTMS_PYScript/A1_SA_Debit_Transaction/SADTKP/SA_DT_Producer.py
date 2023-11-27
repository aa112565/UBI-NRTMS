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
date_query = '''select to_char(a.txn_dtm,'dd-mm-yyyy hh24:mi:ss') txn_dtm from stg_debit_transaction_in_suspense_acc a order by a.txn_dtm desc fetch first 1 rows only'''

with engine2.connect() as adminconn_2:    
    with adminconn_2.begin():
        start_str = adminconn_2.execute(date_query)
        if start_str:
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

        qry = '''WITH acct_select AS
                (
                    SELECT gam.sol_id,
                    gam.acid,
                    gam.cif_id,
                    gam.foracid,
                    gam.acct_name,
                    dtd.tran_id,
                    dtd.tran_date,
                    gam.bacid,
                    SUBSTR(dtd.tran_particular,1,30) tran_particular,
                    to_char(dtd.tran_amt) tran_amt,
                    dtd.PART_TRAN_TYPE,
                    to_char(gam.clr_bal_amt) clr_bal_amt,
                    NVL(vfd_date,dtd.pstd_date) txn_dtm
                    FROM TBAADM.gam,
                    TBAADM.dtd
                    WHERE dtd.sol_id = gam.sol_id
                    AND   gam.acid = dtd.acid
                    AND   gam.gl_sub_head_code IN ('82100','82200','82300','82400','82500','82600')
                    AND   dtd.tran_id NOT LIKE 'S%'
                    AND   gam.bacid NOT IN ('82500050','82500060','82500070','82500080','82500130')
                    AND   dtd.pstd_date>TO_DATE(:dateFilter,'dd-mm-yyyy HH24:MI:SS')
                    AND   dtd.PART_TRAN_TYPE = 'D'
                    AND   tran_amt*GetConversionRate (gam.acct_crncy_code,'INR','COR',dtd.tran_date)>=:alertThreshold
                    ORDER BY dtd.tran_date
                    ),
                c_tran_detail AS
                (
                    SELECT a.sol_id sol_id,
                    (SELECT BR_CODE FROM TBAADM.SOL WHERE SOL_ID = a.sol_id) Branch_code,
                    a.cif_id,
                    a.acid acid,
                    a.foracid dr_foracid,
                    a.acct_name dr_acct_name,
                    a.tran_id tran_id,
                    a.tran_date tran_date,
                    a.bacid bacid,
                    SUBSTR(a.tran_particular,1,30) dr_tran_particular,
                    to_char(a.tran_amt) dr_tran_amt,
                    a.PART_TRAN_TYPE dr_tran_type,
                    to_char(a.clr_bal_amt) clr_bal_amt,
                    (SELECT foracid FROM TBAADM.gam WHERE acid = dtd.ACID) cr_foracid,
                    (SELECT acct_name FROM TBAADM.gam WHERE acid = dtd.ACID) cr_acct_name,
                    (SELECT bacid FROM TBAADM.gam WHERE acid = dtd.ACID) cr_bacid,
                    to_char(dtd.tran_amt) cr_tran_amt,
                    dtd.part_tran_type,
                    a.txn_dtm
                    FROM TBAADM.dtd dtd,
                    acct_select a 
                    WHERE dtd.tran_id = a.tran_id 
                    AND   dtd.tran_date >= a.tran_date 
                    AND   dtd.tran_date <= a.tran_date 
                    AND   dtd.part_tran_type = 'C' 
                    AND   dtd.del_flg = 'N'
                    AND   dtd.part_tran_srl_num = (SELECT MIN(to_number (h.part_tran_srl_num))
                                                   FROM tbaadm.dtd h
                                                   WHERE h.tran_id = dtd.tran_id
                                                   AND   h.tran_date = dtd.tran_date
                                                   AND   h.part_tran_type = dtd.part_tran_type
                                                   AND   h.del_flg = 'N')
                    AND   substr (a.foracid,6) NOT IN ('8250857000','8250091000','8240003000','8240002000','8240001000','8250014000','8250073000','8250009000','8250010000')
                    )
                SELECT DISTINCT *
                FROM c_tran_detail 
                WHERE nvl (CR_BACID,'X') NOT LIKE '%511%'
                AND   nvl (CR_BACID,'X') NOT LIKE '%824%'  '''
 
qry = sqlalchemy.text(qry) 
maxtime = datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S') ####just for comparison

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
                        adminconn1.execute('insert into stg_debit_transaction_in_suspense_acc(SOL,SOL_ID,CIF_ID,ACID,DR_FORACID,DR_ACCT_NAME,TRAN_ID,TRAN_DATE,BACID,DR_TRAN_PARTICULAR,DR_TRAN_AMT,DR_TRAN_TYPE,CLR_BAL_AMT,CR_FORACID,CR_ACCT_NAME,CR_BACID,CR_TRAN_AMT,PART_TRAN_TYPE,TXN_DTM,ASON) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17,:p18,:p19,:p20)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],r[14],r[15],r[16],r[17],r[18],xason])
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