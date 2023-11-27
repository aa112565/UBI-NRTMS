#om
#rotloggin
#cust_id subquery, restart unlike 1k custs will happen only on th start_str which will be the exception time, the time the bot was interrupted, make sure the time lapse is not extensive to avoid bot getting stuck in time
#WithOUT the extrtime added to consumer msg for saving in mstwf for calculating the time ext and alert time diff
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
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
#cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_12')
producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data)
engine1 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))
#############

topic = configs.get('topic').data    
start_str = configs.get('start_str').data 
start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
#custidsason = configs.get('custidsason').data  #taking all 2k custs

qry = '''select g.sol_id,g.cif_id,g.foracid,g.schm_type,
        --tbaadm.getSanctLimitAsOnDate(g.acid,to_date(h.tran_date,'dd-mm-yyyy')) sanct_lim,
        tran_date,to_char(tran_amt) tran_amt,tran_id,tran_type,TRAN_RMKS, NVL(vfd_date,h.pstd_date) txn_dtm,
        PART_TRAN_TYPE,TRAN_SUB_TYPE,
        ENTRY_USER_ID,PSTD_USER_ID,VFD_USER_ID
from tbaadm.GAM@rtmsdb_edw g, tbaadm.DTD@rtmsdb_edw h 
where g.cust_id in (select cust_id from stg_custmaster)
    and (g.acct_cls_date is null)
    and h.acid = g.acid
    and h.vfd_date > :datefilter
    and h.tran_type in ('C','L','T')
    and h.part_tran_type in('D','C')
    and h.del_flg='N' and h.pstd_flg='Y'
    '''

qry = sqlalchemy.text(qry)    
maxtime = datetime.strptime('2022-01-01','%Y-%m-%d') ####just for comparison

logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + configs.get('start_str').data)     
try:   
    while(True):
        #logger.info('--------------------------------sq----')        
        #print('---------------------------------sq----') 
        with engine1.connect() as adminconn:    
            with adminconn.begin():
                rslttemp = adminconn.execute(qry, {'datefilter':start_str}).fetchall()
                #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                if rslttemp:
                    print('---------------------------------sq----')
                    logger.info('--------------------------------sq----')        
                    print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                    logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                    for r in rslttemp:  
                        logger.info(r)
                        adminconn.execute('insert into stg_cashtran(BRANCH_CODE,CUSTOMER_ID,ACCOUNT_NO,TRANSACTION_DATE,TRANSACTION_AMOUNT,ASON,TRANSACTION_ID,TRAN_TYPE,TRANSACTION_REMARKS,PART_TRAN_TYPE,TRAN_SUB_TYPE,ENTERED_USERID,PSOTED_USERID,VERIFIED_USERID) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14)', [r[0],r[1],r[2],r[9],r[5],datetime.now(),r[6],r[7],r[8],r[10],r[11],r[12],r[13],r[14]])
                        ##
                        rec = dict(r)                                
                        rec['tran_date'] = rec['tran_date'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['txn_dtm'] = rec['txn_dtm'].strftime('%Y-%m-%d %H:%M:%S')
                        print(rec)
                        alrec = json.dumps(rec).encode('utf-8')            
                        producer.send(topic, value=alrec)
                        producer.flush()
                        ##
                        if r[9]>maxtime:
                            maxtime = r[9]            
                    start_str = maxtime
except Exception as e:
    print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))  
    print(str(e))      
    logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))
    logger.info(str(e))
    
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')