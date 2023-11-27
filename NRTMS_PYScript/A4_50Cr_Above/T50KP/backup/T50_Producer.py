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
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_6')
#producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=40960)
producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=1048576)
engine1 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))
#############

topic = configs.get('topic').data  

################### Retrieving the start_str from DB #######################
sysTime=datetime.now()
currentTime=sysTime.strftime('%d-%m-%Y %H:%M:%S')
date_query=None
date_query = '''select to_char(a.txn_dtm,'dd-mm-yyyy hh24:mi:ss') txn_dtm from stg_fifty_above a order by a.txn_dtm desc fetch first 1 rows only'''

################### END Of Retrieving the 
with engine1.connect() as adminconn_1:    
    with adminconn_1.begin():
        start_str = adminconn_1.execute(date_query)
        if start_str:
            start_str = list(start_str)
            start_str = start_str[0][0]
        else:
            start_str=currentTime
        print('Start Time:',start_str)
################### END Of Retrieving the start_str from DB ################

#start_str = configs.get('start_str').data 
#start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
#start_str = datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S')
#custidsason = configs.get('custidsason').data  #taking all 2k custs

qry = '''SELECT /* +parallel(64) */ SUBSTR(division_name,8,30) Zone_Name,
                   SUBSTR(region_name,8,30) Region_Name,
                   sol.sol_id,
                   sol.sol_desc,
                   sol.BR_CODE,
                   decode(substr((sol.abbr_br_name),1,1),'4','METRO','3','URBAN','2','SEMI-URBAN','1','RURAL') solcat,
                   gam.cif_id,
                   gam.foracid,
                   gam.ACCT_NAME,
                  gam.acct_opn_date,
                   dtd.tran_date,
                  to_char(dtd.tran_amt) tran_amt,
                   dtd.tran_id,
                   gam.SCHM_CODE,
                   dtd.tran_type,
                   part_tran_type,
                   TRAN_PARTICULAR,
              cust_type,
             (select REF_DESC from tbaadm.rct@ODG_83 where REF_REC_TYPE='43' and REF_CODE=cust_type and rct.DEL_FLG='N' and rownum<2) cust_type_desc,
             crmuser.accounts.cust_hlth,
             (select REF_DESC from tbaadm.rct@ODG_83 where REF_REC_TYPE='17' and REF_CODE=crmuser.accounts.cust_hlth and rct.DEL_FLG='N' and rownum<2) riskdesc,
             NVL(vfd_date,dtd.pstd_date) txn_dtm
            FROM tbaadm.gam@ODG_83,
               tbaadm.dtd@ODG_83,
              tbaadm.sol@ODG_83,
              crmuser.accounts@ODG_83
            WHERE gam.acid = dtd.acid
            AND   gam.sol_id = sol.sol_id
            AND   gam.cif_id = crmuser.accounts.orgkey
            AND   acct_cls_date IS NULL
            AND   gam.del_flg = 'N'
            AND   gam.schm_code NOT IN ('CARCC','CASOL')
            AND   gam.acct_ownership<> 'O'
            AND   dtd.tran_amt >=500000000
            AND   dtd.del_flg = 'N'
            AND   dtd.pstd_date>=TO_DATE(:dateFilter,'dd-mm-yyyy HH24:MI:SS')'''
 
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
                rslttemp = adminconn.execute(qry,{'dateFilter':start_str}).fetchall()
                #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
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
                        adminconn.execute('insert into stg_fifty_above(ZONE_NAME,REGION_NAME, SOL_ID,SOL_DESC, BR_CODE,SOLCAT,v_customer_id,account_no, ACCT_NAME,ACCT_OPN_DATE,TRAN_DATE,TRAN_AMT,TRAN_ID,SCHM_CODE,TRAN_TYPE,PART_TRAN_TYPE,TRAN_PARTICULAR,CUST_TYPE,CUST_TYPE_DESC,CUST_HLTH,RISKDESC,TXN_DTM,ason) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17,:p18,:p19,:p20,:p21,:p22,:p23)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],r[14],r[15],r[16],r[17],r[18],r[19],r[20],r[21],xason])
                        ##
                        rec = dict(r)  
                        rec['acct_opn_date'] = rec['acct_opn_date'].strftime('%Y-%m-%d %H:%M:%S')                        
                        rec['tran_date'] = rec['tran_date'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['txn_dtm'] = rec['txn_dtm'].strftime('%Y-%m-%d %H:%M:%S')
                        rec['xason'] = xason.strftime('%Y-%m-%d %H:%M:%S')
                        print(rec)
                        alrec = json.dumps(rec).encode('utf-8')            
                        producer.send(topic, value=alrec)
                        producer.flush()
                        if r[21]>maxtime:
                            maxtime = r[21]            
                    start_str = maxtime
                        
                       
except Exception as e:
    print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))  
    print(str(e))      
    logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))
    logger.info(str(e))
    
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')