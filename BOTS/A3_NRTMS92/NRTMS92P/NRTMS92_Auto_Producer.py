#rotloggin
#cust_id subquery, restart unlike 1k custs will happen only on th start_str which will be the exception time, the time the bot was interrupted, make sure the time lapse is not extensive to avoid bot getting stuck in time
#21aug, added extime to msg for consumer to save in the mstwf table 
import json
from datetime import datetime
from datetime import timedelta
import time
import kafka
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
logger = logging.getLogger("NRTMS97P")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('./logs/NRTMS92P.log', maxBytes=50*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
AlRefId = int(configs.get('alertlibrefewiid').data)
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_6')
#producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=40960)
producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data, max_request_size=1048576)
odgDB = create_engine(oracle_connection_string.format(username=configs.get('odg_luser').data,password=configs.get('odg_lpwd').data,hostname=configs.get('odg_lhost').data,port=configs.get('odg_lport').data,service_name=configs.get('odg_lsid').data))
#############
#NRTMS_PRD
nrtmsDB = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))

topic = configs.get('topic').data

#custidsason = configs.get('custidsason').data  #taking all 2k custs

################### Retrieving the start_str from DB #######################
sysTime=datetime.now()
currentTime=sysTime.strftime('%d-%m-%Y %H:%M:%S')
date=datetime.now()

def ping():
    try:
        user,pwd,host,port,sid=configs.get("odg_luser").data,configs.get("odg_lpwd").data,configs.get("odg_lhost").data,configs.get("odg_lport").data,configs.get("odg_lsid").data
        conn = cx_Oracle.connect(user=user, password=pwd,dsn=host+":"+port+"/"+sid,encoding="UTF-8")
        if conn:
            return True
    except cx_Oracle.DatabaseError as error:
        return False 

def kafka_check():
    try:
        producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data)
        if producer:
            return True
    except kafka.errors.NoBrokersAvailable as error:
        print('Kafka Error:',error)
        return False

def calculate_time(start,stop):
    difference = stop-start
    return str(difference)

def first_check():
    if ping():
        live="\nODG DB server connection acquired \n"
        print(live)
        connection_acquired_time=datetime.now()
        acquiring_message=" ODG DB server connection acquired at: "+\
        str(connection_acquired_time).split(".")[0]
        print(acquiring_message)
        logger.info(live)
        logger.info(acquiring_message)
        return True
    else:
        not_live="\nODG DB server connection not acquired \n"
        print(not_live)
        logger.info(not_live)
        return False

def get_alert_threshold(AlRefId):
    with nrtmsDB.connect() as nrtmsDBCon:
        with nrtmsDBCon.begin():
            rslt = nrtmsDBCon.execute('select n_amount_fixed from NRTMS_ALERTLIB_PRD.cnfg_alert_threshold where n_param_id = :p1 and n_rule_id = 1', [AlRefId]).fetchone()
            if rslt:
                return rslt[0]
            else:
                return 0.0

alertThreshold = get_alert_threshold(AlRefId)   
next_sno = 0
rnumber = 0
LOG_serial = 0
insertedrows = 0
def audit_log():
    
    
    with nrtmsDB.connect() as nrtmsDBCon:
        with nrtmsDBCon.begin():
            
            alert = nrtmsDBCon.execute("select * from NRTMS_ALERTLIB_PRD.cnfg_alert_params where n_param_id=:1 ",[AlRefId]).fetchone()
            rname = alert[3]
            rnumber = alert[10].split("-")[1]

            next_sno = nrtmsDBCon.execute("SELECT NRTMS_LOG_SNO_SEQUENCE.NEXTVAL FROM dual").fetchone()[0]
            
            data = [{'SNO':next_sno,'Rule_No': rnumber,'Rule_Name':rname, 'EXCEPTION_GENERATED_DATE': date,'DATA_EXTRACTION_START_TIME':sysTime,'LOG_STATUS':'R','NO_OF_RECORDS':0}]          
            sql = "INSERT INTO NRTMS_ALERT_LOGS(SNO,Rule_No, Rule_Name, EXCEPTION_GENERATED_DATE,DATA_EXTRACTION_START_TIME,LOG_STATUS,NO_OF_RECORDS) VALUES (:SNO,:Rule_No, :Rule_Name, :EXCEPTION_GENERATED_DATE,:DATA_EXTRACTION_START_TIME,:LOG_STATUS,:NO_OF_RECORDS)"
            nrtmsDBCon.execute(sql,data)
            

            try:
               with nrtmsDB.connect() as nrtmsDBCon:
                    with nrtmsDBCon.begin():
                        row =  nrtmsDBCon.execute("SELECT MAX(TO_NUMBER(LOG_serial_no)) FROM NRTMS_ALERT_LOGS WHERE EXCEPTION_GENERATED_DATE =:1 AND LOG_STATUS = 'E' AND rule_no=:2",[date,rnumber]).fetchone()
                        if row:
                            LOG_serial = row[0] + 1
            except Exception as e:
                LOG_serial = 1
################### END Of Retrieving the start_str from DB ################

  
def data_extraction_process(start_str):
    print("start_str before",start_str)
    #start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    #start_str = datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S')
    #print("start_str after ",start_str)
    #start_date=datetime.strftime(datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S'),'%d-%m-%Y')
    print('start_date ',start_str)
    qry = ''' SELECT /*+parallel(8)*/ gam.sol_id,
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
        to_char((SELECT eab.tran_date_bal
         FROM tbaadm.eab
         WHERE eab.acid = gam.acid
         AND   eab.eod_date <= TO_DATE(trunc(SYSDATE)-1)
         AND   eab.end_eod_date >= TO_DATE(trunc(SYSDATE)-1))) bal_on_activ_date
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
        AND   acct_opn_date < TO_DATE(SYSDATE,'dd-mm-yy')
        AND   smt.lchg_time> TO_DATE(:dateTimefilter,'dd-mm-yyyy HH24:MI:SS')
        AND   (SELECT eab.tran_date_bal
               FROM tbaadm.eab
               WHERE eab.acid = gam.acid
               AND   eab.eod_date <= TO_DATE(trunc(SYSDATE)-1)
               AND   eab.end_eod_date >= TO_DATE(trunc(SYSDATE)-1)) >=:alertThreshold
        AND   EXISTS (SELECT 1
                      FROM tbaadm.adt
                      WHERE TABLE_NAME = 'SMT'
                      AND   adt.acid = gam.acid
                      AND   AUDIT_BOD_DATE =TO_DATE(SYSDATE,'dd-mm-yy')
                      AND   MODIFIED_FIELDS_DATA LIKE '%acct_status|D|A%') order by TO_CHAR(smt.lchg_time,'DD-MM-YYYY HH24:MI:SS') '''

    qry = sqlalchemy.text(qry) 
    maxtime = datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S') ####just for comparison

    #logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + configs.get('start_str').data)
    logger.info('SUBQUERYBOT@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S') + ' FOR Time Since: ' + str(start_str))

    try:   
        while(True):
            #logger.info('--------------------------------sq----')        
            #print('---------------------------------sq----')
            if kafka_check():
                with odgDB.connect() as odgDBCon:    
                    with odgDBCon.begin():
                        query_records=0
                        query_start_time=datetime.now()
                        #print("start_str passing to query",start_str)
                        rslttemp = odgDBCon.execute(qry,{'dateTimefilter':start_str,'alertThreshold':alertThreshold}).fetchall()
                        query_end_time=datetime.now()
                        total_time=query_end_time-query_start_time
                        query_records=len(rslttemp)
                        #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                        #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                with nrtmsDB.connect() as nrtmsDBCon:    
                    with nrtmsDBCon.begin():
                        if rslttemp:
                            global insertedrows
                            insertedrows=insertedrows+len(rslttemp)
                            print('total Records---------------------sq----',insertedrows)
                            logger.info('--------------------------------sq----')        
                            #print(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                            print('Extracted Count',len(rslttemp))
                            audit_sno = nrtmsDBCon.execute("SELECT NRTMS_92_AUDIT_SNO_SEQUENCE.NEXTVAL FROM dual").fetchone()[0]
                            data = [{'SNO':audit_sno,'DATA_EXTRACTION_START_TIME':query_start_time,'DATA_EXTRACTION_END_TIME':query_end_time,'total_TIME_taken':total_time,'NO_OF_RECORDS':query_records,'LOG_STATUS':'S'}]          
                            sql = "INSERT INTO NRTMS_92_AUDIT_LOGS(SNO,DATA_EXTRACTION_START_TIME,DATA_EXTRACTION_END_TIME, total_TIME_taken,NO_OF_RECORDS,LOG_STATUS) VALUES (:SNO,:DATA_EXTRACTION_START_TIME, :DATA_EXTRACTION_END_TIME, :total_TIME_taken,:NO_OF_RECORDS,:LOG_STATUS)"
                            nrtmsDBCon.execute(sql,data)
                            #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                           #logger.info(start_str+' Count Rsltemp: '+str(len(rslttemp)))
                            for r in rslttemp:  
                                logger.info(r)
                                xason = datetime.now()
                                nrtmsDBCon.execute('insert into NRTMS92(SOL_ID,BR_CODE,SOL_DESC,FORACID,CIF_ID,ACCT_NAME,SCHM_CODE,SCHM_TYPE,CLR_BAL_AMT,ACCT_STATUS,TXN_DTM,BAL_ON_ACTIV_DATE,ASON) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],xason])
                                ##
                                rec = dict(r) 
                                last_tran_time=rec['txn_dtm'].strftime('%d-%m-%Y %H:%M:%S')
                                #rec['tran_date'] = rec['tran_date'].strftime('%Y-%m-%d %H:%M:%S')
                                rec['txn_dtm'] = rec['txn_dtm'].strftime('%Y-%m-%d %H:%M:%S')
                                rec['xason'] = xason.strftime('%Y-%m-%d %H:%M:%S')
                               
                                print(rec)
                                alrec = json.dumps(rec).encode('utf-8')            
                                producer.send(topic, value=alrec)
                                producer.flush()
                                statement = "UPDATE NRTMS_ALERT_LOGS SET No_Of_Records=:1 WHERE SNO = :2"
                                nrtmsDBCon.execute(statement, [insertedrows,next_sno])
                                if datetime.strptime(last_tran_time,'%d-%m-%Y %H:%M:%S')>datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S'):
                                    start_str = last_tran_time            
                            print('start_str ',start_str)
                            return start_str
                               
    except Exception as e:
        with nrtmsDB.connect() as nrtmsDBCon:    
            with nrtmsDBCon.begin():
                
                RUNNING_STATUS  = 'E'
                insertedrows=0
                END_TIME = datetime.now()
                LOG_MESSAGE =str(e)
                statement = "UPDATE NRTMS_ALERT_LOGS SET Data_Extraction_End_Time=:1, No_Of_Records=:2,LOG_STATUS=:3,LOG_MESSAGE=:4,LOG_SERIAL_NO=:5 WHERE SNO = :6"
                nrtmsDBCon.execute(statement, [END_TIME,insertedrows,RUNNING_STATUS,LOG_MESSAGE,LOG_serial,next_sno])
        print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'d-%m-%Y %H:%M:%S'))  
        print(str(e))      
        logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'d-%m-%Y %H:%M:%S'))
        logger.info(str(e))
        return start_str    
            
def main():
            start_str = configs.get('start_str').data
            monitor_start_time=datetime.now()
            monitoring_date_time="odg monitoring started at: "+\
                str(monitor_start_time).split(".")[0]
            if first_check():
            # IF TRUE
                audit_log()
                print(monitoring_date_time)
                logger.info(monitoring_date_time)
                #monitoring will start when the connection is acquired
            else:
                # if false
                while True:
                    # infinite loop to see if the connection is acquired
                    if not ping():
                        #if connection not acquired
                        #print("infinite loop connection not acquired")
                        logger.info("infinite loop connection not acquired")
                        time.sleep(1)
                    else:
                        #if connection acquired
                        first_check()
                        print(monitoring_date_time)
                        logger.info(monitoring_date_time)
                        break
                        
           
                logger.info("\n")
                logger.info(monitor_start_time + "\n")
                #infinite loop to monitor network connection till the machine runs
            while True:
                    if ping():
                    #if true the loop will execute every 5 seconds
                        #time.sleep(5)
                        last_trans_dt=data_extraction_process(start_str)
                        if last_trans_dt:
                            start_str=last_trans_dt
                        else:
                            start_str=start_str
                        print("start_str in while ",start_str)
                       
                        
                    else:
                    #if false fail message will be displayed
                        down_time=datetime.now()
                        fail_message="ODG DB disconnected at: "+\
                            str(down_time).split(".")[0]
                        print(fail_message)
                        logger.info(fail_message + "\n")
                        while not ping():
                        #infinit loop, will run  till ping() return true
                            #print("infinit loop, will run  till ping() return true")
                            time.sleep(1)
                        up_time=datetime.now()
                        uptime_message="ODG DB Connected again at: "+\
                            str(up_time).split(".")[0]
                        down_time= calculate_time(down_time,up_time)
                        unavailability_time="ODG DB connection was unavailable for: "+down_time
                        print(uptime_message)
                        print(unavailability_time)
                        logger.info(uptime_message + "\n")
                        logger.info(unavailability_time + "\n")
main() 


            
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')