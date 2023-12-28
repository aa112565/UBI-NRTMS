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
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
warnings.filterwarnings('ignore')
configs = Properties()
with open('application.properties','rb') as config_file:
    configs.load(config_file)
logger = logging.getLogger("NRTMS2P")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('./logs/NRTMS2P.log', maxBytes=50*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
AlRefId = int(configs.get('alertlibrefewiid').data)
fromuser = configs.get('from').data
touser = configs.get('touser').data
tocc = configs.get('tocc').data
smtpserver = configs.get('smtpserver').data
smtpport = configs.get('smtpport').data
bankname = configs.get('bankname').data
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
next_sno = 0
rnumber = 0
LOG_serial = 0
insertedrows = 0
def ping():
    try:
        user,pwd,host,port,sid=configs.get("odg_luser").data,configs.get("odg_lpwd").data,configs.get("odg_lhost").data,configs.get("odg_lport").data,configs.get("odg_lsid").data
        conn = cx_Oracle.connect(user=user, password=pwd,dsn=host+":"+port+"/"+sid,encoding="UTF-8")
        if conn:
            return True
    except cx_Oracle.DatabaseError as error:
        with nrtmsDB.connect() as nrtmsDBCon:    
            with nrtmsDBCon.begin():
                RUNNING_STATUS  = 'E'
                insertedrows=0
                END_TIME = datetime.now()
                LOG_MESSAGE =str(error)
                statement = "UPDATE NRTMS_ALERT_LOGS SET Data_Extraction_End_Time=:1, No_Of_Records=:2,LOG_STATUS=:3,LOG_MESSAGE=:4,LOG_SERIAL_NO=:5 WHERE SNO = :6"
                nrtmsDBCon.execute(statement, [END_TIME,insertedrows,RUNNING_STATUS,LOG_MESSAGE,LOG_serial,next_sno])
                sendmail('Urgent Attention - ODG Server was Down','NRTMS-10002 ODG server down since '+str(END_TIME)+' Error Msg : '+str(error))
        return False 

def kafka_check():
    try:
        producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data)
        if producer:
            return True
    except kafka.errors.NoBrokersAvailable as error:
        with nrtmsDB.connect() as nrtmsDBCon:    
            with nrtmsDBCon.begin():
                RUNNING_STATUS  = 'E'
                insertedrows=0
                END_TIME = datetime.now()
                LOG_MESSAGE =str(error)
                statement = "UPDATE NRTMS_ALERT_LOGS SET Data_Extraction_End_Time=:1, No_Of_Records=:2,LOG_STATUS=:3,LOG_MESSAGE=:4,LOG_SERIAL_NO=:5 WHERE SNO = :6"
                nrtmsDBCon.execute(statement, [END_TIME,insertedrows,RUNNING_STATUS,LOG_MESSAGE,LOG_serial,next_sno])
                sendmail('Urgent Attention - Kafka Server was Down','NRTMS-10002 Kafka Server was down since : '+str(END_TIME)+' Error Msg : '+str(error))
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
                        else:
                            LOG_serial = 1
            except Exception as e:
                LOG_serial = 1
################### END Of Retrieving the start_str from DB ################

  
def data_extraction_process(start_str):
    print("start_str before",start_str)
    #start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    #start_str = datetime.strptime(start_str,'%d-%m-%Y %H:%M:%S')
    #print("start_str after ",start_str)
    qry='''WITH acct_select AS
        (
          SELECT /*+parallel(8)*/ gam.sol_id,
                 gam.cif_id,
                 gam.acid,
                 gam.foracid,
                 gam.acct_name,
                 dtd.tran_id,
                 dtd.tran_date,
                 gam.bacid,
          gam.schm_code,
          gam.schm_type,
          gam.GL_SUB_HEAD_CODE,
                 SUBSTR(dtd.tran_particular,1,30) tran_particular,
                 dtd.tran_amt,
                 dtd.PART_TRAN_TYPE,
                 gam.clr_bal_amt,
                 NVL(dtd.vfd_date,dtd.pstd_date) txn_dtm
          FROM TBAADM.gam,
               TBAADM.dtd
          WHERE dtd.sol_id = gam.sol_id
          AND   gam.acid = dtd.acid
          AND   gam.gl_sub_head_code IN ('82100','82200','82300','82400','82500','82600')
          AND   gam.ACCT_OWNERSHIP='O'
          AND   upper(substr(trim(tran_id),1,1)) not in ('C','S')
          AND   gam.bacid NOT IN ('82508230','82508260','82508250','82504110')
          AND   dtd.pstd_date > TO_DATE(:dateFilter,'dd-mm-yyyy HH24:MI:SS')
          AND   dtd.PART_TRAN_TYPE = 'D'
          AND   dtd.tran_amt*GetConversionRate (gam.acct_crncy_code,'INR','COR',dtd.tran_date) >=:alertThreshold
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
        AND   substr (a.foracid,6) NOT IN ('8250857000','8250091000','8240003000','8240002000','8240001000','8250014000','8250073000')
        )
        SELECT DISTINCT SOL_ID, BRANCH_CODE, CIF_ID, ACID, DR_FORACID, DR_ACCT_NAME, TRAN_ID, TRAN_DATE, BACID, DR_TRAN_PARTICULAR, DR_TRAN_AMT, DR_TRAN_TYPE, CLR_BAL_AMT, CR_FORACID, CR_ACCT_NAME, CR_BACID, CR_TRAN_AMT, PART_TRAN_TYPE,TXN_DTM
        FROM c_tran_detail 
        WHERE nvl (CR_BACID,'X') NOT LIKE '%511%'
        AND   nvl (CR_BACID,'X') NOT LIKE '%824%' order by TO_CHAR(TXN_DTM,'DD-MM-YYYY HH24:MI:SS')'''
    
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
                        rslttemp = odgDBCon.execute(qry,{'dateFilter':start_str,'alertThreshold':alertThreshold}).fetchall()
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
                            audit_sno = nrtmsDBCon.execute("SELECT NRTMS_2_AUDIT_SNO_SEQUENCE.NEXTVAL FROM dual").fetchone()[0]
                            data = [{'SNO':audit_sno,'DATA_EXTRACTION_START_TIME':query_start_time,'DATA_EXTRACTION_END_TIME':query_end_time,'total_TIME_taken':total_time,'NO_OF_RECORDS':query_records,'LOG_STATUS':'S'}]          
                            sql = "INSERT INTO NRTMS_2_AUDIT_LOGS(SNO,DATA_EXTRACTION_START_TIME,DATA_EXTRACTION_END_TIME, total_TIME_taken,NO_OF_RECORDS,LOG_STATUS) VALUES (:SNO,:DATA_EXTRACTION_START_TIME, :DATA_EXTRACTION_END_TIME, :total_TIME_taken,:NO_OF_RECORDS,:LOG_STATUS)"
                            nrtmsDBCon.execute(sql,data)
                            #logger.info(datetime.strftime(start_str,'%Y-%m-%d %H:%M:%S')+'<<'+'>> Count Rsltemp: '+str(len(rslttemp)))
                           #logger.info(start_str+' Count Rsltemp: '+str(len(rslttemp)))
                            for r in rslttemp:  
                                logger.info(r)
                                xason = datetime.now()
                                nrtmsDBCon.execute('insert into NRTMS2(SOL,SOL_ID,CIF_ID,ACID,DR_FORACID,DR_ACCT_NAME,TRAN_ID,TRAN_DATE,BACID,DR_TRAN_PARTICULAR,DR_TRAN_AMT,DR_TRAN_TYPE,CLR_BAL_AMT,CR_FORACID,CR_ACCT_NAME,CR_BACID,CR_TRAN_AMT,PART_TRAN_TYPE,TXN_DTM,ASON) values(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17,:p18,:p19,:p20)', [r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],r[14],r[15],r[16],r[17],r[18],xason])
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
                                statement = "UPDATE NRTMS_ALERT_LOGS SET No_Of_Records=:1 WHERE SNO = :2"
                                nrtmsDBCon.execute(statement, [insertedrows,next_sno])
                                if datetime.strptime(last_tran_time,'%d-%m-%Y %H:%M:%S')>datetime.strptime('01-11-2023 00:00:00','%d-%m-%Y %H:%M:%S'):
                                    start_str = last_tran_time            
                            print('start_str ',start_str)
                            return start_str
            else:
                print('Kafka Server Down')
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

def sendmail(subject,content):
    try:
        #touser=fromuser
        branchuser='Dear Team,'
        mailmsg = MIMEMultipart('alternative')
        mailmsg['Subject'] = subject
        mailmsg['From'] = fromuser
        mailmsg['To'] = touser
        mailmsg['Cc'] = tocc
        
        htmlmsg = """
        <html><body>
        <h4><b>{branchuser}</b></h4>
        <p>{content} Please attend on priority.  </p>
        <P><h4><b>Thanks & Regards,</br>
            Unified Solution Team,</br>
            {bankname}</br>
        </b></h4></p></br></br>
        <h6><b>Note: This is a system generated mail so please do not reply to this mail.</b></h6>       
        </body></html>
           """.format(branchuser=branchuser,bankname=bankname,content=content)
        part = MIMEText(htmlmsg, 'html')
        mailmsg.attach(part)
        logging.info(mailmsg)
        
        try:
          smtpObj = SMTP(smtpserver,smtpport)
          #smtpObj.login(emailuser,emailpwd)
          #ret = smtpObj.sendmail(fromuser, (touser+tocc), mailmsg.as_string())
          ret = smtpObj.sendmail(fromuser, [touser,tocc], mailmsg.as_string())
          smtpObj.quit()
          #print('------>>> sendmail returned:', ret)
          #print('==>Mail sent', touser)
          logging.info('----Mail sent----' + touser)
          inslst = ['Sent',datetime.now(),fromuser,touser,tocc]
          with nrtmsDB.connect() as conn:
              conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_ccuser) values(:1,:2,:3,:4,:5)',inslst)
        except Exception as e:
          smtpObj.quit()
          #print('Error: ',str(e))
          logging.info('----Mail Error: '+str(e))
          inslst = ['MailError',datetime.now(),fromuser,touser,tocc]
          with nrtmsDB.connect() as conn:
              conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_ccuser) values(:1,:2,:3,:4,:5)',inslst)
        
    except Exception as e:
        #print('Error: ',str(e))
        logging.info('----Mail Error: '+str(e))
        inslst = ['MailError',datetime.now(),fromuser,touser,tocc]
        with nrtmsDB.connect() as conn:
            conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_ccuser) values(:1,:2,:3,:4,:5)',inslst)
            
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
                #print('Log serial :',LOG_serial)
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
                        audit_log()
                        print(uptime_message)
                        print(unavailability_time)
                        logger.info(uptime_message + "\n")
                        logger.info(unavailability_time + "\n")
main() 


            
    
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')