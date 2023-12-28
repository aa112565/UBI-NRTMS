import os
import sys
import socket
import datetime
import time
import sqlalchemy
import requests
from sqlalchemy import create_engine
from jproperties import Properties
import cx_Oracle
import warnings
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from datetime import timedelta
from kafka import KafkaConsumer
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
configs = Properties()
with open('application.properties','rb') as config_file:
    configs.load(config_file)
logger = logging.getLogger("NRTMS92C")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('./logs/NRTMS92C.log', maxBytes=50*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
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
smsgroup= configs.get('smsgroupname').data
smsurl=configs.get('smsurl').data
fromuser = configs.get('from').data
subject = configs.get('subject').data 
emailuser = configs.get('emailuser').data
emailpwd = configs.get('emailpwd').data
smtpserver = configs.get('smtpserver').data
smtpport = configs.get('smtpport').data
deptname = configs.get('deptname').data
bankname = configs.get('bankname').data
city = configs.get('city').data
ewsurl = configs.get('ewsurl').data
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_6')
engine = create_engine(oracle_connection_string.format(username=configs.get("user").data,password=configs.get("pwd").data,hostname=configs.get("host").data,port=configs.get("port").data,service_name=configs.get("sid").data))

def nrtms_check():
    try:
        user,pwd,host,port,sid=configs.get("user").data,configs.get("pwd").data,configs.get("host").data,configs.get("port").data,configs.get("sid").data
        conn = cx_Oracle.connect(user=user, password=pwd,dsn=host+":"+port+"/"+sid,encoding="UTF-8")
        if conn:
            return True
    except cx_Oracle.DatabaseError as error:
        return False 
        
def kafka_check():
    try:
        consumer = KafkaConsumer(bootstrap_servers=configs.get('kafka_bootstrap_server').data)
        if consumer:
            return True
    except kafka.errors.NoBrokersAvailable as error:
        print('Kafka Error:',error)
        return False  
        
        
def calculate_time(start,stop):
    difference = stop-start
    return str(difference)

def first_check():
    if nrtms_check():
        live="\nNRTMS DB server connection acquired \n"
        print(live)
        connection_acquired_time=datetime.now()
        acquiring_message=" NRTMS DB server connection acquired at: "+\
        str(connection_acquired_time).split(".")[0]
        print(acquiring_message)
        logger.info(live)
        logger.info(acquiring_message)
        return True
    else:
        not_live="\nNRTMS DB server connection not acquired \n"
        print(not_live)
        logger.info(not_live)
        return False
        
def get_branchname(sol_id):
    with engine.connect() as conn:
        rslt = conn.execute('select br_code,v_branch_name,region_code,region_name,zone_code,zone_name from UFD_EXTRACTION_PRD.mst_branch_uni where sol_id = :1',[sol_id]).fetchone()
        return dict(rslt)

def get_alert_threshold(alertid):
    with engine.connect() as conn:
        rslt = conn.execute('select n_amount_fixed from NRTMS_ALERTLIB_PRD.cnfg_alert_threshold where n_param_id = :p1 and n_rule_id = 1', [alertid]).fetchone()
        if rslt:
            return rslt[0]
        else:
            return 0.0

def get_alert_info(alertid):
    with engine.connect() as conn:
        rslt = conn.execute('select v_desc,v_display_area,v_risk_mitigation,v_significance,v_severity,v_source,v_type,n_days_to_close from NRTMS_ALERTLIB_PRD.cnfg_alert_params where n_param_id = :p1', [alertid]).fetchone()
        return dict(rslt)     

def logic(rec,amt_thres):
    if float(rec['bal_on_activ_date']) >= amt_thres:
        return True
    else:
        return False

def to_wf(rec,amt_thres):
    cust_info = rec['cif_id']
    branchname = get_branchname(rec['sol_id'])
    alertinfo = get_alert_info(AlRefId)  
    close_by=datetime.strptime(rec['xason'],'%Y-%m-%d %H:%M:%S')
    close_by_date=close_by+timedelta(days=alertinfo['n_days_to_close'])
    
    #print('Cust Info',cust_info)

    
    msg =   'It is observed that the '+rec['foracid']+' of '+rec['acct_name']+' having account Balance of Rs '+rec['clr_bal_amt']+' is activated on '+rec['txn_dtm']+' .'
    'Branch is advised to ensure compliance of KYC guidelines as per IC 2610/2021 dt 17.05.2021, IC 2540/2021 dt 30.03.2021 and IC 8380 dt 25.07.2009, before allowing accounts to be made operational and submit reply on below mentioned points:'
    '1. whether KYC profile updated by obtaining fresh proof of identity, proof of address &recent photograph of the customer.'
    '2. Special care taken before modification/deletion of the registered mobile no./email_id'
    #msg = 'On '+rec['activation_date']+', Activation of Dormant Account Number: '+rec['foracid']+' with  balance on active date Rs.'+rec['bal_on_activ_date']+' have taken place beyond the threshold of Rs.'+amt_thres    
    with engine.connect() as conn:
        alertrefid = conn.execute('select mst_wf_alert_ref_id.nextval from dual').fetchone()[0]
        alertNo ="NRTMS-"+str(alertrefid)
        rundate = datetime.now()
        add_info = 'ACCOUNT NO~' + rec['foracid'] + '^ACCOUNT NAME~' + rec['acct_name'] + '^SCHEME CODE~' + rec['schm_code'] + '^SCHEME TYPE~' + rec['schm_type'] + '^A/C STATUS~' + rec['acct_status'] + '^ACTIVATION DATE~' + rec['txn_dtm'] + '^BALANCE~' + rec['clr_bal_amt']
        reclst = [alertNo,alertinfo['v_type'], ewiid, alertinfo['v_desc'], alertinfo['v_severity'], alertinfo['v_source'], freq, rec['cif_id'], rec['acct_name'],rec['sol_id'], branchname['v_branch_name'], msg,datetime.strptime(rec['xason'],'%Y-%m-%d %H:%M:%S'),alertinfo['v_display_area'], alertinfo['v_significance'], alertinfo['v_risk_mitigation'], alertnum,rundate,close_by_date,branchname['br_code'],branchname['region_code'],branchname['region_name'],branchname['zone_code'],branchname['zone_name'],rec['foracid'],datetime.strptime(rec['txn_dtm'],'%Y-%m-%d %H:%M:%S'),add_info]    
        conn.execute('insert into mst_wf_cust_alert_dtl_uni(V_ALERT_ID,V_TYPE,N_EWI_ID,V_EWI_DESC,V_SEVERITY,V_SOURCE,V_FREQUENCY,V_CUSTOMER_ID,V_CUSTOMER_NAME,V_BRANCH_ID,V_BRANCH_NAME,V_MESSAGE,D_DATE,V_PAGE,V_SIGNIFICANCE,V_RISK_MITIGATION,V_ALERT_NUM,D_RUN_DATE,D_CLOSEBY,V_BRANCH_6DGTCD,V_REGION_ID,V_REGION_NAME,V_ZONE_ID,V_ZONE_NAME,V_ACCOUNTS,D_TRANSACTION_DT,V_ADDITIONAL_INFO) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27)', reclst)
        sendmail(alertnum,ewiid,msg,rec['sol_id'],branchname['v_branch_name'],rec['acct_name'],alertinfo['v_desc'],alertrefid,rundate,alertNo)
        send_SMS(alertnum,ewiid,msg,rec['sol_id'],branchname['v_branch_name'],alertNo,rundate,alertinfo['v_desc'])
    print('To WF:', rec['cif_id'], rec['foracid'])
    logging.info('To WF: '+rec['cif_id']+'   '+rec['foracid'])
    #subroutines.send2mailtopic(msg,alertnum,ewiid,rec['sol_id'],branchname,cust_info['cust_name'],alertinfo['v_desc'],alertrefid,datetime.strftime(rundate,'%Y-%m-%d %H:%M:%S'))
    
#def to_wf_rej(rec,amt_thres):
#    cust_info = mst_cust_info(rec['cif_id'])  
#    branchname = get_branchname(rec['sol_id'])
#    alertinfo = get_alert_info(AlRefId)
#    print('Cust Info',cust_info)    
#    msg = 'On '+rec['txn_dtm']+', Cash Withdrawal transaction in Account Number: '+rec['foracid']+' of Rs.'+rec['tran_amt']+' with transaction ID:'+rec['tran_id']+' have taken place beyond the threshold of Rs.'+amt_thres
#    reclst = [alertinfo['v_type'], ewiid, alertinfo['v_desc'], alertinfo['v_severity'], alertinfo['v_source'], freq, rec['cif_id'], cust_info['cust_name'], cust_info['cust_exposure'], cust_info['cust_asset_classification'], cust_info['vertical_code'], rec['sol_id'], branchname, msg, datetime.strptime(rec['txn_dtm'],'%Y-%m-%d %H:%M:%S'), vstage, alertinfo['v_display_area'], alertinfo['v_significance'], alertinfo['v_risk_mitigation'], cust_info['cust_risk_factor'], alertnum, closurestage, datetime.now(), datetime.strptime(rec['xason'],'%Y-%m-%d %H:%M:%S')]    
#    with engine.connect() as conn:
#        conn.execute('insert into wf_rt_rjts(V_TYPE,N_EWI_ID,V_EWI_DESC,V_SEVERITY,V_SOURCE,V_FREQUENCY,V_CUSTOMER_ID,V_CUSTOMER_NAME,N_CUST_EXPOSURE,V_CUST_CLASSIFICATION,V_CUST_VERTICAL,V_BRANCH_ID,V_BRANCH_NAME,V_MESSAGE,D_DATE,V_STAGE,V_PAGE,V_SIGNIFICANCE,V_RISK_MITIGATION,V_CUST_RISK,V_ALERT_NUM,V_CLOSURE_STAGE,D_RUN_DATE,D_CLOSEBY) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24)', reclst)
#    print('Rejected: ', rec['cif_id'], rec['foracid'])    
#    logging.info('To WF Rejects: '+rec['cif_id']+'   '+rec['foracid'])
def send_SMS(alertnum,ewiid,alertmsg,solid,branchname,alertNo,rundate,alertdesc):
    try:
        with engine.connect() as conn:
            rslt = conn.execute("SELECT V_PHONE FROM ADMIN_PRD.cnfg_users where V_USER_ID in (SELECT v_user_id FROM ADMIN_PRD.user_groups where V_GROUP_ID in (SELECT V_GROUP_ID FROM ADMIN_PRD.cnfg_user_group where upper(v_group_name)=upper('"+smsgroup+"'))) and V_BRANCH_CODE=:p1",[solid]).fetchall()
            #print('rslt',rslt)
            logging.info('rslt:',rslt)
            wfdate=rundate.strftime("%d.%m.%Y %H:%M:%S")
            sms_content="NRTMS alert id "+alertNo+" dated "+wfdate+" generated for "+alertdesc+" for "+branchname+" Br. "+solid+". Please attend on priority. -UNION BANK OF INDIA."
            if rslt:
                for phone in range(len(rslt)):
                
                    #print('phone',rslt[phone][0])
                    logging.info('phone:',rslt[phone][0])
                    sms_service=smsurl
                    payload = {
                                "telephoneno":"91"+rslt[phone][0],
                                "message":sms_content
                              }
                    #print(payload)
                    logging.info('payload:',payload)
                    response = requests.post(url=sms_service,json=payload)
                    #print(f"Response: {response.status_code}")
                    logging.info('response:',response.status_code)
                    if response.status_code == 200:
                        inslst = ['SMS Sent',datetime.now(),rslt[phone][0],solid,ewiid,alertnum,sms_content,alertNo]
                        with engine.connect() as conn:
                            conn.execute('insert into sms_audit_log(v_status,d_when,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id) values(:1,:2,:3,:4,:5,:6,:7,:8)',inslst)
                    else:
                        inslst = ['SMS not sent',datetime.now(),rslt[phone][0],solid,ewiid,alertnum,str(response.status_code),alertNo]
                        with engine.connect() as conn:
                            conn.execute('insert into sms_audit_log(v_status,d_when,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id) values(:1,:2,:3,:4,:5,:6,:7,:8)',inslst)
    except Exception as e:
          #print('Error: ',str(e))
          logging.info('----Mail Error: ',str(e))
          inslst = ['SMS Error',datetime.now(),rslt[phone][0],solid,ewiid,alertnum,str(e),alertNo]
          with engine.connect() as conn:
              conn.execute('insert into sms_audit_log(v_status,d_when,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id) values(:1,:2,:3,:4,:5,:6,:7,:8)',inslst)

def sendmail(alertnum,ewiid,alertmsg,solid,branchname,custname,alertdesc,alertrefid,rundate,alertNo):
    try:
        #touser=fromuser
        touser=''
        branchuser='Dear Sir/Madam,'
        tocc=''
        wfdate=rundate.strftime("%d.%m.%Y %H:%M:%S")
        with engine.connect() as conn:
            rslt = conn.execute('select v_location_email,v_region_email,v_zone_email from UFD_EXTRACTION_PRD.mst_branch_uni where sol_id = :1',[solid]).fetchone()
            print('rslt',rslt)
            if rslt:
                touser=rslt[0]  
                tocc=f'{rslt[1]};{rslt[2]}'
                #print('Branch:',solid,'UserEmail:',rslt[0],'tocc',tocc)
                logging.info('Branch:'+solid+'   UserEmail:'+rslt[0])
            else:
                print('Email Id not found for Branch',solid)
                logging.info('Email Id not found for Branch:'+solid)
               
        mailmsg = MIMEMultipart('alternative')
        mailmsg['Subject'] = subject+str(alertNo)
        mailmsg['From'] = fromuser
        mailmsg['To'] = touser
        mailmsg['Cc'] = tocc
        htmlmsg = """
        <html><body>
        <h4><b>{branchuser}</b></h4>
        <p>NRTMS alert id {alertNo} dated {wfdate} - generated for {alertdesc} for {branchname}, {solid}. Please attend on priority. </p>
        <p>Alert Message:</p>
        <p><h4>{alertmsg}</h4></p></br>
        <p><h4><b>You are requested to attend the alert promptly through the following url:</br>
           <a href={ewsurl}>{ewsurl}</a></b></h4></p></br>
        <p><h4><b>In case of any irregularity,appropriate corrective action should be taken immediately.</b></h4></p>
        <p><h4><b>Kindly ignore the message if the alert is already attended & closed.</b></br></p>
        <P><h4><b>Thanks & Regards,</br>
            Unified Solution Team,</br>
            {bankname}</br>
        </b></h4></p></br></br>
        <h6><b>Note: This is a system generated mail so please do not reply to this mail.</b></h6>       
        </body></html>
           """.format(branchuser=branchuser,custname=custname,branchname=branchname,solid=solid,alertmsg=alertmsg,ewsurl=ewsurl,deptname=deptname,city=city,bankname=bankname,alertdesc=alertdesc,alertnum=alertnum,rundate=rundate,alertNo=alertNo,wfdate=wfdate)
        part = MIMEText(htmlmsg, 'html')
        mailmsg.attach(part)
        logging.info(mailmsg)
        try:
          smtpObj = SMTP(smtpserver,smtpport)
          #smtpObj.login(emailuser,emailpwd)
          ret = smtpObj.sendmail(fromuser, [touser,tocc], mailmsg.as_string())
          smtpObj.quit()
          print('------>>> sendmail returned:', ret)
          print('==>Mail sent', touser)
          logging.info('----Mail sent----' + touser)
          inslst = ['Sent',datetime.now(),fromuser,touser,solid,ewiid,alertnum,alertmsg,alertNo,tocc]
          with engine.connect() as conn:
              conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id,v_ccuser) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)',inslst)
        except Exception as e:
          smtpObj.quit()
          print('Error: ',str(e))
          logging.info('----Mail Error: '+str(e))
          inslst = ['MailError',datetime.now(),fromuser,touser,solid,ewiid,alertnum,str(e),alertNo,tocc]
          with engine.connect() as conn:
              conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id,v_ccuser) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)',inslst)
    except Exception as e:
        print('Error: ',str(e))
        logging.info('----Mail Error: '+str(e))
        inslst = ['MailError',datetime.now(),fromuser,touser,solid,ewiid,alertnum,str(e),alertNo,tocc]
        with engine.connect() as conn:
            conn.execute('insert into mail_audit_log(v_status,d_when,v_fromuser,v_touser,v_branch,n_ewi_id,v_alertnum,v_msg,v_alert_id,v_ccuser) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)',inslst)


def main():
            monitor_start_time=datetime.now()
            monitoring_date_time="NRTMS monitoring started at: "+\
                str(monitor_start_time).split(".")[0]
            if first_check():
            # IF TRUE
                print(monitoring_date_time)
                #monitoring will start when the connection is acquired
            else:
                # if false
                while True:
                    # infinite loop to see if the connection is acquired
                    if not nrtms_check():
                        #if connection not acquired
                        #print("infinite loop connection not acquired")
                        time.sleep(1)
                    else:
                        #if connection acquired
                        first_check()
                        print(monitoring_date_time)
                        break
                        
           
                logger.info("\n")
                logger.info(monitor_start_time + "\n")
                #infinite loop to monitor network connection till the machine runs
            while True:
                    if nrtms_check():
                    #if true the loop will execute every 5 seconds
                        #time.sleep(5)
                        if kafka_check():
                        #if true the loop will execute every 5 seconds
                            #time.sleep(5)
                            print('Consumer started....')
                            consumer = KafkaConsumer(topic,bootstrap_servers=[configs.get('kafka_bootstrap_server').data],  api_version=(2,8,0), auto_offset_reset=configs.get('topic_offset_reset').data, group_id=configs.get('groupid').data)
                            for msg in consumer:
                                logging.info('------------------------------------')   
                                rec = json.loads(msg[6])
                                print(rec)
                                amt_thres = get_alert_threshold(AlRefId)
                                if logic(rec,amt_thres):
                                    to_wf(rec,str(amt_thres))
                        else:
                        #if false fail message will be displayed
                            down_time=datetime.now()
                            fail_message="kafka server disconnected at: "+\
                                str(down_time).split(".")[0]
                            print(fail_message)
                            logger.info(fail_message + "\n")
                            while not kafka_check():
                                time.sleep(1)
                            up_time=datetime.now()
                            uptime_message="kafka server Connected again at: "+\
                            str(up_time).split(".")[0]
                            down_time= calculate_time(down_time,up_time)
                            unavailability_time="kafka server connection was unavailable for: "+down_time
                            print(uptime_message)
                            print(unavailability_time)
                            logger.info(uptime_message + "\n")
                            logger.info(unavailability_time + "\n")
                    else:
                    #if false fail message will be displayed
                        down_time=datetime.now()
                        fail_message="NRTMS DB disconnected at: "+\
                            str(down_time).split(".")[0]
                        print(fail_message)
                        logger.info(fail_message + "\n")
                        while not nrtms_check():
                        #infinit loop, will run  till nrtms_check() return true
                            #print("infinit loop, will run  till nrtms_check() return true")
                            time.sleep(1)
                        up_time=datetime.now()
                        uptime_message="NRTMS DB Connected again at: "+\
                            str(up_time).split(".")[0]
                        down_time= calculate_time(down_time,up_time)
                        unavailability_time="NRTMS DB connection was unavailable for: "+down_time
                        print(uptime_message)
                        print(unavailability_time)
                        logger.info(uptime_message + "\n")
                        logger.info(unavailability_time + "\n")
main()                        
            

