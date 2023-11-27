#max dt for each 1k instead of all
#restart @logged ts
#rotloggin
import re
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
handler = RotatingFileHandler('./logs/CTKP.log', maxBytes=1024*1024, backupCount=100)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
#cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_19_12')
producer = KafkaProducer(bootstrap_servers=configs.get('kafka_bootstrap_server').data)
engine1 = create_engine(oracle_connection_string.format(username=configs.get('luser').data,password=configs.get('lpwd').data,hostname=configs.get('lhost').data,port=configs.get('lport').data,service_name=configs.get('lsid').data))
engine = create_engine(oracle_connection_string.format(username=configs.get('srcuser').data,password=configs.get('srcpwd').data,hostname=configs.get('srchost').data,port=configs.get('srcport').data,service_name=configs.get('srcsid').data))


topic = configs.get('topic').data    
start_str = configs.get('start_str').data   

def get_cust_ids():
    custidsason = configs.get('custidsason').data
    with engine1.connect() as conn:
        rslt = conn.execute('select cust_id from stg_custmaster where ason = :p1', [datetime.strptime(custidsason,'%Y-%m-%d')]).fetchall()
        totlst = [i for i in rslt]
        print('Total CustIds :',len(totlst))
        return totlst

def test_get_cust_ids_temp():
    return ['80117986','79869139','82217795','79268062','83258803','2626866','83925387','77701435','96663941','95052664','88605315','90263157','94186025','71886900','96726941','4067543','81445085','89610232','99735566','89230368','81285451','3163436','77629292','87352323','92110447','76478511','70915622','2829753','71297390','73985013','90824842','3797052','81833474','88467486','87181344','1770037','74291191','70512845','75699351','3692440','87447277','89420430','90753259','404966','83683802','84023066','88665617','87004863','84576019','87348346','73492785','82511684','74371141','4102807','74827344','93980440','84875465','77441415','98843688','4328770','74062516','79498491','84078504','70722458','95971880','92110447','5770064','1623389','85255198','76186322','84925727','79820070','5593027','92111816','82188579','84900249','85704493','2637113','1867102','81198871','93731627','78892392','71357945','70118668','79424701','89152023','7048736','77998811','87166701','88781985','76419598','80543185','79346719','80904293','95498554','6795661','4073873','85054300','94062528','85327815','80321851','99951191','92693459','83325723','80053108','1063963','99500542','81109849','74379130','87004863','85133189','74508695','82916693','6003091','80251074','82395018','73053642','88180151','5204092','88370297','3745606','82476331','99472388','94005621','91366614','5906664','93984446','70675817','90361601','83635099','79536843','76797275','79625321','82018356','6143744','72052291','3143526','84649934','82855749','81905333','85504286','88822477','78229109','2296852','87713680','91983485','89832349','71261151','79329361','87793775','95614559','4793754','83707367','97957087','73123824','3099158','81777415','90895332','87540100','83552931','84507846','81957794','85107173','85704493','85107377','85008166','90999172','95904292','88142705','88411873','91178572','85597447','76389047','90762159','90560751','91965846','86866273','89567601','83933110','97121878','91117753','5907368','85376481','72798848','74714838','90523726','84786610','74512884','5647104','2546652','83819248','98037250','94538501','84788509','83407982','89430667','99956882','1741030','70826127','85067780','2134606','80401198','90122876','74432269','75003201','96705960','81696257','79207430','84700193','94793357','2538876','86843766','90562179','79513826','88937201','82991996','92567420','84217966','92074876','5646111','91981281','83115683','81447491','86037305','82450634','71877549','79364243','85048786','4184347','94435257','89702154','6998162','88115950','6144099','87479128','74777544','97598384','79536180','99470191','90144791','86587776','88371255','78523552','71431247','5141151','81539410','86880611','99477830','97536664','79192604','4221882','83596876','88386322','82834007','96974046','97677351','72673453','89247774','90222693','85798525','75408406','85779023','85327826','77641607','80117986','89316477','84980124','94803596','92682369','99036724','1532755','72694615','74442609','74542345','71953715','86955608','4336981','99800283','83325723','83944949','92298060','85491793','89230368','84785955','90123214','91756932','91212923','84824272','101463977','99339950','72220858','2685194','96705933','89317451','80401835','93459536','3870726','84756566','75156608','91642851','79309023','86898311','74865477','6398636','4244517','100944600','89280508','90635710','81977414','83553765','82188579','3099565','72709396','6928792','86021197','2487849','99281171','85984368','99913506','80019929','6716316','85330885','71580979','84507859','78004948','83242673','87323090','90362590','6419572','78478694','70046611','88904733','90218947','80120697','71461032','77116699','71794237','73777571','5861773','98790503','90938087','3620732','85155255','88109113','3966731','74375383','85678660','84353522','3070058','98335050','85310878','92044035','77410799']    

qry = '''select g.sol_id,g.cif_id,g.foracid,g.schm_type,
        --tbaadm.getSanctLimitAsOnDate(g.acid,to_date(h.tran_date,'dd-mm-yyyy')) sanct_lim,
        tran_date,to_char(tran_amt) tran_amt,tran_id,tran_type,TRAN_RMKS, NVL(vfd_date,h.pstd_date) txn_dtm,
        PART_TRAN_TYPE,TRAN_SUB_TYPE,
        ENTRY_USER_ID,PSTD_USER_ID,VFD_USER_ID
from tbaadm.GAM@rtmsdb_edw g, tbaadm.DTD@rtmsdb_edw h 
where g.cust_id in :custids
    and (g.acct_cls_date is null)
    and h.acid = g.acid
    and h.vfd_date > :datefilter
    and h.tran_type in ('C')--,'L','T')
    and h.part_tran_type in('D')--,'C')
    and h.del_flg='N' and h.pstd_flg='Y'
    '''

qry = sqlalchemy.text(qry)    

totlst = get_cust_ids()
custchunks = [totlst[i:i+1000] for i in range(0,len(totlst), 1000)]
start_str = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
##
dtfltr = {}
restartts = configs.get('restartts').data    
if restartts=='X':
    print('Start')
    logger.info('Start')
    for i in range(0,len(custchunks)):
        dtfltr[i] = start_str   
else:        
    print('RE-Start')
    logger.info('RE-Start')
    dts=configs.get('restartts').data
    dtslst = re.sub(',\s\d:\s','=',dts)[4:][:-1].split("=")
    dt=[]
    for dts in dtslst:
        newstr='dt.append('+dtslst[0][9:]+')'
        exec(newstr)
    for i in range(0,len(dt)):
        dtfltr[i] = dt[i]
print(dtfltr)        
logger.info(dtfltr)
##
logger.info('@@@ST@RTED@@@ : '+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))     
try:   
    while(True):
        logger.info('------------------------------------')        
        print('------------------------------------')        
        rslt = []
        cntr = 0
        for looocusts in custchunks:
            lst = [i for i in looocusts]
            params = {'custids':(lst), 'datefilter':dtfltr[cntr]} #params = {'custids':(lst), 'datefilter':start_str}        
            qry2exe = qry.bindparams(sqlalchemy.bindparam('custids', expanding=True), sqlalchemy.bindparam('datefilter'))
            with engine.connect() as conn:
                rslttemp = conn.execute(qry2exe, params).fetchall()
                logger.info(datetime.strftime(dtfltr[cntr],'%Y-%m-%d %H:%M:%S')+'<<'+str(cntr)+'>> Count Rsltemp: '+str(len(rslttemp)))
                print(cntr,datetime.strftime(dtfltr[cntr],'%Y-%m-%d %H:%M:%S'),'-->',len(rslttemp))
                if rslttemp:
                    with engine1.connect() as adminconn:    
                        with adminconn.begin():
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
                                if r[9]>dtfltr[cntr]:
                                    dtfltr[cntr] = r[9]            
            cntr = cntr+1
            logger.info(dtfltr)
except Exception as e:
    print('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))  
    print(str(e))      
    logger.info('XXXXXXXXxcetion@: ',datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))
    logger.info(str(e))
    
#logging.basicConfig(filename='app.log',filemode='w',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')