import sys
from pyspark.sql import SparkSession
from pyspark import sql
import json
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd
from email.mime.text import MIMEText
from subprocess import Popen, PIPE
import socket

#conf_json = sys.argv[1]
appName = "NDO Validation"
master = "yarn"
driver = 'com.teradata.jdbc.TeraDriver'
url = "jdbc:teradata://<>"
user = ""
password = ""

df_final=pd.DataFrame()

def setup_pyspark(path, conf):
    spark = SparkSession.builder \
    .config("spark.jars","/usr/lib/tdch/1.7/lib/terajdbc4.jar")\
    .config("spark.jars","/usr/lib/tdch/1.7/lib/tdgssconfig.jar" )\
    .appName(appName) \
    .getOrCreate()
    with open(conf) as configFile:
     conf = json.load(configFile)
    return conf,spark
    
def save_to_excel(df,path,excel_name):
    report=path+"/"+excel_name
    writer = pd.ExcelWriter(report, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    
def run_query(query,spark):
    return spark.sql(query)

def parse_json(conf,spark):
    conf_json=conf
    for i in range(0,len(conf_json['Configuration'])):
        print("\n")
        if conf_json['Configuration'][i]['type']=="hive_common_max_check":
            hive_common_max_check(conf_json['Configuration'][i],spark)
        if conf_json['Configuration'][i]['type']=="hive_cdl_refresh_check":
            hive_cdl_refresh_check(conf_json['Configuration'][i],spark)
        if conf_json['Configuration'][i]['type']=="td_count_check":
            td_count_check(conf_json['Configuration'][i],spark)
        if conf_json['Configuration'][i]['type']=="ndo_ehoppa_td_check":
            td_count_check(conf_json['Configuration'][i],spark)


def validate_snap_nbr(df,pgm_name,spark):
    print("inside Validation validate_snap_nbr")
    current_date=datetime.now()
    lastQuarter = (current_date.month - 1) / 3
    quarterBeginMonth = str(int(float(3 * lastQuarter - 2)))
    quarterMnthFormatted=datetime.strptime(quarterBeginMonth,'%m').strftime('%m')
    year=str(datetime.now().year)
    if quarterBeginMonth=='1' :
       year=str(datetime.now().year-1)
       quarterMnthFormatted=datetime.strptime("12",'%m').strftime('%m')
    snap_yr_mnth_nbr_derived=long(year+quarterMnthFormatted)
    print(snap_yr_mnth_nbr_derived)
    df_new=df.withColumn('flag',F.when(F.col('snap_nbr')==F.lit(snap_yr_mnth_nbr_derived),1).otherwise(0))
    df_new.printSchema()
    df_new.show()
    global df_final
    df_final=df_final.append(df_new)

        
def validate_etg_run_id(df,pgm_name,spark):
    print("inside Validation validate_etg_run_id")
    oldEtgRunIdQuery="select etg_run_id from ts_pdppndoph_nogbd_r000_ou."+pgm_name+" limit 1"
    oldEtgRunId=run_query(oldEtgRunIdQuery,spark)
    oldEtgRunIdInt=int(oldEtgRunId.select('etg_run_id').collect()[0].asDict()['etg_run_id']) 
    print(oldEtgRunIdInt)
    df_new=df.withColumn('flag',F.when(F.col('etg_run_id')==F.lit(oldEtgRunIdInt),1).otherwise(0))
    df_new.show()
    global df_final
    df_final=df_final.append(df_new)


def hive_common_max_check(data,spark):
     tables_dict=data['tables']
     program_name=data['program_name']
     final_query="select table_name,"+data['column']+ " from ("
     for table_name,column_name in tables_dict.items():
         query=" select max("+column_name+") as "+ data['column']+",'"+table_name+"' as table_name from "+ table_name
         if (tables_dict.keys().index(table_name)+1     < len(tables_dict.keys())):
             final_query=final_query+query
             final_query=final_query+" union all "
         else:
                final_query=final_query+query+" )d"
     print("Final query: " + final_query)
     query_result_df=pd.DataFrame
     query_result_df=run_query(final_query,spark)
     query_result_df.show()
     print("Inside common max check")
     print(query_result_df)
     query_result_df=query_result_df.insert(0,'Program_Name',program_name)
     if (data['validaion_function']!=""):
         globals()[data['validaion_function']](query_result_df,program_name,spark)

def validate_hive_cdl_refresh_check(df):
    print("Inside validate_hive_cdl_refresh_check")


def hive_cdl_refresh_check(data,spark):
    tables_list=data['tables']
    dbName=data['arguments'][0]
    df=pd.DataFrame()
    with open(data['query'][0], 'r') as sql_file:
        sql_query=sql_file.read()
    for table_name in tables_list:
        sql_query_1=sql_query+table_name.upper()
        sql_query_2="SELECT from_unixtime(unix_timestamp((REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(cdh_sor_dtm,' ',':'),'/',':'),'-',':')),'yyyy:MM:dd:HH:mm:ss'),'yyyyMMdd') cdh_sor_dtm from "+dbName+"."+table_name.replace("'","")+" limit 1"
        print(sql_query_1)
        print(sql_query_2)
        query_result_df=run_query(sql_query_1,spark)
        query_result_df_2=run_query(sql_query_2,spark)
        refresh_date_cdl=str(query_result_df_2.select('cdh_sor_dtm').collect()[0].asDict()['cdh_sor_dtm'])
        print("refresh_date_cdl "+refresh_date_cdl)
        current_date=datetime.now()
        current_quarter = (current_date.month - 1) / 3 + 1
        print("current_quarter "+str(int(current_quarter)))
        quarterBeginMonth = str(int(float(3 * current_quarter - 2)))
        print("quarterBeginMonth "+str(quarterBeginMonth))
        quarterMnthFormatted=datetime.strptime(quarterBeginMonth,'%m').strftime('%m')
        print("quarterMnthFormatted "+str(quarterMnthFormatted))
        year=str(datetime.now().year)
        print("year "+year)
        refresh_quarter=str(year+str(quarterMnthFormatted))
        currentMnthFormatted=datetime.strptime(str(current_date.month),'%m').strftime('%m')
        refresh_month=str(year+str(currentMnthFormatted))
        print("refresh_month "+refresh_month)
        #query_result_df=query_result_df.withColumn('flag',F.when((F.col('refresh_frequency').isin('MNTH#1') & (F.lit(refresh_date_cdl)==F.lit(refresh_month))),1).otherwise(0))
        query_result_df=query_result_df.withColumn('flag',F.when((F.col('refresh_frequency').isin('MNTH#1','MNTH#1#12','MNTH#16','MNTH#1#FRI','MNTH#6','MNTH#10','MNTH#1#SUN','MNTH#15','MNTH#5','MNTH#3','MNTH#12#FRI','MNTH#1#15','MNTH#1#17','MNTH#12','MNTH#13','MNTH#12#SAT','MNTH#18','MNTH#8','MNTH#1#14','MNTH#9','MNTH#2','MNTH#1') & (F.lit(refresh_date_cdl)==F.lit(refresh_month))),1).when((F.col('refresh_frequency').isin('QUARTERLY#16','QUARTERLY#20','QUARTERLY#15','QTRLY#20') & (F.lit(refresh_date_cdl)==F.lit(refresh_month))),1).when((F.col('refresh_frequency').isin('MON','SAT#2','DAILY','SUN-THU','MON_WED','SUN','SUN_THU','SAT','SUN-FRI','MON-FRI','SUN_FRI','MON_FRI','MON_THU','EVERYDAY','TUE','LAST SUN','FRI') & (F.lit(refresh_date_cdl)==F.lit(refresh_month))),1).otherwise(0))
        query_result_df.show()
        df=df.append(query_result_df.toPandas())
    print(df)
    if (data['validaion_function']!=""):
        globals()[data['validaion_function']](df)

def run_td_query(sql,spark):
    return spark.read \
        .format('jdbc') \
        .option('driver', driver) \
        .option('url', url) \
        .option('dbtable', '({sql}) as src'.format(sql=sql)) \
        .option('user', "ag21866") \
        .option('password',"Anthem@12") \
        .load()

def validate_common(df):
    global df_final
    df_final=df_final.append(df)
    print(df_final)

def ndo_ehoppa_td_check(data,spark):
    tables_list=data['tables']
    program_name=data['program_name']
    current_date=datetime.now()
    currentQuarter = (current_date.month - 1) / 3 +1
    if currentQuarter=='1.0' :
       year=str(datetime.now().year-2)
    else: 
       year=str(datetime.now().year-1)
    yearStrtDt=int(year+'01')
    yearEndDt=int(year+'12')
    for table_name in tables_list:
        query="select count(*) as knt from "+table_name+" where Inc_Month between "+yearStrtDt+" and "+yearEndDt
        print("td_query "+query)
        df=run_td_query(query,spark).toPandas()
        df.insert(0,'Program_Name',program_name)
        df.insert(1,'Table_Name',table_name)
        df2=df2.append(df)
        print(df2)
    if (data['validaion_function']!=""):
        globals()[data['validaion_function']](df2)
    
def td_count_check(data,spark):
    tables_list=data['tables']
    program_name=data['program_name']
    df2=pd.DataFrame()
    for table_name in tables_list:
        query="select count(*) as knt from "+table_name
        print("td_query "+query)
        df=run_td_query(query,spark).toPandas()
        print(df)
        df.insert(0,'Program_Name',program_name)
        df.insert(1,'Table_Name',table_name)
        df2=df2.append(df)
        print(df2)
    if (data['validaion_function']!=""):
        globals()[data['validaion_function']](df2)

def frame_email(df_final,path):
    print("Inside the frame email method")
    fail_list={}
    flag=1
    program_list=""
    for index,row in df_final.iterrows():
        if row['Status']==0:
            fail_list.add(row['Program_Name'])
            flag=0
    for val in fail_list:
        program_list=program_list+", "+val
    if(flag==0): # Some Program Failed
        message="Hi All, \nNDO Pre-Validation completed. Please find the programs below with failed status. Kindly check the attached excel for more information.\n"+program_list
    else:
        message="Hi All, \nNDO Pre-Validation completed successfully."
    return message
        
def send_mail(message):   
    recipients = ['shubham.chawla@legatohealth.com','Aparna.Vittala@legatohealth.com']
    host = socket.gethostname()
    msg = MIMEText(message,"html")
    msg['Subject'] = "Server Name:{0} - Data validation Status for {1} Table - {2}".format(host,'tablename','status')
    msg['From'] = "NDO Validation"
    msg["To"] = ",".join(recipients)
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_string())
    print(msg.as_string())
    
            
def main():
    # Setup logger
    # Setup Spark
    path=sys.argv[1]
    conf=sys.argv[2]
    conf, spark = setup_pyspark(path,conf)
    print("Setup Done")
    #spark.addFile("conf.json")
    #read_sample_table(spark)
    parse_json(conf,spark)
    global df_final
    save_to_excel(df_final,path,"NDO_Validation")
    message=frame_email(df_final,path)
    send_mail(message)
    #sc.stop()

if __name__ == '__main__':
    main()
