import pyodbc
import psycopg2
import pymysql
import sqlalchemy as sc
import sqlalchemy_redshift
import pandas as pd
import configparser
import datetime

# Using config parser to retrieve credentials from config file
initConfig = configparser.ConfigParser()
initConfig.read("AWS_List.config")

#getting current date and time
d = datetime.datetime.today()
filedatetimestamp=str(d.month)+str(d.day)+str(d.year)

# Connect to Prod Redshift and gather data
conn=psycopg2.connect(dbname= 'vcdwh', host=initConfig.get('profile prod', 'prodhost'), 
port= initConfig.get('profile prod', 'port'), user= initConfig.get('profile prod', 'dbuser'), password= initConfig.get('profile prod', 'dbpwd')) 

# getting cursor
cur = conn.cursor()

# Connect to PCM SQL Server and gather data
sqlconn=pyodbc.connect(initConfig.get('profile prod', 'sqlserver'))

# Connecting SQLAlchemy engine for import
sql_alch_engine = sc.create_engine(initConfig.get('profile prod', 'sqlengine'))

# getting cursor
sqlcur=sqlconn.cursor()

# truncating the gaps in care table
with cur:
    cur.execute("truncate table reporting.tbl_pcm_gaps_in_care;")

    
# getting pcm sql view for vision claims SQL Server
with sqlcur:
    SQL=f"""
    select *
    from vw_pcm_gaps_in_care_import

    """

    pcmdf=pd.read_sql_query(SQL,sqlconn)

#print("Appending info to redshift")
pcmdf.to_sql("tbl_pcm_gaps_in_care",sql_alch_engine,schema='reporting',if_exists='append',index=False,chunksize=100)

# running view and making a dataframe for the information
with cur:
    
    SQL=f"""
        select *
        from reporting.vw_pcm_gaps_in_care
    """

    gapdf=pd.read_sql_query(SQL,conn)

# exporting results to shared drive folder
#print("creating file")
newfilename="{1}pcmgapsincare_{0}.csv".format(filedatetimestamp,initConfig.get('profile prod', 'filedir'))
gapdf.to_csv(newfilename,index=False)





