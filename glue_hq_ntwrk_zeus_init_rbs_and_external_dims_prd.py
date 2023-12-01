import sys
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
bucket ='s3-hq-anl-prd-ntwrk'
dim1 ='src_stm_dim'
dim2 ='src_obj_dim'
dim3 ='zeus_cntry_dim'
dim4 ='zeus_rbs_dim'
cntry_dim_raw='s3a://s3-hq-raw-prd-ntwrk/zeus_cntry_dim/'

def getFakeDataForRBS_DIM():
    return [
        (datetime.strptime("2023-07-06", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230706"),
        (datetime.strptime("2023-07-07", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230707"),
        (datetime.strptime("2023-07-08", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230708"),
        (datetime.strptime("2023-07-09", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230709"),
        (datetime.strptime("2023-07-10", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230710"),
        (datetime.strptime("2023-07-11", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230711"),
        (datetime.strptime("2023-07-12", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230712"),
        (datetime.strptime("2023-07-13", "%Y-%m-%d").date(),"HN","3.23E+07","AT5111","AT5111",datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2012-07-25", "%Y-%m-%d").date(),datetime.strptime("2023-06-03", "%Y-%m-%d").date(),datetime.strptime("2040-12-31", "%Y-%m-%d").date(),"DECOMISSIONED","ERICSSON","2G","2537","JUTIAPA_PUEBLO_ATLANTIDA","-1","N/A","TBD","To be Determined","TO BE DETERMINED","TO BE DETERMINED","TO BE DETERMINED","NORTH-WEST","ATLANTIDA","JUTIAPA",15.744139,-86.51425,0,"1011853","ENEE","TO BE DETERMINED","To be Determined","TBD","TO BE DETERMINED","TIGO","N/A","20230713"),
    ]
def getDataForSRC_STM_DIM():
    return [
        (-2, datetime.date.today(), "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"N/A" , "Not Applicable"  , "N/A", "Not Applicable"),
        (-1, datetime.date.today(), "TBD", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"TBD" ,"To Be Determined", "N/A", "Not Applicable"),
        (0 , datetime.date.today(), "STD", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"STD" ,"Standard Value"  , "N/A", "Not Applicable"),
        (1 , datetime.date.today(), "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"1"   ,"CDW"             , "N/A", "Not Applicable"),
        (2 , datetime.date.today(), "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"2"   ,"FTP"             , "N/A", "Not Applicable"),
        (3 , datetime.date.today(), "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(),"3"   ,"AWS"             , "N/A", "Not Applicable"),     
    ]
def getDataForSRC_OBJ_DIM():
    return [
        (-2, datetime.date.today(), -2, "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(), "N/A", "Not Applicable"  , "Not Applicable"   , "Not Applicable", 0.0, "N/A", "Not Applicable"),
        (-1, datetime.date.today(), -2, "TBD", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(), "TBD", "To Be Determined", "To Be Determined" , "Not Applicable", 0.0, "N/A", "Not Applicable"),
        (0 , datetime.date.today(), -2, "STD", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(), "STD", "Standard Value  ", "Standard Value"   , "Not Applicable", 0.0, "N/A", "Not Applicable"),
        #(-1, datetime.date.today(),  3, "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(), "N/A", "zeus_rbs_dim"    , "hn-anl-ntwrk-link", "Not Applicable", 0.0, "N/A", "Glue Catalog"),
        #(-1, datetime.date.today(),  3, "N/A", "N/A", datetime.datetime.strptime('1/1/1990', '%m/%d/%Y').date(), datetime.datetime.strptime('12/31/2040', '%m/%d/%Y').date(), "N/A", "cell_rbs_dim"    , "hn-anl-ntwrk-link", "Not Applicable", 0.0, "N/A", "Glue Catalog"),
    ]
def getDataForRBS_DIM_SCND_LVL():
    return [
    ]
def getDataBTCH_DIM():
    return [
    ]
def getRBSDimScndLvlSchema():
    return StructType([
        StructField("RBS_KEY", StringType(), True),
        StructField("CNTRY_KEY", IntegerType(), True),
        StructField("BTCH_KEY", StringType(), True),
        StructField("PPN_DT", DateType(), True),
        StructField("LAST_UDT_DT", DateType(), True),
        StructField("SRC_STM_KEY", StringType(), True),
        StructField("SRC_OBJ_KEY", StringType(), True), #Original StringType()
        StructField("UNQ_ID_SRC_STM", StringType(), True),
        StructField("CNTRL_HASH", StringType(), True),
        StructField("EFF_DT", DateType(), True),
        StructField("END_DT", DateType(), True),
        StructField("ACTV_F", StringType(), True),
        StructField("LAST_FCT_DT_KEY", IntegerType(), True),
        #StructField("LAST_RCRD_F", StringType(), True),
        StructField("RBS_CD", StringType(), True),
        StructField("RBS_ANL_CD", StringType(), True),
        StructField("RBS_NM", StringType(), True),
        StructField("RBS_CNSTR_DT", DateType(), True),
        StructField("RBS_EFF_DT", DateType(), True),
        StructField("RBS_END_DT", DateType(), True),
        StructField("RBS_RETM_DT", DateType(), True),
        StructField("FUNC_ST_NM", StringType(), True),
        StructField("VNDR_NM", StringType(), True),
        StructField("TCHNLGY_CD", StringType(), True),
        StructField("SITE_ID", StringType(), True),
        StructField("SITE_NM", StringType(), True),
        StructField("CNTLR_KEY", IntegerType(), True),
        StructField("CNTLR_CD", StringType(), True),
        StructField("CNTLR_ANL_CD", StringType(), True),
        StructField("CVE_CD", StringType(), True),
        StructField("CVE_NM", StringType(), True),
        StructField("CGY_NM", StringType(), True),
        StructField("STC_TP_NM", StringType(), True),
        StructField("ROLE_NM", StringType(), True),
        StructField("TERR_NM", StringType(), True),
        StructField("DEPT_NM", StringType(), True),
        StructField("MNCP_NM", StringType(), True),
        StructField("LTT", IntegerType(), True),
        StructField("LGT", IntegerType(), True),
        StructField("MAMSL", IntegerType(), True),
        StructField("ELC_AC_CD", StringType(), True),
        StructField("ELC_ENG_SRC_NM", StringType(), True),
        StructField("ELC_VNDR_NM", StringType(), True),
        StructField("OWN_NM", StringType(), True),
        StructField("SHR_F", StringType(), True),
        StructField("OWN_ST_NM", StringType(), True),
        StructField("OPR_NM", StringType(), True),
        StructField("SYMB_PWR_SVG_F", StringType(), True),
        #StructField("DatePartKey", StringType(), True),
    ])
def getBTCH_DIM():
    return StructType([
        StructField("BTCH_KEY", StringType(), False),
        StructField("CNTRY_KEY", StringType(), False),
        StructField("PPN_DT", StringType(), False),
        StructField("LAST_UDT_DT", StringType(), False),
        StructField("SRC_STM_KEY", StringType(), False),
        StructField("SRC_OBJ_KEY", StringType(), False),
        StructField("UNQ_ID_SRC_STM", StringType(), False),
        StructField("EFF_DT", StringType(), False),
        StructField("END_DT", StringType(), False),
        StructField("BTCH_TP_NM", StringType(), False),
        StructField("BTCH_VER_CD", StringType(), False),
        StructField("BTCH_ST_CD", StringType(), False),
        StructField("PRN_BTCH_KEY", StringType(), False),
        StructField("EXEC_CD", StringType(), False),
        StructField("EXEC_ST_CD", StringType(), False),
        StructField("EXEC_CMNT", StringType(), False),
        StructField("EXEC_ERR_CMNT", StringType(), False),
        StructField("EMPE_CD", StringType(), False),
        StructField("NOTF_F", StringType(), False),
        StructField("NBR_RCRD_SRC", StringType(), False),
        StructField("NBR_RCRD_PCS", StringType(), False),
        StructField("NBR_RCRD_ISRT", StringType(), False),
        StructField("NBR_RCRD_UDT", StringType(), False),
        StructField("NBR_RCRD_DEL", StringType(), False),
        StructField("NBR_RCRD_RJCT", StringType(), False),
        StructField("PRD_EFF_DT", StringType(), False),
        StructField("PRD_END_DT", StringType(), False),
        StructField("ORIG_FILE_PATH_NM", StringType(), False),
        StructField("DST_FILE_PATH_NM", StringType(), False),
        StructField("FILE_NM", StringType(), False),
        StructField("ZON_NM", StringType(), False)
    ])
def getSRC_STM_DIM():
    return StructType([
        StructField("SRC_STM_KEY", IntegerType(), False),
        StructField("PPN_DT", DateType(), False),
        StructField("UNQ_ID_SRC_STM", StringType(), False),
        StructField("CNTRL_MD5", StringType(), False),
        StructField("EFF_DT", DateType(), False),
        StructField("END_DT", DateType(), False),
        StructField("SRC_STM_CD", StringType(), False),
        StructField("SRC_STM_NM", StringType(), False),
        StructField("IP_ADR_DD", StringType(), False),
        StructField("SRC_STM_UNIT_NM", StringType(), False)
    ])
def getSRC_OBJ_DIM():
    return StructType([
        StructField("SRC_OBJ_KEY", IntegerType(), False),
        StructField("PPN_DT", DateType(), False),
        StructField("SRC_STM_KEY", IntegerType(), False),
        StructField("UNQ_ID_SRC_STM", StringType(), False),
        StructField("CNTRL_MD5", StringType(), False),
        StructField("EFF_DT", DateType(), False),
        StructField("END_DT", DateType(), False),
        StructField("SRC_OBJ_CD", StringType(), False),
        StructField("SRC_OBJ_NM", StringType(), False),
        StructField("SRC_OBJ_LNG_NM", StringType(), False),
        StructField("SRC_STREAM_NM", StringType(), False),
        StructField("NODE_NO", FloatType(), False),
        StructField("SRC_TP_CD", StringType(), False),
        StructField("SRC_TP_NM", StringType(), False)
    ])
def getCNTRY_DIM():
    return StructType([
        StructField("CNTRY_KEY", IntegerType(), False),
        StructField("BTCH_KEY", IntegerType(), False),
        StructField("PPN_DT",DateType(), False),
        StructField("SRC_STM_KEY", IntegerType(), False),
        StructField("SRC_OBJ_KEY", IntegerType(), False),
        StructField("UNQ_ID_SRC_STM", StringType(), False),
        StructField("CNTRL_MD5", StringType(), False),
        StructField("EFF_DT",DateType(), False),
        StructField("END_DT",DateType(), False),
        StructField("CNTRY_CD", StringType(), False),
        StructField("CNTRY_SHRT_NM", StringType(), False),
        StructField("CNTRY_NM", StringType(), False),
        StructField("MBL_CNTRY_CD", StringType(), False),
        StructField("RGN_CD", StringType(), False),
        StructField("RGN_NM", StringType(), False)
    ])
def writeOnBucket(DF,bkt,dim,MODE):
    DF.write.mode(MODE).parquet("s3a://"+str(bkt)+"/"+str(dim))
    return "saved "+dim
def writeOnBucketByOnePart(DF,bkt,dim,part,MODE):
    DF.write.mode(MODE).partitionBy(part).parquet("s3a://"+str(bkt)+"/"+str(dim))
    return "saved "+dim
def loadParquet(route):
    df = spark.read.options(encoding='UTF-8',characterEncoding="UTF-8").parquet(route)
    return df
def loadCsv(route,schema):
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").schema(schema).load(route)
    return df
def createDf(data,schema):
    return spark.createDataFrame(data, schema)

def run():
    cntry_dim = loadCsv(cntry_dim_raw,getCNTRY_DIM())

    df1 = createDf(getDataForSRC_STM_DIM(), getSRC_STM_DIM())
    df2 = createDf(getDataForSRC_OBJ_DIM(), getSRC_OBJ_DIM())
    #df3 = 
    df4 = createDf(getDataForRBS_DIM_SCND_LVL(), getRBSDimScndLvlSchema())

    writeOnBucket(df1,bucket,dim1,"overwrite")
    writeOnBucket(df2,bucket,dim2,"overwrite")
    writeOnBucket(cntry_dim,bucket,dim3,"overwrite")
    writeOnBucket(df4,bucket,dim4,"overwrite")

run()

job.commit()