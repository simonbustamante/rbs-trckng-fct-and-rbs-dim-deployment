import sys, os
import boto3
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

dim_bucket = "s3-hq-anl-prd-ntwrk"
db_athena = "bo-anl-prd-ntwrk-link"
table_athena = "zeus_rbs_dim"
src_obj_dim = "src_obj_dim"
src_stm_dim = "src_stm_dim"
zeus_cntry_dim = "zeus_cntry_dim"
rbs_trckng_fct = "rbs_trckng_fct"

def selectOrderedFields(df):
    return df.select(
        col("FCT_DT_KEY"),
        col("FCT_DT"),
        col("RBS_KEY"),
        col("CNTRY_KEY"),
        col("CNTRY_CD"),
        col("BTCH_KEY"),
        col("PPN_DT"),
        col("SRC_STM_KEY"),
        col("SRC_OBJ_KEY"),
        col("UNQ_ID_SRC_STM"),
        col("CNTRL_HASH"),
        col("RBS_CD"),##
        col("RBS_ANL_CD"),
        col("RBS_NM"),
        col("RBS_CNSTR_DT"),
        col("RBS_EFF_DT"),
        col("RBS_END_DT"),
        col("RBS_RETM_DT"),
        col("FUNC_ST_NM"),
        col("VNDR_NM"),
        col("TCHNLGY_CD"),
        col("SITE_ID"),
        col("SITE_NM"),
        col("CNTLR_KEY"),
        col("CNTLR_NM"),
        col("CNTLR_ANL_CD"),
        col("CVE_CD"),
        col("CVE_NM"),
        col("CGY_NM"),
        col("STC_TP_NM"),
        col("ROLE_NM"),
        col("TERR_NM"),
        col("DEPT_NM"),
        col("MNCP_NM"),
        col("LTT"),
        col("LGT"),  
        col("MAMSL"),
        col("ELC_AC_CD"),
        col("ELC_VNDR_NM"),
        col("ELC_ENG_SRC_NM"),
        col("OWN_NM"),
        col("SHR_F"),
        col("OWN_ST_NM"),
        col("OPR_NM"),
        col("SYMB_PWR_SVG_F"),
        col("DatePartKey")
    )
    
def getRBSDimSchema():
    return StructType([
        StructField("Date", DateType(), True),
        StructField("CountryCode", StringType(), True),
        StructField("AnalyticsRBSKey", StringType(), True),
        StructField("RBSCode", StringType(), True),
        StructField("RBSName", StringType(), True),
        StructField("ConstructionDate", DateType(), True),
        StructField("ServiceEffectiveDate", DateType(), True),
        StructField("ServiceEndDate", DateType(), True),
        StructField("RetirementDate", DateType(), True),
        StructField("FunctionalStatus", StringType(), True),
        StructField("VendorName", StringType(), True),
        StructField("TechnologyCode", StringType(), True),
        StructField("SiteID", StringType(), True),
        StructField("SiteName", StringType(), True),
        StructField("AnalyticsControllerKey", StringType(), True),
        StructField("ControllerName", StringType(), True),
        StructField("CVECode", StringType(), True),
        StructField("CVEName", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("StructureType", StringType(), True),
        StructField("Role", StringType(), True),
        StructField("Territory", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Municipality", StringType(), True),
        StructField("Latitude", FloatType(), True),
        StructField("Longitude", FloatType(), True),
        StructField("MetersAboveSeaLevel", FloatType(), True),
        StructField("ElectricalAccountNumber", StringType(), True),
        StructField("ElectricalVendor", StringType(), True),
        StructField("ElectricalEnergySource", StringType(), True),
        StructField("SiteOwner", StringType(), True),
        StructField("SharedSite", StringType(), True),
        StructField("OwnershipStatus", StringType(), True),
        StructField("Operator", StringType(), True),
        StructField("SymbolPowerSaving", StringType(), True),
        StructField("DatePartKey", StringType(), True)
    ])
    
def readFromCatalog(DB,TBL):
    dyf = glueContext.create_dynamic_frame.from_catalog(database=DB, table_name=TBL)
    return dyf

def getCCRbsDimFromArray():
    # include [DB,TABLE] separated by comma
    return [
        [db_athena,table_athena,'Glue Catalog'],
    ]
    
def readFromCatalog(DB,TBL):
    dyf = glueContext.create_dynamic_frame.from_catalog(database=DB, table_name=TBL)
    return dyf
    
def writeOnBucket(df,PART1,PART2,MODE):
    df.write.mode(MODE).partitionBy(PART1,PART2).parquet("s3a://"+str(bucket_dim)+"/"+str(rbs_trckng_fct))
    return "saved"

def getFromBucket(bucket_name, file_path):
    df = spark.read.parquet("s3://{}/{}".format(bucket_name, file_path))
    return df
    
def getCurrentDate():
    #diaAnterior = datetime.now().date() - timedelta(days=1)
    return datetime.now().date() - timedelta(days=1)
    
def insertIntoSrcObjKey(dim,lakedb,src_tp_nm):
    dfObjDim = getFromBucket(dim_bucket, src_obj_dim)
    max_id = dfObjDim.select(max("SRC_OBJ_KEY")).collect()[0][0]
    new_id = int(max_id) + 1
    new_record = (
        int(new_id),                                                   # SRC_OBJ_KEY
        getCurrentDate() + timedelta(days=1),                          # PPN_DT
        3,                                                             # SRC_STM_KEY
        "N/A",                                                         # UNQ_ID_SRC_STM
        "N/A",                                                         # CNTRL_MD5
        datetime.strptime("1990-01-01", "%Y-%m-%d").date(),            # EFF_DT
        datetime.strptime("2040-12-31", "%Y-%m-%d").date() ,           # END_DT
        "N/A",                                                         # SRC_OBJ_CD
        dim,                                                           # SRC_OBJ_NM
        lakedb,                                                        # SRC_OBJ_LNG_NM
        "Not Applicable",                                              # SRC_STREAM_NM
        0.0,                                                           # NODE_NO
        "N/A",                                                         # SRC_TP_CD
        src_tp_nm                                                      # SRC_TP_NM
    )
    newDf = spark.createDataFrame([new_record], dfObjDim.schema)
    newDf.write.mode("append").parquet("s3a://"+dim_bucket+"/"+src_obj_dim)
    return new_id
    
def insertIntoRbsTrckgFct(df,part1,part2,mode):
    df.write.mode(mode).partitionBy(part1,part2).parquet("s3a://"+dim_bucket+"/"+rbs_trckng_fct)
    return "saved "
    
def getHashColumns():
    return ["RBS_CD","RBS_ANL_CD","RBS_NM","RBS_CNSTR_DT","RBS_EFF_DT","RBS_END_DT","RBS_RETM_DT","FUNC_ST_NM","VNDR_NM",
    "TCHNLGY_CD","SITE_ID","SITE_NM","CNTLR_KEY","CNTLR_NM","CNTLR_ANL_CD","CVE_CD","CVE_NM","CGY_NM","STC_TP_NM",
    "ROLE_NM","TERR_NM","DEPT_NM","MNCP_NM","LTT","LGT","MAMSL","ELC_AC_CD","ELC_VNDR_NM","ELC_ENG_SRC_NM","OWN_NM",
    "SHR_F","OWN_ST_NM","OPR_NM","SYMB_PWR_SVG_F"]
    
def getBoOrderFields(df):
    return df.select(col("Date"),col("CountryCode"),col("AnalyticsRBSKey"),col("RBSCode"),col("RBSName"),
        col("ConstructionDate"),col("ServiceEffectiveDate"),col("ServiceEndDate"),col("RetirementDate"),
        col("FunctionalStatus"),col("VendorName"),col("TechnologyCode"),col("SiteID"),col("SiteName"),
        col("SiteName"),col("ControllerName"),col("CVECode"),col("CVEName"),col("Category"),
        col("StructureType"),col("Role"),col("Territory"),col("Department"),col("Municipality"),
        col("Latitude"),col("Longitude"),col("MetersAboveSeaLevel"),col("ElectricalAccountNumber"),
        col("ElectricalVendor"),col("ElectricalEnergySource"),col("SiteOwner"),col("SharedSite"),
        col("OwnershipStatus"),col("Operator"),col("SymbolPowerSaving"),col("DatePartKey")
    )
    
def bulkDataLoad():
    dfObjDim = getFromBucket(dim_bucket, src_obj_dim)
    #dfObjDim.show()
    dfStmDim = getFromBucket(dim_bucket, src_stm_dim)
    #dfStmDim.show()
    dfCntryDim = getFromBucket(dim_bucket, zeus_cntry_dim)
    #dfCntryDim = getFromBucket(dim_bucket, cntry_dim)
    ## CARGA MASIVA
    
    start_date = datetime(2023,11, 21).date()  # Se agrega .date() para obtener solo la fecha
    end_date = datetime(2023, 12, 13).date()  # De igual forma aquí
    currentDate = start_date
    
    while currentDate <= end_date:
        for conn in getCCRbsDimFromArray():
            #print(current_date)
            dyf = readFromCatalog(conn[0], conn[1])
            df = dyf.toDF().distinct()
            df = getBoOrderFields(df)
            df = df.withColumn("MetersAboveSeaLevel", col("MetersAboveSeaLevel").cast(FloatType()))
            df = spark.createDataFrame(df.collect(), schema=getRBSDimSchema())
            
            #currentDate = getCurrentDate() + timedelta(days=1)
            #currentDate = datetime.strptime("2023-08-21", "%Y-%m-%d").date()
            
            df = df.filter(col("Date") == lit(currentDate))
            
            df = df.withColumn("FCT_DT_KEY",date_format(to_date(col("Date")), "yyyyMMdd"))
            df = df.withColumnRenamed("Date","FCT_DT")
            df = df.withColumn("RBS_KEY",concat(col("CountryCode"),lit("-"),col("RBSName")))
            df = df.alias("A").join(
                dfCntryDim.alias("B").select(col("B.CNTRY_KEY"),col("B.CNTRY_CD")), 
                col("B.CNTRY_CD") == col("A.CountryCode"), 
                "inner").select(
                    "A.*","B.CNTRY_KEY"
                )
            df = df.withColumnRenamed("CountryCode","CNTRY_CD")
            df = df.withColumn("BTCH_KEY",lit("empty"))
            df = df.withColumn("PPN_DT", lit(currentDate))
            df = df.crossJoin(dfStmDim.filter(col("SRC_STM_NM")==lit("AWS")).select("SRC_STM_KEY"))
            #new_id = insertIntoSrcObjKey(conn[1],conn[0],conn[2])
            #df = df.withColumn("SRC_OBJ_KEY",lit(new_id))
            df = df.withColumn("SRC_OBJ_KEY",lit("Empty"))
            df = df.withColumn("UNQ_ID_SRC_STM",concat(col("CNTRY_CD"),lit("-"),col("RBSName")))
            #df = df.withColumn("CNTRL_HASH",sha2(concat_ws('', *[df[col] for col in df.columns]),256))
            df = df.withColumnRenamed("RBSCode","RBS_CD")
            df = df.withColumnRenamed("AnalyticsRBSKey","RBS_ANL_CD")
            df = df.withColumnRenamed("RBSName","RBS_NM")
            df = df.withColumnRenamed("ConstructionDate","RBS_CNSTR_DT")
            df = df.withColumnRenamed("ServiceEffectiveDate","RBS_EFF_DT")
            df = df.withColumnRenamed("ServiceEndDate","RBS_END_DT")
            df = df.withColumnRenamed("RetirementDate","RBS_RETM_DT")
            df = df.withColumnRenamed("FunctionalStatus","FUNC_ST_NM")
            df = df.withColumnRenamed("VendorName","VNDR_NM")
            df = df.withColumnRenamed("TechnologyCode","TCHNLGY_CD")
            df = df.withColumnRenamed("SiteID","SITE_ID")
            df = df.withColumnRenamed("SiteName","SITE_NM")
            df = df.withColumn("CNTLR_KEY",concat(col("CNTRY_CD"),lit("-"),col("ControllerName")))
            df = df.withColumnRenamed("ControllerName","CNTLR_NM")
            df = df.withColumnRenamed("AnalyticsControllerKey","CNTLR_ANL_CD")
            df = df.withColumnRenamed("CVECode","CVE_CD")
            df = df.withColumnRenamed("CVEName","CVE_NM")
            df = df.withColumnRenamed("Category","CGY_NM")
            df = df.withColumnRenamed("StructureType","STC_TP_NM")
            df = df.withColumnRenamed("Role","ROLE_NM")
            df = df.withColumnRenamed("Territory","TERR_NM")
            df = df.withColumnRenamed("Department","DEPT_NM")
            df = df.withColumnRenamed("Municipality","MNCP_NM")
            df = df.withColumnRenamed("Latitude","LTT")
            df = df.withColumnRenamed("Longitude","LGT")
            df = df.withColumnRenamed("MetersAboveSeaLevel","MAMSL")
            df = df.withColumnRenamed("ElectricalAccountNumber","ELC_AC_CD")
            df = df.withColumnRenamed("ElectricalVendor","ELC_VNDR_NM")
            df = df.withColumnRenamed("ElectricalEnergySource","ELC_ENG_SRC_NM")
            df = df.withColumnRenamed("SiteOwner","OWN_NM")
            df = df.withColumnRenamed("SharedSite","SHR_F")
            df = df.withColumnRenamed("OwnershipStatus","OWN_ST_NM")
            df = df.withColumnRenamed("Operator","OPR_NM")
            df = df.withColumnRenamed("SymbolPowerSaving","SYMB_PWR_SVG_F")
            df = df.withColumnRenamed("datepartkey","DatePartKey")
            df = df.withColumn("CNTRL_HASH",sha2(concat_ws('', *[df[col] for col in getHashColumns()]),256))
            df = selectOrderedFields(df)
            msg = insertIntoRbsTrckgFct(df,"DatePartKey","CNTRY_CD","append")

            currentDate += timedelta(days=1)

#bulkDataLoad()    
## RUN

def run():
    dfObjDim = getFromBucket(dim_bucket, src_obj_dim)
    dfStmDim = getFromBucket(dim_bucket, src_stm_dim)
    dfCntryDim = getFromBucket(dim_bucket, zeus_cntry_dim)
    
    for conn in getCCRbsDimFromArray():
        dyf = readFromCatalog(conn[0], conn[1])
        df = dyf.toDF().distinct()
        df = getBoOrderFields(df)
        df = df.withColumn("MetersAboveSeaLevel", col("MetersAboveSeaLevel").cast(FloatType()))
        df = spark.createDataFrame(df.collect(), schema=getRBSDimSchema())
        
        currentDate = getCurrentDate() #+ timedelta(days=1)
        #currentDate = datetime.strptime("2023-11-21", "%Y-%m-%d").date()
        
        df = df.filter(col("Date") == lit(currentDate))
        
        df = df.withColumn("FCT_DT_KEY",date_format(to_date(col("Date")), "yyyyMMdd"))
        df = df.withColumnRenamed("Date","FCT_DT")
        df = df.withColumn("RBS_KEY",concat(col("CountryCode"),lit("-"),col("RBSName")))
        df = df.alias("A").join(
            dfCntryDim.alias("B").select(col("B.CNTRY_KEY"),col("B.CNTRY_CD")), 
            col("B.CNTRY_CD") == col("A.CountryCode"), 
            "inner").select(
                "A.*","B.CNTRY_KEY"
            )
        df = df.withColumnRenamed("CountryCode","CNTRY_CD")
        df = df.withColumn("BTCH_KEY",lit("empty"))
        df = df.withColumn("PPN_DT", lit(getCurrentDate()))
        df = df.crossJoin(dfStmDim.filter(col("SRC_STM_NM")==lit("AWS")).select("SRC_STM_KEY"))
        #new_id = insertIntoSrcObjKey(conn[1],conn[0],conn[2])
        #df = df.withColumn("SRC_OBJ_KEY",lit(new_id))
        df = df.withColumn("SRC_OBJ_KEY",lit("Empty"))
        df = df.withColumn("UNQ_ID_SRC_STM",concat(col("CNTRY_CD"),lit("-"),col("RBSName")))
        #df = df.withColumn("CNTRL_HASH",sha2(concat_ws('', *[df[col] for col in df.columns]),256))
        df = df.withColumnRenamed("RBSCode","RBS_CD")
        df = df.withColumnRenamed("AnalyticsRBSKey","RBS_ANL_CD")
        df = df.withColumnRenamed("RBSName","RBS_NM")
        df = df.withColumnRenamed("ConstructionDate","RBS_CNSTR_DT")
        df = df.withColumnRenamed("ServiceEffectiveDate","RBS_EFF_DT")
        df = df.withColumnRenamed("ServiceEndDate","RBS_END_DT")
        df = df.withColumnRenamed("RetirementDate","RBS_RETM_DT")
        df = df.withColumnRenamed("FunctionalStatus","FUNC_ST_NM")
        df = df.withColumnRenamed("VendorName","VNDR_NM")
        df = df.withColumnRenamed("TechnologyCode","TCHNLGY_CD")
        df = df.withColumnRenamed("SiteID","SITE_ID")
        df = df.withColumnRenamed("SiteName","SITE_NM")
        df = df.withColumn("CNTLR_KEY",concat(col("CNTRY_CD"),lit("-"),col("ControllerName")))
        df = df.withColumnRenamed("ControllerName","CNTLR_NM")
        df = df.withColumnRenamed("AnalyticsControllerKey","CNTLR_ANL_CD")
        df = df.withColumnRenamed("CVECode","CVE_CD")
        df = df.withColumnRenamed("CVEName","CVE_NM")
        df = df.withColumnRenamed("Category","CGY_NM")
        df = df.withColumnRenamed("StructureType","STC_TP_NM")
        df = df.withColumnRenamed("Role","ROLE_NM")
        df = df.withColumnRenamed("Territory","TERR_NM")
        df = df.withColumnRenamed("Department","DEPT_NM")
        df = df.withColumnRenamed("Municipality","MNCP_NM")
        df = df.withColumnRenamed("Latitude","LTT")
        df = df.withColumnRenamed("Longitude","LGT")
        df = df.withColumnRenamed("MetersAboveSeaLevel","MAMSL")
        df = df.withColumnRenamed("ElectricalAccountNumber","ELC_AC_CD")
        df = df.withColumnRenamed("ElectricalVendor","ELC_VNDR_NM")
        df = df.withColumnRenamed("ElectricalEnergySource","ELC_ENG_SRC_NM")
        df = df.withColumnRenamed("SiteOwner","OWN_NM")
        df = df.withColumnRenamed("SharedSite","SHR_F")
        df = df.withColumnRenamed("OwnershipStatus","OWN_ST_NM")
        df = df.withColumnRenamed("Operator","OPR_NM")
        df = df.withColumnRenamed("SymbolPowerSaving","SYMB_PWR_SVG_F")
        df = df.withColumnRenamed("datepartkey","DatePartKey")
        df = df.withColumn("CNTRL_HASH",sha2(concat_ws('', *[df[col] for col in getHashColumns()]),256))
        df = selectOrderedFields(df)
        msg = insertIntoRbsTrckgFct(df,"DatePartKey","CNTRY_CD","append")

run()


    
job.commit()


    
