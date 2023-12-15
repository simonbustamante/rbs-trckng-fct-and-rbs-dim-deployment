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
db_athena = "hn-anl-prd-ntwrk-link"
table_athena = "zeus_rbs_dim"
src_obj_dim = "src_obj_dim"
src_stm_dim = "src_stm_dim"
zeus_cntry_dim = "zeus_cntry_dim"
rbs_trckng_fct = "rbs_trckng_fct"
rbs_trckng_fct_temp = "rbs_trckng_fct_temp"

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
        StructField("Latitude", DecimalType() , True), # original FloatType()
        StructField("Longitude", DecimalType(), True), # original FloatType()
        StructField("MetersAboveSeaLevel", DecimalType(), True), # original FloatType()
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
    
def insertIntoRbsTrckgFctTemp(df,mode):
    df.write.mode(mode).parquet("s3a://"+dim_bucket+"/"+rbs_trckng_fct_temp)
    
def getHashColumns():
    return ["RBS_CD","RBS_ANL_CD","RBS_NM","RBS_CNSTR_DT","RBS_EFF_DT","RBS_END_DT","RBS_RETM_DT","FUNC_ST_NM","VNDR_NM",
    "TCHNLGY_CD","SITE_ID","SITE_NM","CNTLR_KEY","CNTLR_NM","CNTLR_ANL_CD","CVE_CD","CVE_NM","CGY_NM","STC_TP_NM",
    "ROLE_NM","TERR_NM","DEPT_NM","MNCP_NM","LTT","LGT","MAMSL","ELC_AC_CD","ELC_VNDR_NM","ELC_ENG_SRC_NM","OWN_NM",
    "SHR_F","OWN_ST_NM","OPR_NM","SYMB_PWR_SVG_F"]
    
def bulkDataLoad():
    dfObjDim = getFromBucket(dim_bucket, src_obj_dim)
    #dfObjDim.show()
    dfStmDim = getFromBucket(dim_bucket, src_stm_dim)
    #dfStmDim.show()
    dfCntryDim = getFromBucket(dim_bucket, zeus_cntry_dim)
    #dfCntryDim.show(5)
    #dfCntryDim = getFromBucket(dim_bucket, cntry_dim)
    ## CARGA MASIVA
    start_date = datetime(2023, 11, 19).date()  # Se agrega .date() para obtener solo la fecha
    end_date = datetime(2023, 12, 13).date()  # De igual forma aquí
    currentDate = start_date
    while currentDate <= end_date:
        #print(current_date)


        for conn in getCCRbsDimFromArray():
            dyf = readFromCatalog(conn[0], conn[1])
            df = dyf.toDF().distinct()
            df = spark.createDataFrame(df.collect(), schema=getRBSDimSchema())

            #currentDate = getCurrentDate()
            #currentDate = datetime.strptime("2023-07-05", "%Y-%m-%d").date()
            
            df = df.withColumn("Latitude", col("Latitude").cast(FloatType()))
            df = df.withColumn("Longitude", col("Longitude").cast(FloatType()))
            df = df.withColumn("MetersAboveSeaLevel", col("MetersAboveSeaLevel").cast(FloatType()))

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
            print(msg)
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
        df = spark.createDataFrame(df.collect(), schema=getRBSDimSchema())
        
        currentDate = getCurrentDate() #+ timedelta(days=1)
        #currentDate = datetime.strptime("2023-11-16", "%Y-%m-%d").date()
        
        df = df.withColumn("Latitude", col("Latitude").cast(FloatType()))
        df = df.withColumn("Longitude", col("Longitude").cast(FloatType()))
        df = df.withColumn("MetersAboveSeaLevel", col("MetersAboveSeaLevel").cast(FloatType()))
        
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
        
        # limpiando registros existentes
        currentRbsTrckngFct = getFromBucket(dim_bucket, rbs_trckng_fct)
        rbs_trckng_fct_joined = df.alias("A").join(
            currentRbsTrckngFct.alias("B"),
            (col("A.fct_dt_key") == col("B.fct_dt_key")) & (col("A.rbs_key") == col("B.rbs_key")) & (col("A.cntry_key") == col("B.cntry_key")),
            "leftouter"
        ).select(
            coalesce(col("A.FCT_DT_KEY"),col("B.FCT_DT_KEY")).alias("FCT_DT_KEY"),
            coalesce(col("A.FCT_DT"),col("B.FCT_DT")).alias("FCT_DT"),
            coalesce(col("A.RBS_KEY"),col("B.RBS_KEY")).alias("RBS_KEY"),
            coalesce(col("A.CNTRY_KEY"),col("B.CNTRY_KEY")).alias("CNTRY_KEY"),
            coalesce(col("A.CNTRY_CD"),col("B.CNTRY_CD")).alias("CNTRY_CD"),
            coalesce(col("A.BTCH_KEY"),col("B.BTCH_KEY")).alias("BTCH_KEY"),
            coalesce(col("A.PPN_DT"),col("B.PPN_DT")).alias("PPN_DT"),
            coalesce(col("A.SRC_STM_KEY"),col("B.SRC_STM_KEY")).alias("SRC_STM_KEY"),
            coalesce(col("A.SRC_OBJ_KEY"),col("B.SRC_OBJ_KEY")).alias("SRC_OBJ_KEY"),
            coalesce(col("A.UNQ_ID_SRC_STM"),col("B.UNQ_ID_SRC_STM")).alias("UNQ_ID_SRC_STM"),
            coalesce(col("A.CNTRL_HASH"),col("B.CNTRL_HASH")).alias("CNTRL_HASH"),
            coalesce(col("A.RBS_CD"),col("B.RBS_CD")).alias("RBS_CD"),
            coalesce(col("A.RBS_ANL_CD"),col("B.RBS_ANL_CD")).alias("RBS_ANL_CD"),
            coalesce(col("A.RBS_NM"),col("B.RBS_NM")).alias("RBS_NM"),
            coalesce(col("A.RBS_CNSTR_DT"),col("B.RBS_CNSTR_DT")).alias("RBS_CNSTR_DT"),
            coalesce(col("A.RBS_EFF_DT"),col("B.RBS_EFF_DT")).alias("RBS_EFF_DT"),
            coalesce(col("A.RBS_END_DT"),col("B.RBS_END_DT")).alias("RBS_END_DT"),
            coalesce(col("A.RBS_RETM_DT"),col("B.RBS_RETM_DT")).alias("RBS_RETM_DT"),
            coalesce(col("A.FUNC_ST_NM"),col("B.FUNC_ST_NM")).alias("FUNC_ST_NM"),
            coalesce(col("A.VNDR_NM"),col("B.VNDR_NM")).alias("VNDR_NM"),
            coalesce(col("A.TCHNLGY_CD"),col("B.TCHNLGY_CD")).alias("TCHNLGY_CD"),
            coalesce(col("A.SITE_ID"),col("B.SITE_ID")).alias("SITE_ID"),
            coalesce(col("A.SITE_NM"),col("B.SITE_NM")).alias("SITE_NM"),
            coalesce(col("A.CNTLR_KEY"),col("B.CNTLR_KEY")).alias("CNTLR_KEY"),
            coalesce(col("A.CNTLR_NM"),col("B.CNTLR_NM")).alias("CNTLR_NM"),
            coalesce(col("A.CNTLR_ANL_CD"),col("B.CNTLR_ANL_CD")).alias("CNTLR_ANL_CD"),
            coalesce(col("A.CVE_CD"),col("B.CVE_CD")).alias("CVE_CD"),
            coalesce(col("A.CVE_NM"),col("B.CVE_NM")).alias("CVE_NM"),
            coalesce(col("A.CGY_NM"),col("B.CGY_NM")).alias("CGY_NM"),
            coalesce(col("A.STC_TP_NM"),col("B.STC_TP_NM")).alias("STC_TP_NM"),
            coalesce(col("A.ROLE_NM"),col("B.ROLE_NM")).alias("ROLE_NM"),
            coalesce(col("A.TERR_NM"),col("B.TERR_NM")).alias("TERR_NM"),
            coalesce(col("A.DEPT_NM"),col("B.DEPT_NM")).alias("DEPT_NM"),
            coalesce(col("A.MNCP_NM"),col("B.MNCP_NM")).alias("MNCP_NM"),
            coalesce(col("A.LTT"),col("B.LTT")).alias("LTT"),
            coalesce(col("A.LGT"),col("B.LGT")).alias("LGT"),
            coalesce(col("A.MAMSL"),col("B.MAMSL")).alias("MAMSL"),
            coalesce(col("A.ELC_AC_CD"),col("B.ELC_AC_CD")).alias("ELC_AC_CD"),
            coalesce(col("A.ELC_VNDR_NM"),col("B.ELC_VNDR_NM")).alias("ELC_VNDR_NM"),
            coalesce(col("A.ELC_ENG_SRC_NM"),col("B.ELC_ENG_SRC_NM")).alias("ELC_ENG_SRC_NM"),
            coalesce(col("A.OWN_NM"),col("B.OWN_NM")).alias("OWN_NM"),
            coalesce(col("A.SHR_F"),col("B.SHR_F")).alias("SHR_F"),
            coalesce(col("A.OWN_ST_NM"),col("B.OWN_ST_NM")).alias("OWN_ST_NM"),
            coalesce(col("A.OPR_NM"),col("B.OPR_NM")).alias("OPR_NM"),
            coalesce(col("A.SYMB_PWR_SVG_F"),col("B.SYMB_PWR_SVG_F")).alias("SYMB_PWR_SVG_F"),
            coalesce(col("A.DatePartKey"),col("B.DatePartKey")).alias("DatePartKey") 
        )
        
        # save rbs tracking fact
        insertIntoRbsTrckgFctTemp(rbs_trckng_fct_joined,"overwrite")
        RbsTrckngFctToSave = getFromBucket(dim_bucket, rbs_trckng_fct_temp)
        msg = insertIntoRbsTrckgFct(RbsTrckngFctToSave,"DatePartKey","CNTRY_CD","append")
        print(msg)

# Ejecución
run()
