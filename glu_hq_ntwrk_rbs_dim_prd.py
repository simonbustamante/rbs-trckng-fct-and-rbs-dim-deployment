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
from pyspark.sql.window import Window
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

dim_bucket = "s3-hq-anl-prd-ntwrk"
db_athena = "hq-anl-prd-ntwrk-link"
src_obj_dim = "src_obj_dim"
src_stm_dim = "src_stm_dim"
zeus_cntry_dim = "zeus_cntry_dim"
rbs_trckng_fct = "rbs_trckng_fct"
zeus_rbs_dim_sl = "zeus_rbs_dim"
rbs_dim_tmp = "zeus_rbs_dim_tmp"

def selectOrderedFields(df):
    return df.select(
        col("RBS_KEY"),
        col("CNTRY_KEY"),
        col("BTCH_KEY"),
        col("PPN_DT"),
        col("LAST_UDT_DT"),
        col("SRC_STM_KEY"),
        col("SRC_OBJ_KEY"),
        col("UNQ_ID_SRC_STM"),
        col("CNTRL_HASH"),
        col("EFF_DT"),
        col("END_DT"),
        col("ACTV_F"),
        col("LAST_FCT_DT_KEY"),
        #col("LAST_RCRD_F"),
        col("RBS_CD"),
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
        col("CNTLR_CD"),
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
        col("ELC_ENG_SRC_NM"),
        col("ELC_VNDR_NM"),
        col("OWN_NM"),
        col("SHR_F"),
        col("OWN_ST_NM"),
        col("OPR_NM"),
        col("SYMB_PWR_SVG_F"),
        #col("DatePartKey")
    )
    
def getRBSDimScndLvlSchema():
    return StructType([
        StructField("RBS_KEY", StringType(), True),
        StructField("CNTRY_KEY", IntegerType(), True),
        StructField("BTCH_KEY", StringType(), True),
        StructField("PPN_DT", DateType(), True),
        StructField("LAST_UDT_DT", DateType(), True),
        StructField("SRC_STM_KEY", StringType(), True),
        StructField("SRC_OBJ_KEY", StringType(), True), 
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
    
def readFromCatalog(DB,TBL):
    dyf = glueContext.create_dynamic_frame.from_catalog(database=DB, table_name=TBL)
    return dyf
    
def readFromBucket(bucket,prefix):
    return spark.read.parquet("s3a://"+bucket+"/"+prefix)
    
def getRbsTrckngFctFromArray():
    # include [DB,TABLE] separated by comma
    return [
        [db_athena,rbs_trckng_fct,'Glue Catalog'],
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
        getCurrentDate(),                                              # PPN_DT
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
    
def insertIntoRbsDim(df,path,mode):
    df.write.mode(mode).parquet("s3a://"+dim_bucket+"/"+path)
    return "saved "
    
## RUN
def run():
    dfObjDim = getFromBucket(dim_bucket, src_obj_dim)
    dfStmDim = getFromBucket(dim_bucket, src_stm_dim)
    dfCntryDim = getFromBucket(dim_bucket, zeus_cntry_dim)

    for conn in getRbsTrckngFctFromArray():
        rbs_trckg_fct = readFromCatalog(conn[0], conn[1]).toDF()
        rbsdimSl = readFromBucket(dim_bucket,zeus_rbs_dim_sl)
        
        
        # Definir ventana para RBS_TRCKNG_FCT
        window_spec = Window.orderBy("FCT_DT").partitionBy("RBS_KEY")
        
        # Agregar el CNTRL_HASH anterior y FCT_DT anterior
        rbs_trckg_fct = rbs_trckg_fct.withColumn("previous_CNTRL_HASH", lag("CNTRL_HASH").over(window_spec))
        rbs_trckg_fct = rbs_trckg_fct.withColumn("previous_FCT_DT", lag("FCT_DT").over(window_spec))
        
        # Asigna un 1 cuando el CNTRL_HASH cambia, de lo contrario 0
        rbs_trckg_fct = rbs_trckg_fct.withColumn("hash_change", when(
            (col("CNTRL_HASH") != col("previous_CNTRL_HASH")) |
            ((datediff(col("FCT_DT"), col("previous_FCT_DT")) > 1) &
            (col("CNTRL_HASH") == col("previous_CNTRL_HASH"))), 1
        ).otherwise(0))
        
        # Crea la columna incremental sumando la columna hash_change. Usamos la función cumsum de ventana
        window_spec_cumsum = Window.orderBy("FCT_DT").partitionBy("RBS_KEY").rowsBetween(Window.unboundedPreceding, 0)
        rbs_trckg_fct = rbs_trckg_fct.withColumn("INCREMENTAL", sum("hash_change").over(window_spec_cumsum))
        
        #currentDate = getCurrentDate()
        currentDate = getCurrentDate() - timedelta(days=1) # data del dia anterior
        #currentDate = datetime.strptime("2023-08-22", "%Y-%m-%d").date()
        #currentLessOne = currentDate - timedelta(days=1)
        
        rbs_dim_scnd_lvl_tmp = rbs_trckg_fct.groupBy(
            col("RBS_KEY").alias("OLD_RBS_KEY"),
            col("INCREMENTAL"),
            col("CNTRY_KEY"),
            col("BTCH_KEY"),
            lit(currentDate).alias("PPN_DT"),
            lit(currentDate).alias("LAST_UDT_DT"),
            col("SRC_STM_KEY"),
            col("UNQ_ID_SRC_STM"),
            ####### hash below next key #######
            col("CNTRL_HASH").alias("CNTRL_HASH"),
            col("RBS_CD"),
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
            col("CNTLR_NM").alias("CNTLR_CD"),
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
            col("ELC_ENG_SRC_NM"),
            col("ELC_VNDR_NM"),
            col("OWN_NM"),
            col("SHR_F"),
            col("OWN_ST_NM"),
            col("OPR_NM"),
            col("SYMB_PWR_SVG_F")
        ).agg(
            concat(col("RBS_KEY"),lit("-"),date_format(min("FCT_DT"),"yyyyMMdd").cast(StringType())).alias("RBS_KEY"),
            min("FCT_DT").alias("EFF_DT"),
            max("FCT_DT").alias("END_DT"),
            max("FCT_DT_KEY").alias("LAST_FCT_DT_KEY"),
        ).orderBy(col("UNQ_ID_SRC_STM").asc())
        
        
        
        #new_id = insertIntoSrcObjKey(conn[1],conn[0],conn[2])
        #incluyendo SRC_OBJ_KEY
        #rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("SRC_OBJ_KEY",lit(new_id))
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("SRC_OBJ_KEY",lit("Empty"))
        
        #activar ultima END_DT como ACTV_F = Y segun UNQ_ID_SRC_STM
        window_spec = Window.partitionBy("UNQ_ID_SRC_STM")
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("ACTV_F",when(col("END_DT") == max("END_DT").over(window_spec), lit("Y")).otherwise(lit("N")))
        
        #actualizar ultima END_DT con fecha 2040-12-31
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("END_DT",when(col("END_DT") == max("END_DT").over(window_spec), lit(datetime.strptime("2040-12-31", "%Y-%m-%d").date())).otherwise(col("END_DT")))  
        
        #se crea una campo auxiliar LAST_FCT_DT_KEY_AUX que permite verificar en formato de fecha con LAST_UDT_DT 
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("LAST_FCT_DT_KEY_AUX", concat_ws("-", substring("LAST_FCT_DT_KEY", 1, 4), substring("LAST_FCT_DT_KEY", 5, 2), substring("LAST_FCT_DT_KEY", 7, 2)).cast(DateType()))
        
        #desactivar ACTV_F = N todas las RBS_KEY que no han sido  recibidas al dia de hoy entonces la diferencia entre LAST_UDT_DT y LAST_FCT_DT_KEY_AUX es mayor que 1
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("ACTV_F",when((datediff("LAST_UDT_DT", "LAST_FCT_DT_KEY_AUX") > 0) & (col("ACTV_F")==lit("Y")), lit("N")).otherwise(col("ACTV_F")))
        
        #actualizar max(END_DT) con el valor de LAST_FCT_DT_KEY_AUX para todo los  ACTV_F = N que tienen diferencia de LAST_UDT_DT y LAST_FCT_DT_KEY_AUX mayor a 1
        rbs_dim_scnd_lvl_tmp = rbs_dim_scnd_lvl_tmp.withColumn("END_DT",when((col("END_DT") == max("END_DT").over(window_spec)) & (col("ACTV_F") == lit("N")),col("LAST_FCT_DT_KEY_AUX")).otherwise(col("END_DT")))
        
        # Standarización de rbs_dim_scnd_lvl_tmp y rbsdimSl para hacer join antes de salvar y actualizar - se descartan campos auxialiares
        rbs_dim_scnd_lvl_tmp = selectOrderedFields(rbs_dim_scnd_lvl_tmp)
        rbsdimSl = selectOrderedFields(rbsdimSl)
        
        # Join entre lo existente rbsdimSl y lo nuevo rbs_dim_scnd_lvl_tmp
        rbsdimSl_tmp = rbs_dim_scnd_lvl_tmp.alias("A").join(
            rbsdimSl.alias("B"),
            col("A.RBS_KEY") == col("B.RBS_KEY"),
            "leftouter"
        ).select(
            coalesce(col("A.RBS_KEY"),col("B.RBS_KEY")).alias("RBS_KEY"),
            coalesce(col("A.CNTRY_KEY"),col("B.CNTRY_KEY")).alias("CNTRY_KEY"),
            coalesce(col("A.BTCH_KEY"),col("B.BTCH_KEY")).alias("BTCH_KEY"),
            coalesce(col("A.PPN_DT"),col("B.PPN_DT")).alias("PPN_DT"),
            coalesce(col("A.LAST_UDT_DT"),col("B.LAST_UDT_DT")).alias("LAST_UDT_DT"),
            coalesce(col("A.SRC_STM_KEY"),col("B.SRC_STM_KEY")).alias("SRC_STM_KEY"),
            coalesce(col("A.SRC_OBJ_KEY"),col("B.SRC_OBJ_KEY")).alias("SRC_OBJ_KEY"),
            coalesce(col("A.UNQ_ID_SRC_STM"),col("B.UNQ_ID_SRC_STM")).alias("UNQ_ID_SRC_STM"),
            coalesce(col("A.CNTRL_HASH"),col("B.CNTRL_HASH")).alias("CNTRL_HASH"),
            coalesce(col("A.EFF_DT"),col("B.EFF_DT")).alias("EFF_DT"),
            coalesce(col("A.END_DT"),col("B.END_DT")).alias("END_DT"),
            coalesce(col("A.ACTV_F"),col("B.ACTV_F")).alias("ACTV_F"),
            coalesce(col("A.LAST_FCT_DT_KEY").cast(IntegerType()),col("B.LAST_FCT_DT_KEY").cast(IntegerType())).alias("LAST_FCT_DT_KEY").cast(IntegerType()),
            #coalesce(col("A.LAST_RCRD_F"),col("B.LAST_RCRD_F")).alias(""),
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
            coalesce(col("A.CNTLR_CD"),col("B.CNTLR_CD")).alias("CNTLR_CD"),
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
            coalesce(col("A.ELC_ENG_SRC_NM"),col("B.ELC_ENG_SRC_NM")).alias("ELC_ENG_SRC_NM"),
            coalesce(col("A.ELC_VNDR_NM"),col("B.ELC_VNDR_NM")).alias("ELC_VNDR_NM"),
            coalesce(col("A.OWN_NM"),col("B.OWN_NM")).alias("OWN_NM"),
            coalesce(col("A.SHR_F"),col("B.SHR_F")).alias("SHR_F"),
            coalesce(col("A.OWN_ST_NM"),col("B.OWN_ST_NM")).alias("OWN_ST_NM"),
            coalesce(col("A.OPR_NM"),col("B.OPR_NM")).alias("OPR_NM"),
            coalesce(col("A.SYMB_PWR_SVG_F"),col("B.SYMB_PWR_SVG_F")).alias("SYMB_PWR_SVG_F"),
            #coalesce(col("A.DatePartKey"),col("B.DatePartKey")).alias("DatePartKey")
        ).orderBy("UNQ_ID_SRC_STM")
        
        #rbsdimSl_tmp.filter((col("UNQ_ID_SRC_STM")==lit("HN-SPG024"))).show()
        
        insertIntoRbsDim(rbsdimSl_tmp,rbs_dim_tmp,"overwrite")
        rbsdimSl = readFromBucket(dim_bucket,rbs_dim_tmp)
        insertIntoRbsDim(rbsdimSl,zeus_rbs_dim_sl,"overwrite")

run()

job.commit()