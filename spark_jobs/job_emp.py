from ntpath import join
from pyspark.sql import SparkSession
# , SaveMode, Row, DataFrame
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import col, expr, struct, to_json
import pyspark.sql.functions as func
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql import DataFrameWriter
# from pyspark.sql.functions import expr


spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath","/opt/bitnami/spark/jars/mssql-jdbc-6.4.0.jre8.jar") \
    .appName("STREAM") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

EMP = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "PRECDB.C##MYUSER.EMP") \
    .option("startingOffsets", "earliest") \
    .option("minPartitions",4) \
    .load()

# HZ_CUST_ACCOUNTS = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.AR.HZ_CUST_ACCOUNTS") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

# HR_ALL_ORGANIZATION_UNITS = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.HR.HR_ALL_ORGANIZATION_UNITS") \
#     .option("minPartitions",4) \
#     .option("startingOffsets", "earliest") \
#     .load()

# MTL_SYSTEM_ITEMS_B = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.INV.MTL_SYSTEM_ITEMS_B") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

# OE_TRANSACTION_TYPES_ALL = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.ONT.OE_TRANSACTION_TYPES_ALL") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

# OE_TRANSACTION_TYPES_TL = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.ONT.OE_TRANSACTION_TYPES_TL") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

# OE_ORDER_HEADERS_ALL = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.ONT.OE_ORDER_HEADERS_ALL") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

# OE_ORDER_LINES_ALL = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("subscribe", "EBSPRE.ONT.OE_ORDER_LINES_ALL") \
#     .option("startingOffsets", "earliest") \
#     .option("minPartitions",4) \
#     .load()

with open('/opt/Confluent/schemas/emp.json','r') as f:
  schema_HZP = f.read()

# with open('/opt/Confluent/schemas/hz_cust_accounts.json','r') as f:
#   schema_HZC = f.read()

# with open('/opt/Confluent/schemas/hr_all_organization_units.json','r') as f:
#   schema_hr = f.read()

# with open('/opt/Confluent/schemas/mtl_system_items_b.json','r') as f:
#   schema_inv = f.read()
  
# with open('/opt/Confluent/schemas/oe_transaction_types_all.json','r') as f:
#   schema_oe_all = f.read()

# with open('/opt/Confluent/schemas/oe_transaction_types_tl.json','r') as f:
#   schema_oe_tl = f.read()

# with open('/opt/Confluent/schemas/oe_order_headers_all.json','r') as f:
#   schema_oe_headers_all = f.read()

# with open('/opt/Confluent/schemas/oe_order_lines_all.json','r') as f:
#   schema_oe_lines_all = f.read()

# Perfectly Working
hp = HZ_PARTIES.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_HZP).alias("hp")) \
       .select("hp.PARTY_ID")
# Perfectly Working
hca = HZ_CUST_ACCOUNTS.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_HZC).alias("hca")) \
      .select("hca.PARTY_ID", "hca.CUST_ACCOUNT_ID")
# Perfectly Working
haou = HR_ALL_ORGANIZATION_UNITS.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_hr).alias("haou")) \
        .select("haou.ORGANIZATION_ID") \
            .filter("haou.BUSINESS_GROUP_ID = 101")

# yet to test
inv = MTL_SYSTEM_ITEMS_B.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_inv).alias("inv")) \
        .select("inv.DESCRIPTION", "inv.SEGMENT1") \
            .filter("inv.ORGANIZATION_ID = 105")

# Perfectly Working
ot = OE_TRANSACTION_TYPES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_all).alias("ot")) \
        .select("ot.TRANSACTION_TYPE_ID", "ot.ATTRIBUTE4", "ot.ATTRIBUTE6")
# Perfectly Working
ottt = OE_TRANSACTION_TYPES_TL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_tl).alias("ottt")) \
        .select("ottt.TRANSACTION_TYPE_ID") \
            .filter("ottt.LANGUAGE = 'US'")
# Perfectly Working
ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE") 

# Perfectly Working
ool = OE_ORDER_LINES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_lines_all).alias("ool")) \
        .select( "ool.CREATION_DATE", "ool.LAST_UPDATE_DATE", "ool.LINE_CATEGORY_CODE" \
            ,  "ool.UNIT_LIST_PRICE", "ool.ORDERED_QUANTITY" \
                , "ool.ORDERED_ITEM","ool.HEADER_ID") \
                    .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'") \
                        .filter("ool.LAST_UPDATE_DATE >= '2022-01-01'")

print("ready to join")
# Join
joining_result = ooh.join(ool, "HEADER_ID") \
    .join(ot, ot["TRANSACTION_TYPE_ID"] == ooh["ORDER_TYPE_ID"]) \
        .join(ottt, "TRANSACTION_TYPE_ID") \
            .join(hca, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
                .join(hp, "party_id") \
                    .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
                        .join(inv, ool["ORDERED_ITEM"] == inv["SEGMENT1"])

print("join successfull")
joining_result.printSchema()
# print("ready to write on console")
# query = joining_result \
#     .writeStream \
#     .format("console") \
#     .start().awaitTermination()

# print("start to write")

# database = "STCC"
# table = "dbo.device_sales_tables_new"
# user = "SA"
# password  = "MhffPOC2022"
# # intvl = 0
# def writesql(dff, epoch_id):
#     dff.write.mode("overwrite") \
#         .format("jdbc") \
#         .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
#         .option("dbtable", table) \
#         .option("user", user) \
#         .option("password", password) \
#         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#         .save()
#     print("Iteration ")
#     # intvl+=1



# print("after iteration")
# query = joining_result.writeStream.outputMode("append").foreachBatch(writesql).start()
# query.awaitTermination()


print("start to write")
query = joining_result \
            .selectExpr("CAST(struct(*) as string) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
            .option("checkpointLocation", "checkk") \
            .option("topic", "complex_query") \
            .start().awaitTermination() 

# # write as avro
# joining_result.select(to_json(struct(joining_result.ORDERED_DATE, joining_result.ATTRIBUTE4, joining_result.ATTRIBUTE6, joining_result.DESCRIPTION, 
# joining_result.UNIT_LIST_PRICE, joining_result.ORDERED_QUANTITY, joining_result.ORDERED_ITEM,  joining_result.CREATION_DATE,  joining_result.LAST_UPDATE_DATE, 
#  joining_result.ORDERED_ITEM,  joining_result.ORDERED_ITEM))).alias("value") \
#       .writeStream \
#       .format("kafka") \
#       .outputMode("append") \
#       .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#       .option("topic", "avro_data_topic_1") \
#       .option("checkpointLocation","check") \
#       .start() \
#       .awaitTermination()
# [PARTY_ID: bigint, TRANSACTION_TYPE_ID: double, HEADER_ID: double, ORDER_TYPE_ID: double, SHIP_FROM_ORG_ID: double, SOLD_TO_ORG_ID: double, ORDERED_DATE: date, CREATION_DATE: date, LAST_UPDATE_DATE: date, LINE_CATEGORY_CODE: string, UNIT_LIST_PRICE: double, ORDERED_QUANTITY: double, ORDERED_ITEM: string, ATTRIBUTE4: string, ATTRIBUTE6: string, CUST_ACCOUNT_ID: bigint, ORGANIZATION_ID: bigint, DESCRIPTION: string, SEGMENT1: string]

# joining_result\
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
#     .option("topic", "kontext-kafka-3") \
#     .option("checkpointLocation", "checkpoint") \
#     .start().awaitTermination()