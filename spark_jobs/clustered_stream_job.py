from pyspark.sql import SparkSession
# , SaveMode, Row, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
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

HZ_PARTIES = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.AR.HZ_PARTIES") \
    .option("startingOffsets", "earliest") \
    .load()

HZ_CUST_ACCOUNTS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.AR.HZ_CUST_ACCOUNTS") \
    .option("startingOffsets", "earliest") \
    .load()

HR_ALL_ORGANIZATION_UNITS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.HR.HR_ALL_ORGANIZATION_UNITS") \
    .option("startingOffsets", "earliest") \
    .load()

MTL_SYSTEM_ITEMS_B = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.INV.MTL_SYSTEM_ITEMS_B") \
    .option("startingOffsets", "earliest") \
    .load()

OE_TRANSACTION_TYPES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.ONT.OE_TRANSACTION_TYPES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_TRANSACTION_TYPES_TL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.ONT.OE_TRANSACTION_TYPES_TL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_ORDER_HEADERS_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.ONT.OE_ORDER_HEADERS_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_ORDER_LINES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "SPROD.ONT.OE_ORDER_LINES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/home/jobs/Confluent/schemas/hz_parties.json','r') as f:
  schema_HZP = f.read()

with open('/home/jobs/Confluent/schemas/hz_cust_accounts.json','r') as f:
  schema_HZC = f.read()

with open('/home/jobs/Confluent/schemas/hr_all_organization_units.json','r') as f:
  schema_hr = f.read()

with open('/home/jobs/Confluent/schemas/mtl_system_items_b.json','r') as f:
  schema_inv = f.read()
  
with open('/home/jobs/Confluent/schemas/oe_transaction_types_all.json','r') as f:
  schema_oe_all = f.read()

with open('/home/jobs/Confluent/schemas/oe_transaction_types_tl.json','r') as f:
  schema_oe_tl = f.read()

with open('/home/jobs/Confluent/schemas/oe_order_headers_all.json','r') as f:
  schema_oe_headers_all = f.read()

with open('/home/jobs/Confluent/schemas/oe_order_lines_all.json','r') as f:
  schema_oe_lines_all = f.read()

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
# .filter("ottt.TRANSACTION_TYPE_ID == 1226.0")
# Perfectly Working
ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE") 
                # .filter( "ooh.ORDERED_DATE >= '2022-01-01'")

# .filter("ooh.FLOW_STATUS_CODE = 'CLOSED'")
# .filter("ooh.SOLD_TO_ORG_ID= 132778.0000000000")
# ,"ooh.FLOW_STATUS_CODE").filter("ooh.FLOW_STATUS_CODE == 'CLOSED'")
# .filter("ooh.HEADER_ID == 1669.0")
# .filter("ooh.HEADER_ID == 1669")

# Perfectly Working
ool = OE_ORDER_LINES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_lines_all).alias("ool")) \
        .select( "ool.CREATION_DATE", "ool.LAST_UPDATE_DATE", "ool.LINE_CATEGORY_CODE" \
            ,  "ool.UNIT_LIST_PRICE", "ool.ORDERED_QUANTITY" \
                , "ool.ORDERED_ITEM","ool.HEADER_ID") \
                    .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'") \
                        .filter("ool.LAST_UPDATE_DATE >= '2022-01-01'")

                    # .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'")
# .filter("ool.LAST_UPDATE_DATE >= '2022-01-01'")

# hca.printSchema()
# ooh.printSchema()
print("ready to join")
# Join
joining_result = ooh.join(ool, "HEADER_ID") \
    .join(ot, ot["TRANSACTION_TYPE_ID"] == ooh["ORDER_TYPE_ID"]) \
        .join(ottt, "TRANSACTION_TYPE_ID") \
            .join(hca, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
                .join(hp, "party_id") \
                    .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
                        .join(inv, ool["ORDERED_ITEM"] == inv["SEGMENT1"])







# ot.join(ooh, ot["TRANSACTION_TYPE_ID"] == ooh["ORDER_TYPE_ID"]) 
    # .join(ooh, ot["TRANSACTION_TYPE_ID"] == ooh["ORDER_TYPE_ID"])

print("join successfull")
# hp.join(hca, "PARTY_ID") 
    # .join(ooh, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
    #     .join(ot, ooh["ORDER_TYPE_ID"] == ot["TRANSACTION_TYPE_ID"]) \
    #         .join(ottt, "TRANSACTION_TYPE_ID") \
    #             .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
    #                 .join(ool, "HEADER_ID") \
    #                     .join(inv, ool["ORDERED_ITEM"] == inv["SEGMENT1"])



# ooh.join(ool, "HEADER_ID") \
#     .join(ot, ooh["ORDER_TYPE_ID"] == ot["TRANSACTION_TYPE_ID"]) \
#         .join(ottt, ot["TRANSACTION_TYPE_ID"] == ottt["TRANSACTION_TYPE_ID"])
# 
# joining_result =  ot.join(ottt, "TRANSACTION_TYPE_ID") 

# hca.join(ooh, func.round(hca["CUST_ACCOUNT_ID"]) == func.round(ooh["SOLD_TO_ORG_ID"])) \


# hp.join(hca, "party_id") \

# ooh.join(ool, "HEADER_ID") \
#                         .select("HEADER_ID", "FLOW_STATUS_CODE")

#     .join(ot, ooh["ORDER_TYPE_ID"] == ot["TRANSACTION_TYPE_ID"]) \
#         .join(ottt, ot["TRANSACTION_TYPE_ID"] == ottt["TRANSACTION_TYPE_ID"]) \
#             .join(hca, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
#                 .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
#                     .join(hp, hca["party_id"] == hp["party_id"])
# print("ready to write on console")
# query = joining_result \
#     .writeStream \
#     .format("console") \
#     .start().awaitTermination()

print("start to write")

database = "STCC"
table = "dbo.device_sales_tables_new"
user = "SA"
password  = "MhffPOC2022"
# intvl = 0
def writesql(dff, epoch_id):
    dff.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    print("Iteration ")
    # intvl+=1

print("after iteration")
query = joining_result.writeStream.outputMode("append").foreachBatch(writesql).start()
query.awaitTermination()

# .trigger(processingTime='60 seconds')



    # .option("mode", "DROPMALFORMED") \
