from pyspark.sql import {SparkSession, SaveMode, Row, DataFrame}
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
# from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath","/opt/bitnami/spark/jars/mssql-jdbc-6.4.0.jre8.jar")\
    .appName("STREAM") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

HZ_PARTIES = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.AR.HZ_PARTIES") \
    .option("startingOffsets", "earliest") \
    .load()

HZ_CUST_ACCOUNTS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.AR.HZ_CUST_ACCOUNTS") \
    .option("startingOffsets", "earliest") \
    .load()

HR_ALL_ORGANIZATION_UNITS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.HR.HR_ALL_ORGANIZATION_UNITS") \
    .option("startingOffsets", "earliest") \
    .load()

MTL_SYSTEM_ITEMS_B = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.INV.MTL_SYSTEM_ITEMS_B") \
    .option("startingOffsets", "earliest") \
    .load()

OE_TRANSACTION_TYPES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_TRANSACTION_TYPES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_TRANSACTION_TYPES_TL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_TRANSACTION_TYPES_TL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_ORDER_HEADERS_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_ORDER_HEADERS_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

OE_ORDER_LINES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_ORDER_LINES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/opt/Schemas/schemas/hz_parties.json','r') as f:
  schema_HZP = f.read()

with open('/opt/Schemas/schemas/hz_cust_accounts.json','r') as f:
  schema_HZC = f.read()

with open('/opt/Schemas/schemas/hr_all_organization_units.json','r') as f:
  schema_hr = f.read()

with open('/opt/Schemas/schemas/mtl_system_items_b.json','r') as f:
  schema_inv = f.read()

with open('/opt/Schemas/schemas/oe_transaction_types_all.json','r') as f:
  schema_oe_all = f.read()

with open('/opt/Schemas/schemas/oe_transaction_types_tl.json','r') as f:
  schema_oe_tl = f.read()

with open('/opt/Schemas/schemas/oe_order_headers_all.json','r') as f:
  schema_oe_headers_all = f.read()

with open('/opt/Schemas/schemas/oe_order_lines_all.json','r') as f:
  schema_oe_lines_all = f.read()

hp = HZ_PARTIES.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_HZP).alias("hp")) \
       .select("hp.PARTY_ID", "hp.PARTY_NAME")

hca = HZ_CUST_ACCOUNTS.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_HZC).alias("hca")) \
      .select("hca.PARTY_ID", "hca.CUST_ACCOUNT_ID")

haou = HR_ALL_ORGANIZATION_UNITS.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_hr).alias("haou")) \
        .select("haou.LOCATION_ID", "haou.BUSINESS_GROUP_ID", "haou.ORGANIZATION_ID") \
            .filter("haou.BUSINESS_GROUP_ID = 101")

inv = MTL_SYSTEM_ITEMS_B.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_inv).alias("inv")) \
        .select("inv.DESCRIPTION", "inv.SEGMENT1", "inv.ORGANIZATION_ID") \
            .filter("inv.ORGANIZATION_ID = 105")

ot = OE_TRANSACTION_TYPES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_all).alias("ot")) \
        .select("ot.ATTRIBUTE6", "ot.ATTRIBUTE2", "ot.TRANSACTION_TYPE_ID")

ottt = OE_TRANSACTION_TYPES_TL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_tl).alias("ottt")) \
        .select("ottt.TRANSACTION_TYPE_ID", "ottt.LANGUAGE") \
            .filter("ottt.LANGUAGE = 'US'")

ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE")



ool = OE_ORDER_LINES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_lines_all).alias("ool")) \
        .select("ool.*").filter("ool.FLOW_STATUS_CODE  = 'CLOSED'")
        
        , "ool.LAST_UPDATE_DATE", "ool.LINE_CATEGORY_CODE" \
            ,  "ool.UNIT_LIST_PRICE", "ool.INVENTORY_ITEM_ID" \
                , "ool.SHIP_FROM_ORG_ID", "ool.ORDERED_ITEM","ool.HEADER_ID", "ool.FLOW_STATUS_CODE", "ool.LAST_UPDATE_DATE") \
                    .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'")





# Join

joining_result = ooh.join(ool, "HEADER_ID") \
    .join(ot, ooh["ORDER_TYPE_ID"] == ot["TRANSACTION_TYPE_ID"]) \
        .join(ottt, ot["TRANSACTION_TYPE_ID"] == ottt["TRANSACTION_TYPE_ID"]) \
            .join(hca, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
                .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
                    .join(hp, hca["party_id"] == hp["party_id"]) \
                            .join(inv, ool["ordered_item"] == inv["segment1"])

query = ool \
    .writeStream \
    .format("console") \
    .option("mode", "update") \
    .start().awaitTermination()





joining_result = hp.join(hca, hca["party_id"] == hp["party_id"])

# # query = df.writeStream.outputMode("append").foreachBatch( (batchDF: DataFrame, batchId: Long) => 
# #           batchDF.write
# #            .format("com.microsoft.sqlserver.jdbc.spark")
# #            .mode("append")
# #             .option("url", "jdbc:sqlserver://10.92.26.184:1433")
# #             .option("dbtable", TestDB)
# #             .option("user", SA)
# #             .option("password", MhffPOC2022)
# #             .save()
# #          ).start()

# # query.awaitTermination(40000)
# # query.stop() 
# # db_target_url = "jdbc:mysql://localhost/database"


def process_row(dff, epoch_id):
    dff.write.jdbc(url=db_target_url, table="PARTY", mode="append", properties=db_target_properties)
    pass


query = joining_result.writeStream.foreachBatch(process_row).start()


# def foreach_batch_function(df, epoch_id):
#     df.format("jdbc").option("url", db_target_url)\
#       .option("dbtable","PARTY").option("user","SA")\
#       .option("password", "MhffPOC2022").save()
  
# # df_hz_parties.rdd.writeStream.foreachBatch(foreach_batch_function).start()
# HZ_PARTIES.writeStream\
#         .format("kafka")\
#         .option("kafka.bootstrap.servers", "10.92.26.188:29093")\
#         .option("checkpointLocation", "checkpoint")\
#         .option("topic", "end")\
#         .start().awaitTermination()


# # from pyspark.sql.functions import *
# # from pyspark.sql import *

# def writeToSQLWarehouse(df, epochId):
#   df.write \
#     .format("com.databricks.spark.sqldw") \
#     .mode('overwrite') \
#     .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
#     .option("forward_spark_azure_storage_credentials", "true") \
#     .option("dbtable", "my_table_in_dw_copy") \
#     .option("tempdir", "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>") \
#     .save()

# spark.conf.set("spark.sql.shuffle.partitions", "1")

# query = (
#   spark.readStream.format("rate").load()
#     .selectExpr("value % 10 as key")
#     .groupBy("key")
#     .count()
#     .toDF("key", "count")
#     .writeStream
#     .foreachBatch(writeToSQLWarehouse)
#     .outputMode("update")
#     .start()
#     )

jdbcDF = spark.read\
        .option("driver" , "com.microsoft.sqlserver.jdbc.spark")\
        .option("url", "jdbc:sqlserver://10.92.26.184:1433;TestDB")\
        .option("dbtable", "PARTY")\
        .option("user", "SA")\
        .option("password", "MhffPOC2022").load()


db_target_url = "jdbc:sqlserver://10.92.26.184:1433;TestDB"
db_target_properties = {"user":"SA", "password":"MhffPOC2022"}

def writesql(dff, epoch_id):
    dfwrite = dff.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

    dfwrite.jdbc(url=db_target_url, table="PARTY", properties=db_target_properties) # if this is not working use below
    #df.write.jdbc(url=jdbcurl, table=table_name, properties=db_properties, mode="append")
    pass


query = joining_result.writeStream.outputMode("append").foreachBatch(writesql).start()
query.awaitTermination()