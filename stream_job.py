from ntpath import join
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

with open('/opt/Confluent/schemas/hz_parties.json','r') as f:
  schema_HZP = f.read()

with open('/opt/Confluent/schemas/hz_cust_accounts.json','r') as f:
  schema_HZC = f.read()

with open('/opt/Confluent/schemas/hr_all_organization_units.json','r') as f:
  schema_hr = f.read()

with open('/opt/Confluent/schemas/oe_transaction_types_all.json','r') as f:
  schema_oe_all = f.read()

with open('/opt/Confluent/schemas/oe_transaction_types_tl.json','r') as f:
  schema_oe_tl = f.read()

with open('/opt/Confluent/schemas/oe_order_headers_all.json','r') as f:
  schema_oe_headers_all = f.read()

with open('/opt/Confluent/schemas/oe_order_lines_all.json','r') as f:
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

ot = OE_TRANSACTION_TYPES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_all).alias("ot")) \
        .select("ot.TRANSACTION_TYPE_ID", "ot.ATTRIBUTE2", "ot.ATTRIBUTE6")

ottt = OE_TRANSACTION_TYPES_TL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_tl).alias("ottt")) \
        .select("ottt.TRANSACTION_TYPE_ID", "ottt.LANGUAGE") \
            .filter("ottt.LANGUAGE = 'US'").filter("ottt.TRANSACTION_TYPE_ID == 1226.0")

ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE").filter("ooh.FLOW_STATUS_CODE = 'CLOSED'").filter("ooh.SOLD_TO_ORG_ID= 132778.0000000000")
            # ,"ooh.FLOW_STATUS_CODE").filter("ooh.FLOW_STATUS_CODE == 'CLOSED'")
            # .filter("ooh.HEADER_ID == 1669.0")
            # .filter( "ooh.ORDERED_DATE = '2022-01-01 09:23:34'")

            # .filter("ooh.HEADER_ID == 1669")

ool = OE_ORDER_LINES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_lines_all).alias("ool")) \
        .select("ool.HEADER_ID", "ool.FLOW_STATUS_CODE") \
                    .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'")

# hca.printSchema()
# ooh.printSchema()

# Join
joining_result = ot.join(ottt, "TRANSACTION_TYPE_ID") 

# hca.join(ooh, func.round(hca["CUST_ACCOUNT_ID"]) == func.round(ooh["SOLD_TO_ORG_ID"])) \


# hp.join(hca, "party_id") \

# ooh.join(ool, "HEADER_ID") \
#                         .select("HEADER_ID", "FLOW_STATUS_CODE")

    # .join(ot, ooh["ORDER_TYPE_ID"] == ot["TRANSACTION_TYPE_ID"]) \
    #     .join(ottt, ot["TRANSACTION_TYPE_ID"] == ottt["TRANSACTION_TYPE_ID"]) \
    #         .join(hca, hca["CUST_ACCOUNT_ID"] == ooh["SOLD_TO_ORG_ID"]) \
    #             .join(haou, ooh["SHIP_FROM_ORG_ID"] == haou["ORGANIZATION_ID"]) \
                    # .join(hp, hca["party_id"] == hp["party_id"])

query = ot \
    .writeStream \
    .format("console") \
    .start().awaitTermination()

# database = "STCC"
# table = "dbo.complex_query"
# user = "SA"
# password  = "MhffPOC2022"

# def writesql(dff, epoch_id):
#     dff.write.mode("overwrite") \
#         .format("jdbc") \
#         .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
#         .option("dbtable", table) \
#         .option("user", user) \
#         .option("password", password) \
#         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#         .save()

# query = ot.writeStream.outputMode("append").foreachBatch(writesql).start()
# query.awaitTermination()



