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

OE_ORDER_HEADERS_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_ORDER_HEADERS_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/opt/Confluent/schemas/oe_order_headers_all.json','r') as f:
  schema_oe_headers_all = f.read()

ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE","ooh.FLOW_STATUS_CODE").filter("ooh.FLOW_STATUS_CODE == 'CLOSED'")

query = ooh \
    .writeStream \
    .format("console") \
    .start().awaitTermination()

