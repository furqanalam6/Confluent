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

OE_ORDER_LINES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_ORDER_LINES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/opt/Schemas/schemas/oe_order_lines_all.json','r') as f:
  schema_oe_lines_all = f.read()

ool = OE_ORDER_LINES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_lines_all).alias("ool")) \
        .select("ool.ORDERED_ITEM")
        
        # , "ool.LAST_UPDATE_DATE", "ool.LINE_CATEGORY_CODE" \
        #     ,  "ool.UNIT_LIST_PRICE", "ool.INVENTORY_ITEM_ID" \
        #         , "ool.SHIP_FROM_ORG_ID", "ool.ORDERED_ITEM","ool.HEADER_ID", "ool.FLOW_STATUS_CODE") \
        #             .filter("ool.FLOW_STATUS_CODE  = 'CLOSED'")

query = ool \
    .writeStream \
    .format("console") \
    .option("mode", "update") \
    .start().awaitTermination()

