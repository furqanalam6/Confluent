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

OE_TRANSACTION_TYPES_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_TRANSACTION_TYPES_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/opt/Schemas/schemas/oe_transaction_types_all.json','r') as f:
  schema_oe_all = f.read()

ot = OE_TRANSACTION_TYPES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), \
schema_oe_all).alias("person")) \
        .select("person.*").filter("ot.ATTRIBUTE6 == 'Saleco Retail'")


query = ot \
    .writeStream \
    .format("console") \
    .option("mode", "update") \
    .start().awaitTermination() 



val jsonFormatSchema = new String(
Files.readAllBytes(Paths.get("./src/main/resources/person.avsc")))


ot = OE_TRANSACTION_TYPES_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_all).alias("ot")) \
        .select("ot.*")




