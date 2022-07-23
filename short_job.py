from pyspark.sql import SparkSession
# , SaveMode, Row, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql import DataFrameWriter
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

OE_ORDER_HEADERS_ALL = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.92.26.188:29093") \
    .option("subscribe", "EBSPRE.ONT.OE_ORDER_HEADERS_ALL") \
    .option("startingOffsets", "earliest") \
    .load()

with open('/opt/Confluent/schemas/hz_parties.json','r') as f:
  schema_HZP = f.read()

with open('/opt/Confluent/schemas/oe_order_headers_all.json','r') as f:
  schema_oe_headers_all = f.read()

hp = HZ_PARTIES.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_HZP).alias("hp")) \
       .select("hp.PARTY_ID", "hp.PARTY_NAME")

ooh = OE_ORDER_HEADERS_ALL.selectExpr("substring(value, 6) as value") \
    .select(from_avro(col("value"), schema_oe_headers_all).alias("ooh")) \
        .select("ooh.HEADER_ID" ,"ooh.ORDER_TYPE_ID" ,"ooh.SHIP_FROM_ORG_ID" \
            ,"ooh.SOLD_TO_ORG_ID" ,"ooh.ORDERED_DATE","ooh.FLOW_STATUS_CODE")

query = hp \
    .writeStream \
    .format("console") \
    .start().awaitTermination()

#set variable to be used to connect the database
database = "TestDB"
table = "dbo.PARTY"
user = "SA"
password  = "MhffPOC2022"
db_target_url = "jdbc:sqlserver://10.92.26.184:1433;TestDB"
db_target_properties = {"user":"SA", "password":"MhffPOC2022"}

 
#read table data into a spark dataframe
jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
 
#show the data loaded into dataframe
jdbcDF.show()


def writesql(dff, epoch_id):
    dff.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

query = hp.writeStream.outputMode("append").foreachBatch(writesql).start()
query.awaitTermination()

#read table data into a spark dataframe
jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
 
#show the data loaded into dataframe
jdbcDF.show()


