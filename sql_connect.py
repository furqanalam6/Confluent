#import required modules
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
 
#Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")
 
#Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
#set variable to be used to connect the database
database = "stcc"
table = "dbo.device_sales_table"
user = "SA"
password  = "MhffPOC2022"
 
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

# db_target_url = "jdbc:sqlserver://10.92.26.184:1433;TestDB"
# db_target_properties = {"user":"SA", "password":"MhffPOC2022"}

# def writesql(dff, epoch_id):
#     dfwrite = dff.write.mode("overwrite") \
#     .format("jdbc") \
#     .option("url", f"jdbc:sqlserver://10.92.26.184:1433;databaseName={database};") \
#     .option("dbtable", table) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#     .save()

#     dfwrite.jdbc(url=db_target_url, table="PARTY", properties=db_target_properties) # if this is not working use below
#     #df.write.jdbc(url=jdbcurl, table=table_name, properties=db_properties, mode="append")
#     pass


# query = joining_result.writeStream.outputMode("append").foreachBatch(writesql).start()
# query.awaitTermination()