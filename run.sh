#!/bin/bash
cd /Confluent 
docker cp /home/poc/Confluent/ spark_spark-master_1:/opt
docker exec -it spark_spark-master_1 /bin/bash spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1,org.apache.spark:spark-sql_2.12:3.2.1 --driver-memory 14g --executor-memory 31g --executor-cores 5 --num-executors 2 --conf spark.sql.avro.datetimeRebaseModeInRead=CORRECTED /opt/Confluent/stream_job.py