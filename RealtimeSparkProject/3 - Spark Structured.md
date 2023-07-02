
## Spark Streaming

#### Start Hadoop
`dfs-

```Py
sampleDF.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/emp") \
  .option("dbtable", "employee") \
  .option("user", "root") \
  .option("password", "root") \
  .save()
```


```py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("Begin")

kafka_topic_name = "orderstopic"
kafka_bootstrap_servers = 'localhost:9092'
customers_data_file_path = "/home/datamaking/workarea/data/customers.csv"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class="com.mysql.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "root"
mysql_password = "datamaking"
mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no+"/"+ mysql_database_name + "?useSSL=false"

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sales_ks"
cassandra_table_name = "orders_tbl"


def save_to_cassandra (current_df, epoc_id):
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing before Cassandra table save:" + str(epoc_id))
    current_df \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=cassandra_table_name, keyspace=cassandra_keyspace_name) \
        .save()
    print("Printing before Cassandra table save:" + str(epoc_id))


def save_to_mysql (current_df, epoch_id):
    db_credentials = {"user": mysql_user_name,
                        "password": mysql_password,
                        "driver": mysql_driver_class}

    print("Printing epoc_id: ")
    print(epoch_id)
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final = current_df \
    .withColumn("processed_at", lit(processed_at)) \
    .withColumn("batch_id", lit(epoch_id) )


    # current_df_final \
    #     .write.mode("append") \
    #     .format("jdbc") \
    #     .option("driver",mysql_driver_class) \
    #     .option("url", mysql_jdbc_url) \
    #     .option("dbtable", mysql_table_name) \
    #     .option("user", mysql_user_name) \
    #     .option("password", mysql_password) \
    #     .save()

    print("Before MYSQL Table save",str(epoch_id))

    current_df_final \
    .write \
    .jdbc(url=mysql_jdbc_url,
          table=mysql_table_name,
          mode="append",
          properties=db_credentials
          )

    print("After MYSQL Table save", str(epoch_id))


## jars
##

jars_location = r"file:///home/datamaking/workarea/jars/"
jar_jsr = jars_location + r"jsr305-3.0.0.jar"
jar_kafka_client = jars_location + r"kafka-clients-3.2.0.jar"
jar_sql_kafka = jars_location + r"spark-sql-kafka-0-10_2.12-3.3.0.jar"
jar_cassandra = jars_location + r"spark-cassandra-connector_2.12-3.3.0.jar"
jar_mysql = r"file:///usr/share/java/mysql-connector-java.jar"
jar_commons_pool = jars_location + r"commons-pool2-2.11.1.jar"
jar_kafka_assembly = jars_location + r"spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar"
jar_cassandra_assembly = jars_location + r"spark-cassandra-connector-assembly_2.12-3.3.0.jar"

jars_comma_sep = jar_jsr + "," + jar_kafka_client + "," + jar_cassandra_assembly + "," + jar_kafka_assembly + "," + jar_commons_pool + "," + jar_mysql + "," + jar_sql_kafka + "," + jar_cassandra
jars_colon_sep = jar_jsr + ":" + jar_kafka_client + ":" + jar_cassandra_assembly + ":" + jar_kafka_assembly + ":" + jar_commons_pool + ":" + jar_mysql + "," + jar_sql_kafka + ":" + jar_cassandra


## if __name__ == "_main_":
if True:
    print("Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    spark = SparkSession \
            .builder \
            .appName("PySpark Structured Streaming with Kafka and Cassandra") \
            .master("local") \
            .config("spark.jars", jars_comma_sep) \
            .config("spark.executor.extraClassPath", jars_colon_sep) \
            .config("spark.executor.extraLibrary", jars_colon_sep) \
            .config("spark.driver.extraClassPath", jars_colon_sep) \
            .config('spark.cassandra.connection.host', cassandra_host_name) \
            .config('spark.cassandra.connection.port', cassandra_port_no) \
            .getOrCreate()

    ## Add path to Cassandra and Kafka Jar files to spark app at four instances
    ## In Spark Jars COnfig option , the jars are separated by ',' otherwise they are separated by :
    ## For Kafka< we need Spark Kafka Jar File and Kafka Clients Jar>
    ## Video 8 Time 7.53

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from orderstopic
    orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    #orders_df.writeStream.format("console").start()
        #.options(truncate=False).start()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # orders_df1.writeStream.format("console").options(truncate=False).start()
    #
    # Define a schema for the orders data
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("created_at", StringType()) \
        .add("discount", StringType()) \
        .add("product_id", StringType()) \
        .add("quantity", StringType()) \
        .add("subtotal", StringType()) \
        .add("tax", StringType()) \
        .add("total", StringType()) \
        .add("customer_id", StringType())

    orders_df2 = orders_df1 \
        .select( from_json(col("value"), orders_schema).alias("orders"), "timestamp" )

    # orders_df2.writeStream.format("console").options(truncate=False).start()

    orders_df3 = orders_df2.select("orders.*", "timestamp")

    ##

    orders_df3 \
    .writeStream\
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .foreachBatch(save_to_cassandra) \
    .start()

    customers_df = spark.read.csv(customers_data_file_path, header=True, inferSchema=True)
    customers_df.printSchema()
    customers_df.show(5, False)

    orders_df4 = orders_df3.join(customers_df, orders_df3.customer_id == customers_df.ID, how = 'inner')

    ## Extremely useful feature in SparkStreaming, doing a join on STreaming data (unbounded data) and data in hdfs (bounded data)

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    #Simple aggregate - find total_sum_amount by grouping source, state

    orders_df5 = orders_df4.groupBy("source", "state") \
        .agg({'total': 'sum'}).select("source", "state", col("sum(total)").alias ("total_sum_amount"))

    print("Printing Schema of orders_df5: ")
    orders_df5.printSchema()

    #Write final result into console for debugging purpose
    trans_detail_write_stream = orders_df5 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .option("truncate","false") \
        .format("console") \
        .start()

    orders_df5 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .foreachBatch(save_to_mysql) \
        .start()

    trans_detail_write_stream.awaitTermination()

    print("Completed")

```