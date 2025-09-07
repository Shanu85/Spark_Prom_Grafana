from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKERS="localhost:29092,localhost:39092,localhost:49092"
SOURCE_TOPIC="financial_transactions"
AGGREGATE_TOPIC="transactions_aggregate"
ANOMALIES_TOPIC="transactions_anomalies"
CHECKPOINT_DIR="directory_local_path"
STATES_DIR="directory_local_path"

spark=(SparkSession.builder
       .appName('FinancialTransactionProcessor')
       .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0')
       .config('spark.sql.streaming.checkpointLocation',f'{CHECKPOINT_DIR}/raw')
       .config('spark.sql.streaming.stateStore.stateStoreDir',STATES_DIR)
       .config('spark.sql.shuffle.partitions',20)
       ).getOrCreate()

transaction_schema = StructType([
    StructField('transactionId',StringType(),True),
    StructField( 'userId', StringType(), True),
    StructField(  'merchantId', StringType(), True),
    StructField(  'amount', DoubleType(), True),
    StructField( 'transactionTime', LongType(), True),
    StructField(  'transactionType', StringType(), True),
    StructField(  'location', StringType(), True),
    StructField(  'paymentMethod', StringType(), True),
    StructField( 'isInternational', StringType(), True),
    StructField( 'currency', StringType(), True)
])

kafka_stream=(spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers',KAFKA_BROKERS)
              .option('subscribe',SOURCE_TOPIC)
              .option('startingOffsets','earliest') # once done processing the log change it to latest otherwise it will again process all the log
              ).load()

transaction_df=kafka_stream.selectExpr("CAST(VALUE AS STRING)")\
    .select(from_json(col('value'),transaction_schema).alias('data'))\
    .select('data.*')


transaction_df=transaction_df.withColumn('transactionTimeStamp',(col('transactionTime')/1000).cast('timestamp'))

# aggregate on merchantID
aggregate_df=transaction_df.groupby(['merchantId']).agg(sum(col('amount')).alias('total_amount'),count('*').alias('transactionCount'))

aggregate_query = aggregate_df \
    .withColumn('key', col('merchantId').cast('string')) \
    .withColumn('value', to_json(struct(col('merchantId'),col('total_amount'),col('transactionCount')))) \
    .selectExpr("key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", AGGREGATE_TOPIC) \
    .option("checkpointLocation", f'{CHECKPOINT_DIR}/checkpoint') \
    .outputMode("update") \
    .start()

print("Streaming query started. Press Ctrl+C to stop.")

# Await termination of the query
aggregate_query.awaitTermination()




