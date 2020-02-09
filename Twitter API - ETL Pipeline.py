# Databricks notebook source
# MAGIC %run "./Includes/Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define and Applying a Schema
# MAGIC 
# MAGIC Capture the hashtags and dates from the data to get a sense for Twitter trends.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ER-diagram.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

from pyspark.sql.types import * 
path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"

fullTweetSchema = StructType([
  StructField("id", LongType(), True),
  StructField("user", StructType([
    StructField("id", LongType(), True),
    StructField("screen_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("friends_count", IntegerType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("description", StringType(), True)
  ]), True),
  StructField("entities", StructType([
    StructField("hashtags", ArrayType(
      StructType([
        StructField("text", StringType(), True)
      ]),
    ), True),
    StructField("urls", ArrayType(
      StructType([
        StructField("url", StringType(), True),
        StructField("expanded_url", StringType(), True),
        StructField("display_url", StringType(), True)
      ]),
    ), True)
  ]), True),
  StructField("lang", StringType(), True),
  StructField("text", StringType(), True),
  StructField("created_at", StringType(), True)
])

fullTweetDF = (spark.read
           .schema(fullTweetSchema)
           .json(path)
)
display(fullTweetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Tables
# MAGIC 
# MAGIC Apply the schema you defined to create tables that match the relational data model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Nulls from dataset
# MAGIC 
# MAGIC The Twitter data contains both deletions and tweets.  This is why some records appear as null values. Create a DataFrame called `fullTweetFilteredDF` that filters out the null values.

# COMMAND ----------

from pyspark.sql.functions import col
fullTweetFilteredDF = fullTweetDF.na.drop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### `Tweet` Table

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp
timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

tweetDF = fullTweetFilteredDF.select(col('id').alias('tweetId'),
                                     col('user.id').alias("userId"), 
                                     'lang', 
                                     'text', 
                                     unix_timestamp('created_at', timestampFormat).cast(TimestampType()).alias("createdAt"))
display(tweetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Account` Table

# COMMAND ----------


accountDF = fullTweetFilteredDF.select(col("user.id").alias("userId"),
                                      col("user.screen_name").alias("screenName"),
                                      col("user.location").alias("location"),
                                      col("user.friends_count").alias("friendsCount"),
                                      col("user.followers_count").alias("followersCount"),
                                      col("user.description").alias("description"))

display(accountDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creating `Hashtag` and `URL` Tables 

# COMMAND ----------

fullTweetFilteredDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode
hashtagDF = fullTweetFilteredDF.select(col('id').alias('tweetID'), 
                                       explode(col('entities.hashtags.text')).alias('hashtag'))
display(hashtagDF)


# COMMAND ----------

urlDF = fullTweetFilteredDF.select(col('id').alias('tweetID'), 
                                  explode(col('entities.urls.url')).alias('URL'),
                                  col('entities.urls.expanded_url').alias('expandedURL'),
                                  col('entities.urls.display_url').alias('displayURL'))


display(urlDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Loading the Results
# MAGIC 
# MAGIC Use DBFS as our target warehouse for your transformed data. Save the DataFrames in Parquet format to the following endpoints:  
# MAGIC 
# MAGIC | DataFrame    | Endpoint                                 |
# MAGIC |:-------------|:-----------------------------------------|
# MAGIC | `accountDF`  | `"/tmp/" + username + "/account.parquet"`|
# MAGIC | `tweetDF`    | `"/tmp/" + username + "/tweet.parquet"`  |
# MAGIC | `hashtagDF`  | `"/tmp/" + username + "/hashtag.parquet"`|
# MAGIC | `urlDF`      | `"/tmp/" + username + "/url.parquet"`    |

# COMMAND ----------

accountLocation = "/tmp/" + username + "/account.parquet"
tweetLocation = "/tmp/" + username + "/tweet.parquet"
hashtagLocation = "/tmp/" + username + "/hashtag.parquet"
urlLocation = "/tmp/" + username + "/url.parquet"

accountDF.write.parquet(accountLocation)
tweetDF.write.parquet(tweetLocation)
hashtagDF.write.parquet(hashtagLocation)
urlDF.write.parquet(urlLocation)


# COMMAND ----------

# MAGIC %run ./Includes/Cleanup