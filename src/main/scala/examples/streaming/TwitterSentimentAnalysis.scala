package examples.streaming

import java.util.Properties

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import scala.util.Try

object TwitterSentimentAnalysis35 extends App {

  val logger = LogManager.getLogger(getClass)

  val pgURL = Try(args(0)).getOrElse("jdbc:postgresql://localhost:5432/postgres")
  val kafkaBootstrap = Try(args(1)).getOrElse("localhost:9092")
  val kafkaTopic = Try(args(2)).getOrElse("tweets")
  val windowSecs = Try(args(3).toInt).getOrElse(30)
  val hdfsBasePath = Try(args(4)).getOrElse("hdfs://localhost:8020/tmp/streaming")

  val spark = SparkSession.builder()
    .appName("TwitterSentimentAnalysis35")
    .master("local[*]")   // quitar si vas a cluster
    .getOrCreate()

  import spark.implicits._

  logger.warn(s"pgURL=$pgURL")
  logger.warn(s"kafkaBootstrap=$kafkaBootstrap")
  logger.warn(s"kafkaTopic=$kafkaTopic")
  logger.warn(s"windowSecs=$windowSecs")
  logger.warn(s"hdfsBasePath=$hdfsBasePath")

  // Esquema esperado en Kafka
  val tweetSchema = StructType(Seq(
    StructField("source", StringType, nullable = true),
    StructField("hashtags", ArrayType(StringType), nullable = true),
    StructField("text", StringType, nullable = true)
  ))

  // Lectura desde Kafka
  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrap)
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "latest")
    .load()

  val tweets = kafkaDF
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json($"json_value", tweetSchema).as("tweet"))
    .select("tweet.*")
    .withColumn("hashtags", coalesce($"hashtags", array()))
    .withColumn("text", coalesce($"text", lit("")))
    .withColumn("source", coalesce($"source", lit("")))

  val query = tweets.writeStream
    .trigger(Trigger.ProcessingTime(s"${windowSecs} seconds"))
    .outputMode("append")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      logger.warn(s">>> New batch: $batchId")

      val jdbcProperties = new Properties()
      jdbcProperties.setProperty("user", "postgres")
      jdbcProperties.setProperty("password", "postgres")

      val pgHashtags = batchDF.sparkSession.read
        .jdbc(pgURL, "public.hashtags", jdbcProperties)

      val keywords = pgHashtags.collect().map(_.getString(0).toLowerCase).toSeq

      logger.warn(s">>> Looking up tweets with keywords: ${keywords.mkString(",")}")

      if (keywords.nonEmpty) {
        val relevantTweets = batchDF
          .withColumn(
            "source_clean",
            when(
              regexp_extract($"source", ">([^<]+)<", 1) =!= "",
              regexp_extract($"source", ">([^<]+)<", 1)
            ).otherwise($"source")
          )
          .withColumn("hashtags_lower", transform($"hashtags", h => lower(h)))
          .filter(size(array_intersect($"hashtags_lower", typedLit(keywords))) > 0)
          .select(
            $"source_clean".as("source"),
            concat_ws(" ", $"hashtags_lower").as("hashtags"),
            $"text"
          )

        relevantTweets.cache()

        val iphoneTweets = relevantTweets.filter(lower($"source").contains("iphone"))
        val iphoneCount = iphoneTweets.count()
        logger.warn(s">>> Saving $iphoneCount tweets from iphone sources")

        iphoneTweets.write
          .mode(SaveMode.Append)
          .parquet(s"$hdfsBasePath/iphone")

        val otherSourcesTweets = relevantTweets.filter(not(lower($"source").contains("iphone")))
        val otherCount = otherSourcesTweets.count()
        logger.warn(s">>> Saving $otherCount tweets from other sources")

        otherSourcesTweets.write
          .mode(SaveMode.Append)
          .parquet(s"$hdfsBasePath/other")

        relevantTweets.unpersist()
      } else {
        logger.warn(">>> No keywords found in PostgreSQL table public.hashtags")
      }

      ()
    }.start()

  query.awaitTermination()
}