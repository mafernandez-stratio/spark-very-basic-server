package examples.streaming

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.util.Try

object TwitterSentimentAnalysis extends App {

  val rootLogger = Logger.getRootLogger()

  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val twitterFile = Try(args(0)).getOrElse("/Users/miguelangelfernandezdiaz/workspace/twitter.properties")
  val pgURL = Try(args(1)).getOrElse("jdbc:postgresql://localhost:5432/postgres")
  val windowSecs: Int = Try(args(2).toInt).getOrElse(30)

  val filters: Array[String] = Array.empty[String]
  val twitterCredentials = new Properties()
  twitterCredentials.load(new FileInputStream(twitterFile))

  // Set the system properties so that Twitter4j library used by Twitter stream
  // can use them to generate OAuth credentials
  System.setProperty("twitter4j.oauth.consumerKey", twitterCredentials.getProperty("twitter-source.consumerKey"))
  System.setProperty("twitter4j.oauth.consumerSecret", twitterCredentials.getProperty("twitter-source.consumerSecret"))
  System.setProperty("twitter4j.oauth.accessToken", twitterCredentials.getProperty("twitter-source.token"))
  System.setProperty("twitter4j.oauth.accessTokenSecret", twitterCredentials.getProperty("twitter-source.tokenSecret"))

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterSentimentAnalysis")
  //val sparkConf = new SparkConf().setAppName("TwitterSentimentAnalysis")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val hadoopCfg = new Configuration()
  hadoopCfg.set("fs.defaultFS", "hdfs://localhost:9000")
  val fs = FileSystem.newInstance(hadoopCfg)
  fs.delete(new Path("/tmp/streaming/"), true)

  rootLogger.error(" >>> Starting Streaming context")
  rootLogger.error(s"twitterFile=$twitterFile")
  rootLogger.error(s"pgURL=$pgURL")
  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(windowSecs))
  val stream = TwitterUtils.createStream(ssc, None, filters)

  rootLogger.error(" >>> New data window")

  val jdbcProperties = new Properties()
  jdbcProperties.setProperty("user","postgres")
  val pgHashtags = sparkSession.read.jdbc(pgURL, "public.hashtags", jdbcProperties)
  val keywords = pgHashtags.collect().map(_.getString(0).toLowerCase)
  rootLogger.error(s" >>> Looking up tweets with keywords: ${keywords.mkString(",")}")

  val relevantTweets = stream.filter{ event =>
    event.getHashtagEntities.nonEmpty && event.getHashtagEntities.map(_.getText.toLowerCase).intersect(keywords).length > 0
  }.map{ status =>
    val source = status.getSource
    (source.drop(source.indexOf(">")).stripPrefix(">").stripSuffix("</a>"), status.getHashtagEntities.map(_.getText.toLowerCase).mkString(" "), status.getText)
  }
  relevantTweets.cache()

  val iphoneTweets = relevantTweets.filter(_._1.toLowerCase.contains("iphone"))
  iphoneTweets.foreachRDD{ rdd =>
    import sparkSession.implicits._
    rootLogger.error(s" >>> Saving ${rdd.count()} tweets from iphone sources")
    rdd.toDF("source", "hashtags", "text").write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/tmp/streaming/iphone")

    /*val df = sparkSession.read.parquet("hdfs://localhost:9000/tmp/streaming/iphone")
    rootLogger.error(s" >>> Total tweets from iphone sources: ${df.count()}")
    rootLogger.error(s" >>> Tweets from iphones:${System.lineSeparator}${df.collect().map{ row =>
      s"${row.getString(0)} | ${row.getString(1)} | ${row.getString(2)}"
    }.mkString(System.lineSeparator())}")*/
  }

  val otherSourcesTweets = relevantTweets.filter(!_._1.toLowerCase.contains("iphone"))
  otherSourcesTweets.foreachRDD{ rdd =>
    import sparkSession.implicits._
    rootLogger.error(s" >>> Saving ${rdd.count()} tweets from other sources")
    rdd.toDF("source", "hashtags", "text").write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/tmp/streaming/other")

    /*val df = sparkSession.read.parquet("hdfs://localhost:9000/tmp/streaming/other")
    rootLogger.error(s" >>> Total tweets from other sources: ${df.count()}")
    rootLogger.error(s" >>> Tweets from others:${System.lineSeparator}${df.collect().map{ row =>
      s"${row.getString(0)} | ${row.getString(1)} | ${row.getString(2)}"
    }.mkString(System.lineSeparator())}")*/
  }

  ssc.start()
  ssc.awaitTermination()

}
