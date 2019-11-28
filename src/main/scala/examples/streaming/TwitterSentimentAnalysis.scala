package examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterSentimentAnalysis extends App with Logging {

  val consumerKey = "ATqx5VZzXUszXXRnLphHBng33"
  val consumerSecret = "NfrxW56H5radSSBO4wy0P24XDvkypO4vMihXIkN1tVx5Xo3Fb0"
  val accessToken = "29101445-L15aZeWpGe04iKp8x29mj9YNidOh9F6SAfEU7qvOJ"
  val accessTokenSecret = "BFH3sNuOtqysPf2xpMKL3DBqnr3MMmK3XPflhC21077i4"

  val filters: Array[String] = Array.empty[String]

  // Set the system properties so that Twitter4j library used by Twitter stream
  // can use them to generate OAuth credentials
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterSentimentAnalysis")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))
  val stream = TwitterUtils.createStream(ssc, None, filters)

  val tweets = stream.filter(_.getHashtagEntities.nonEmpty).map{ status =>
    val source = status.getSource
    (source.drop(source.indexOf(">")).stripPrefix(">").stripSuffix("</a>"), status.getHashtagEntities.map(_.getText.toLowerCase), status.getText)
  }

  //val keywords = sparkSession.read.parquet("file:///tmp/keywords.csv").select("word").collect().map(_.getString(0))
  //val test = sparkSession.read.parquet("file:///tmp/keywords.csv").select("word")
  val keywords = Array("global","warming","pollution","earth","temperature","increase","weather","change","biosphere",
    "hydrosphere","lithosphere","solar","radiation","ice","core","fossil","fuel","depolarization","emissions",
    "climate","co2","air","quality","dust","carbondioxide","greenhouse","ozone","methane","sealevel","sea","level",
    "TheApprentice","MCCelebrity","LIVNAP","news", "MasterChefCelebrity", "istandwithmelissa", "bigdata", "spark",
    "streaming", "kafka", "usa").map(_.toLowerCase)

  val relevantTweets = tweets.filter{ tweet =>
    tweet._2.intersect(keywords).length > 0
  }.map(row => (row._1, row._2.mkString(" "), row._3))

  val iphoneTweets = relevantTweets.filter(_._1.toLowerCase.contains("iphone"))
  iphoneTweets.foreachRDD{ rdd =>
    import sparkSession.implicits._
    logInfo(s"Saving ${rdd.count()} tweets from iphone sources")
    rdd.toDF("source", "hashtags", "text").write.mode(SaveMode.Append).csv("/tmp/test/iphone")
  }

  val otherSourcesTweets = relevantTweets.filter(!_._1.toLowerCase.contains("iphone"))
  otherSourcesTweets.foreachRDD{ rdd =>
    import sparkSession.implicits._
    logInfo(s"Saving ${rdd.count()} tweets from other sources")
    rdd.toDF("source", "hashtags", "text").write.mode(SaveMode.Append).csv("/tmp/test/other")
  }

  ssc.start()
  ssc.awaitTermination()

}
