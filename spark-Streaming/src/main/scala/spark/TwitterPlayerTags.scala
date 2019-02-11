package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit, regexp_replace}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime


object TwitterPlayerTags {


  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Minutes(30))
    val stream = TwitterUtils.createStream(ssc, None,filters)

    val spark = new SparkSession
    .Builder()
      .config(sparkConf)
      //.appName("Twitter Tags")
      //.enableHiveSupport()
      .getOrCreate()


    val playerNames=obtenerDatos(spark)

    val names=playerNames.select(regexp_replace(col("player_name")," ","").alias("player_name"))

    val df2 = names.withColumn("player_name", concat(lit("#"), col("player_name")))

    import spark.implicits._
    val hashNames =df2.map(r => r.getString(0)).collect.toList



    //val fruit = Array("#NCT127", "#NCT", "#NCT127_1stTour")

    val hashTags=stream.flatMap(status=>status.getText.split(" ").filter(value=>hashNames.contains(value)))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Minutes(30))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    var countTags:Unit=""
    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 30 Minutes (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s ,%s".format(tag, count))}

    })

    topCounts60.countByValue()
      .foreachRDD {rdd =>
        val now = DateTime.now()
        rdd
          .sortBy(_._2)
          .map(x => (x, now))
          .saveAsTextFile("/home/cloudera/Desktop/DataOut/Resultados")
      }

    ssc.start()
    ssc.awaitTermination()

  }


  private def obtenerDatos(sqlContext: SparkSession) = {
    val path = "/home/cloudera/Desktop/DataSets/Player.csv"
    val playernames = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load(path)
    playernames

  }


}
