package utils


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


class SparkBaseApplication {

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val ssc = new StreamingContext(sc, Seconds(30))
  val stream = TwitterUtils.createStream(ssc, None)

  val spark = new SparkSession
  .Builder()
    .appName("Twitter Tags")
    //.enableHiveSupport()
    .getOrCreate()

}
