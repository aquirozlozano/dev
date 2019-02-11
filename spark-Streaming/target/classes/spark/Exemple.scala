package spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Exemple {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exemple")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2)) //this line throws error

  }

}
