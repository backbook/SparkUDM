package spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.sparkUtils.getSparkConf


/**
  * @author 76886049@qq.com sujinyang   date: 2018/6/8 9:46  computer: backbook
  */
object SparkSteamingConsumer {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
   val sparksession: SparkSession = SparkSession.builder().config(getSparkConf()).getOrCreate()
  }
}
