package spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Spark_kafka_comsumer {
  def main(args: Array[String]): Unit = {

    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()
    val ssc = new StreamingContext(sparkconf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.print()
    stream.count().print()


    ssc.start()
    ssc.awaitTermination()


  }
}
