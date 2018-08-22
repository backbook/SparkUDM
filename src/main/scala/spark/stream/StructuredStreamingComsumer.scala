package spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StructuredStreamingComsumer {
  def main(args: Array[String]): Unit = {

    kafkaSpakrk()
  }
  def  kafkaSpakrk(): Unit ={

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val streamingContext = new StreamingContext(conf, Seconds(1))



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test7",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)

    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def ncSpark(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("node1", 9999)

    val words = lines.flatMap(_.split(" "))

    //    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
