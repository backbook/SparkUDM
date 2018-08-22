package spark.stream

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._

object Kafka_Consumer_scala {

  def main(args: Array[String]): Unit = {

    val prop = new Properties
    //这里不是配置zookeeper了，这个是配置bootstrap.servers
    prop.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    //这个配置是让消费端进行消费时候不自动提交offset,避免了kafka
    prop.put("enable.auto.commit", "false")
    //这个配置组，之前我记得好像不配置可以，现在如果不配置那么不能运行
    prop.put("group.id", "test_cluster")
    //序列化
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer= new KafkaConsumer[String, String](prop)
    consumer.subscribe(Collections.singleton("test"))
    while (true) { //这里是得到ConsumerRecords实例
      val records = consumer.poll(100)
      for (record <- records) { //这个有点好，可以直接通过record.offset()得到offset的值
        println( record.value() )

      }
    }
  }
}
