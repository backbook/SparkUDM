package spark.stream

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Kafka_Producer_scala {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    //这里不是配置zookeeper了，这个是配置bootstrap.servers
    prop.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    //这个是
//    prop.put("enable.auto.commit", "false")
    //序列化
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

   for (i <- 0 to 1000){
     val iL:String = "{\"data\":{'zhima_watch_lists':[{\"biz_code\"?\"AA\",\"code\"?\"AA001001\",\"extend_info\":[{\"description\":\"???????\",\"key\":\"event_max_amt_code\",\"value\":\"M01\"},{\"description\":\"??\",\"key\":\"id\",\"value\":\"28db82bce40fdb3a96e3c6def405d077\"},{\"description\":\"????\",\"key\":\"event_end_time_desc\",\"value\":\"2016-08\"}],\"level\"=>1,\"refresh_time\":\"2018-04-1100:00:00\",\"settlement\":true,\"type\":\"AA001\"}],\"user_info\":{\"alipay_account\":\"18588888888\",\"gender\":\"2\",\"id_number\":\"2****************X\",\"phone_number\":\"185******88\",\"real_name_certification\":true,\"taobao_account\":\"呵呵\",\"user_name\":\"张三\",\"zhima_credit_score\":\"743\"}}\"transNo\":\"18041611INSUxgNS2L7c\",\"apiKey\":\"511\",\"sysSourceId\":\"万卡系统\",\"sourceUserId\":\"8374283\",\"certId\":\"2****************X\",\"realName\":\"张三\",\"timestamp\":\"1526355000000\"}realtime_credit_parttwo"
     //异步发送
//     producer.send(new ProducerRecord[String, String]("test4", iL, iL),new Callback() {
//       override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
//         if (exception != null) {
//           println(metadata.offset())
//         }
//       }
//     })

     //同步发送数据到kafka的缓冲区(序列化器，分区器)
     try{
       val metadata = producer.send(new ProducerRecord[String, String]("test", iL, iL)).get()
     }catch {
       case e:Exception => print(e.printStackTrace())
     }
      }

    producer.close()
  }
}
