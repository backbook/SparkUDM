//package spark.stream
//
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
//object spark_comsumer {
//  def main(args: Array[String]): Unit = {
//
//
//   kafkaSpark
//
//
//  }
//
//  def kafkaSpark={
//
//    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[2]")
//    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()
//
//
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "node1:9092,node2:9092")
//      .option("subscribe", "test")
//      .load()
//    import spark.implicits._
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//
//
//  }
//
//  def ncStruSpark={
//
//    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
//    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()
//
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", "node1")
//      .option("port", 9999)
//      .load()
//
//    import spark.implicits._
//    // Split the lines into words
//    val words = lines.as[String].flatMap(_.split(" "))
//
//    // Generate running word count
//    val wordCounts = words.groupBy("value").count()
//
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    query.awaitTermination()
//
//  }
//}
