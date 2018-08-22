package spark.elasticsearch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.elasticsearch.spark._

//import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author 76886049@qq.com sujinyang   date: 2018/4/26 18:45  computer: backbook
  */
object sparkToes5_x {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    sparkconf.setMaster("local[*]")

//    sparkconf.set("es.nodes", getResources.getString("InetAddress.name"))
////    conf.set("es.nodes","127.0.0.1:9200")
//    sparkconf.set("es.port",getResources.getString("InetAddress.port"))
    sparkconf.set("es.index.auto.create", "true")

    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()

    val line: RDD[String] = spark.sparkContext.textFile("src\\main\\resources\\json.json")

//    line.collect().foreach(println)

    val index_type = "test/doc"

    //es.mapping.id可以自定的将id传入，以保证id的自定义
    line.saveJsonToEs(index_type)
//
//    val query = """{"query":{"match":{"name": "wnagming"}}}"""
//
//    spark.sparkContext.esRDD("json/doc")

//    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

//    line.saveToEs("myes/myes")
//    查询合作方为abc的数据
//    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

//    sc.makeRDD(Seq(numbers, airports)).saveToEs("my_es/docs")
//    val  esResult  = spark.sparkContext.esRDD(s"json/doc",query)
//    esResult.collect().foreach(println)

//    sc.makeRDD("j").saveToEs("job1")
    spark.stop()

  }
}