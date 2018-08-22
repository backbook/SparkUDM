package spark.norm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/**
  * @author 76886049@qq.com sujinyang   date: 2018/7/25 21:36  computer: backbook
  */
object GenderDistribution {
  def main(args: Array[String]): Unit = {
    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark  = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    val  fileRdd= spark.sparkContext.textFile("file:///E:\\norm\\GenderDistribution.txt").distinct()
    val  mapRDD =  fileRdd.map(_.split(",")).map(x=>{
      (x(0),x(1),x(2))
    })
    mapRDD.collect().foreach(println)

//    //字段
//    val arr = Array[String]("id","uid","sex")
//
//    //创建schemaString
//    val schemaString = arr.mkString(" ")
//    // Generate the schema based on the string of schema
//    val fields = schemaString.split(" ")
//      .map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//
//    val  dataRdd = fileRdd.map(r=>{
//      val seq= r.split(",").flatMap(r => Try(r.trim).toOption)
//      Row.fromSeq(seq)
//    })
//
//    val testDF = spark.createDataFrame(dataRdd, schema)
//    testDF.createOrReplaceTempView("user")


    spark.stop()

  }
}
