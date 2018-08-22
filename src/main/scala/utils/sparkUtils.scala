package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author 76886049@qq.com sujinyang   date: 2018/5/3 10:05  computer: backbook
  */
object sparkUtils {

  def getSparkConf(): SparkConf ={

    val sparkconf: SparkConf = new SparkConf().setAppName("sparkUDM")
    sparkconf

    //此模式为设置master的模式
    if ( getResources.getBoolean("sparkConfStatus")) {
      getSparkConf().setMaster("local[*]")
    }

    if (getResources.getBoolean("sparkToEsOnconf")){
      sparkconf.set("es.nodes", getResources.getString("InetAddress.name"))
      //    conf.set("es.nodes","127.0.0.1:9200")
      sparkconf.set("es.port",getResources.getString("InetAddress.port"))
      sparkconf.set("es.index.auto.create", "true")

    }
    sparkconf
  }


  def getSparkSession(): SparkSession ={

    val sparksession:SparkSession  = SparkSession.builder().config(getSparkConf()).getOrCreate()
    sparksession
  }




}
