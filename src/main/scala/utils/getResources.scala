package utils

import java.io.InputStream
import java.util._
import java.util.Properties

import scala.collection.JavaConversions._

/**
  * @author 76886049@qq.com sujinyang   date: 2018/4/25 10:09  computer: backbook
  */
object getResources  {

  private def geMap(): HashMap [String, String] ={
    //配置对象
    val properties = new Properties()
    var stream: InputStream = null
    try{
      //采用getResourceAsstream的方法主要是spark运行程序依赖不要变化太大
       stream = this.getClass.getResourceAsStream("/config.properties")
    }catch { case e:Exception => new Throwable("读取配置参数流出错") }
    //将resources文件目录下的config.peoperties读取成流
    properties.load(stream)
    val map = new HashMap[String,String]()
    for ( key <- properties.stringPropertyNames){
      val value: String = properties.getProperty(key).trim
      map.put(key,value)
    }
    map
  }

  def getString(parameter:String): String ={
    val map = geMap()
    map.get(parameter).toString
  }

  def getInt(parameter:String): Int ={
    val map = geMap()
    map.get(parameter).toInt
  }

  def getBoolean(parameter:String): Boolean ={
    val map = geMap()
    map.get(parameter).toBoolean
  }

  def getFloat(parameter:String):Float={
    val map = geMap()
    map.get(parameter).toFloat
  }
  def getDouble(parameter:String):Double={
    val map = geMap()
    map.get(parameter).toDouble
  }

}
