package spark.hbase


import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import utils.getResources
import org.elasticsearch.spark._

import scala.util.Try


/**
  * @author 76886049@qq.com sujinyang   date: 2018/5/1 15:02  computer: backbook
  */
object sparkGetHbase {
  def main(args: Array[String]): Unit = {
    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark  = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    val  hbaseConf= HBaseConfiguration.create()
    //设置写入的表
    val tablename= "cr_zhimafen_base"
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.quorum",getResources.getString("hbase.zookeeper.quorum"))
    //设置zookeeper连接端口，默认2181
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val conn = ConnectionFactory.createConnection(hbaseConf)

//        val admin: Admin = conn.getAdmin
    val admin: HBaseAdmin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._
    import spark.sql

    //字段
    val arr = Array[String]("number","su","dasd","23eqw")


    //根据字段拉取
    var str  = new StringBuffer()
    val data  = hbaseRdd.map(r => {
      val  rowkey = Bytes.toString(r._2.getRow)
      var  appendStr:String = null
      for(name <- arr){
        val  value  =  Bytes.toString(r._2.getValue(Bytes.toBytes("f"),Bytes.toBytes(name)))
        str.append("\u0001")
        str.append(value)
      }
      appendStr=str.toString
      str.setLength(0)
      rowkey+appendStr
    })

    //创建schemaString
    val schemaString = arr.mkString(" ")
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)


    val  dataRdd = data.map(r=>{
      val seq= r.split("\u0001").flatMap(r => Try(r.trim).toOption)
      Row.fromSeq(seq)
    })

    val testDF = spark.createDataFrame(dataRdd, schema)
    testDF.createOrReplaceTempView("user")

     //写入到hive
//    sql("use bd_src")
//    sql("create table if not exists user_t(number string,su string,dasd string,23eqw string)")
//    sql("insert into user_t from user")

    sql("select * from user").show()

       //循环遍历rowkey
//    def getRes(result: Result) ={
//        //获取行键
//        val rowkey = Bytes.toString(result.getRow)
//        //使用的方式是用keyset
//        val keysetmap = result.getFamilyMap("f".getBytes())
//        val keySet = keysetmap.navigableKeySet().iterator()
//        val map = new util.HashMap[String, String]()
//        map.put("rowkey", rowkey)
//        while (keySet.hasNext) {
//          val key = keySet.next()
//          val familyKey = Bytes.toString(key)
//          val value = Bytes.toString(result.getValue("f".getBytes, familyKey.getBytes))
//          println(rowkey + "------" + familyKey + "----" + value)
//          map.put(familyKey, value)
//        }
//        (rowkey,map)
//      }
//    }


    spark.stop()
    admin.close()

  }
}
