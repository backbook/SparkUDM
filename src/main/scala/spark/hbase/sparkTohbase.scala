package spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.getResources

/**
  * @author 76886049@qq.com sujinyang   date: 2018/5/1 15:02  computer: backbook
  */
object sparkTohbase {
  def main(args: Array[String]): Unit = {
    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()

    val  hbaseConf= HBaseConfiguration.create()
    //设置写入的表
    val tablename= "hbase"
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

    val count = hbaseRdd.count()
    println(count)
    hbaseRdd.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("f".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("f".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }}

    spark.stop()
    admin.close()

  }
}
