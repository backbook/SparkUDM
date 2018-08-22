package spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64,Bytes}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.getResources

object sparkScanHbase {

  def main(args: Array[String]): Unit = {
    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark  = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    val  hbaseConf= HBaseConfiguration.create()
    //设置写入的表
    val tablename= "cr_zhimafen_base"
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.quorum",getResources.getString("hbase.zookeeper.quorum"))
    //设置zookeeper连接端口，默认2181
    hbaseConf.set("hbase.zookeeper.property.clientPort", getResources.getString("hbase.zookeeper.property.clientPort"))
    hbaseConf.set(TableInputFormat.INPUT_TABLE, getResources.getString("hbaseGetTable"))

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

    val data:RDD[(String, String)]  = hbaseRdd.map(r => {
      (Bytes.toString(r._2.getRow),
        Bytes.toString(r._2.getValue(Bytes.toBytes("f"),Bytes.toBytes("number"))))
    })

    var scan = new Scan();
    scan.addFamily(Bytes.toBytes("v"))
    var proto = ProtobufUtil.toScan(scan)
    var scanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN,scanToString)

    val datas = hbaseRdd.map( x=>x._2).map{result => (result.getRow,result.getValue(Bytes.toBytes("v"),Bytes.toBytes("value")))}.map(row => (new String(row._1),new String(row._2))).collect.foreach(r => (println(r._1+":"+r._2)))

    spark.stop()
    admin.close()
  }
}
