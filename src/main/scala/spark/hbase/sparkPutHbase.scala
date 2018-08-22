package spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkPutHbase {

  def main(args: Array[String]): Unit = {
    val  sparkconf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark  = SparkSession.builder().config(sparkconf).getOrCreate()

    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "cr_zhimafen_base")
    jobConf.setOutputFormat(classOf[TableOutputFormat])


    val rdd = spark.sparkContext.makeRDD(Array(1)).flatMap(_ => 0 to 10)

    rdd.map(x => {
      var put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("number"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)

  }
}
