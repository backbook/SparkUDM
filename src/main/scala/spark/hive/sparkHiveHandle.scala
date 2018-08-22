package spark.hive

import org.apache.spark.sql.SparkSession

object sparkHiveHandle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hive").enableHiveSupport().getOrCreate()
    import spark.sql
    sql("use bd_src")
    sql("select * from cr_test").show()

  }
}
