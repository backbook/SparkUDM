package spark

import java.util

import scala.util.Try

object test {
  def main(args: Array[String]): Unit = {
    var str  = "asa,sfas,fasf"
    val s = parseIds(str)
    println(s.toSeq)

  }

  def parseIds(ids: String): Seq[String] = {
    ids.split(",").flatMap(id => Try(id.trim).toOption)
  }
}
