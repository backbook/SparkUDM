package spark.loadPython

import org.python.util._
import org.python.core.PyFunction
import org.python.core._


object loadPython {
  def main(args: Array[String]): Unit = {
    val interpreter = new PythonInterpreter
//    interpreter.exec("days=('mod','Tue','Wed','Thu','Fri','Sat','Sun'); ")
//    interpreter.exec("print days[1];")
    interpreter.execfile("F:\\Python_for_Pycharm\\spark_UDM_script\\spark_demo\\my_utils.py")


    val func = interpreter.get("adder", classOf[PyFunction]).asInstanceOf[PyFunction]
    val  a = " { 'a' : 1, 'b' : {'a':1,'c':45}, 'c' : 3, 'd' : 4, 'e' : 5 }  "
    val pyobj = func.__call__(new PyString(a))
    System.out.println("anwser = " + pyobj.toString)

  }
}
