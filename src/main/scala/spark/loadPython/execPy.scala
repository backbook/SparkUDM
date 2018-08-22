package spark.loadPython

import org.python.util.PythonInterpreter

object execPy {
  def main(args: Array[String]): Unit = {
    val interpreter = new PythonInterpreter
    interpreter.execfile("F:\\Python_for_Pycharm\\Python_first_demo\\HelloWord.py")

  }
}
