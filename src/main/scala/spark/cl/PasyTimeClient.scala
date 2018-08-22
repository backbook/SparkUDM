package spark.cl

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.Socket


object PasyTimeClient {
  def main(args: Array[String]): Unit = {
    var port = 8085

    if (args != null && args.length > 0){
      try
      {
        port = args(0).toInt
      }catch {
        case e:NumberFormatException =>{
          print(e.printStackTrace())
        }
      }
    }

    var socket:Socket = null
    var in:BufferedReader = null
    var  out:PrintWriter = null
    try
    {
      socket = new Socket("192.168.1.100",port)
      in = new BufferedReader(new InputStreamReader(socket.getInputStream))
      out = new PrintWriter(socket.getOutputStream,true)
      out.println("QUERY TIME ORDER")
      println("send order 2 server succeed")
      val resp:String  =  in.readLine()
      println("now is "+resp)
    }
    catch {
      case e:Exception =>
    }finally {
      if (out !=null){
        out.close()
        out = null
      }
      if (in != null){
        try
        {
          in.close()
        }catch {case  e:Exception => println(e.printStackTrace())}
        in = null
      }
      if (socket  != null){
        try{
          socket.close()
        }catch {
          case  e:Exception => println(e.printStackTrace())
        }
        socket = null
      }
    }
  }
}
