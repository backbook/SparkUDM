//package utils
//
//import java.net.InetAddress
//import java.util.concurrent.locks.{Lock, ReadWriteLock, ReentrantReadWriteLock}
//
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.action.update.UpdateRequest
//import org.elasticsearch.client.transport.TransportClient
//import org.elasticsearch.common.settings.Settings
//import org.elasticsearch.common.transport.InetSocketTransportAddress
//import org.elasticsearch.common.xcontent.XContentFactory
//import org.slf4j.{Logger, LoggerFactory}
//
///**
//  * @author 76886049@qq.com sujinyang   date: 2018/4/29 15:45  computer: backbook
//  */
//object esUtils {
//
//  private val LOG = LoggerFactory.getLogger(this.getClass)
//  private val rwlock = new ReentrantReadWriteLock
//  private val wlock = rwlock.writeLock
//
//  def main(args: Array[String]): Unit = {
//   val client =  getTransportClient()
////    val  indexrequest = new IndexRequest("json","json","1").
////      source(XContentFactory.jsonBuilder().startObject().field("name","wang")
////      .field("age",15).field("price",156).endObject())
////    val updateRequest =  new UpdateRequest("json","json","1").doc(XContentFactory.jsonBuilder().startObject().field("price",156).endObject())
////      .upsert(indexrequest)
////    val  updateRespond = client.update(updateRequest).get()
//
//    val is  = createIndex("name")
//    print(is)
//  }
//
//  def getTransportClient():TransportClient={
//
//    var client: TransportClient = null
//
//    val settings: Settings = Settings.settingsBuilder.put("client.transport.sniff", true).put().build
//    try {
//      wlock.lock()
//      client = TransportClient.
//        builder.settings(settings).build.
//        addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(getResources.getString("InetAddress.name")),
//          getResources.getInt("InetAddress.port")))
//      LOG.info("create elasticsearch client success !!")
//    }catch {
//      case e:Exception=>{
//        LOG.error("出现异常"+ e.printStackTrace())
//      }
//    }finally {
//      wlock.unlock()
//    }
////    print(client.connectedNodes())
//    client
//  }
//
//
//
//  def createIndex(indexName:String):Boolean={
//    val  cIndexResponse= getTransportClient().admin().indices.prepareCreate(indexName)
//      .setSettings(Settings.builder()
//        .put("index.number_of_shards", getResources.getInt("index.number_of_shards"))
//        .put("index.number_of_replicas", getResources.getInt("index.number_of_replicas"))
//      ).get()
//    if(cIndexResponse.isAcknowledged){
//      true
//    }else{
//      false
//    }
//  }
//
//
//
//}
