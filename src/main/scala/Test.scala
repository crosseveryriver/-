import com.mongodb.DBCollection
import com.mongodb.casbah.commons.MongoDBObject

/**
  * Created by Administrator on 2017/11/9.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val dbName = "test"
    val collectionName = "writetest"
    val ip = "192.168.1.102"
    val port = 27017

    val collection :DBCollection = ConnectMongo.createDatabase(ip,port,dbName).getCollection(collectionName)

    collection.insert(MongoDBObject("from" -> "a", "to" -> "b", "count" -> "10"))
  }

}
