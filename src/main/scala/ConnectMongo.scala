import com.mongodb.casbah.{MongoClient, MongoDB}

/**
  * Created by Administrator on 2017/11/8.
  */
object ConnectMongo {

  def createDatabase(url : String, port : Int, dbName: String): MongoDB = {
    MongoClient(url,port).getDB(dbName)
  }

  def main(args: Array[String]): Unit = {
    val db : MongoDB = createDatabase("192.168.1.102",27017,"test")
    val graph = db.getCollection("graph")
    println("=======================")
    println(graph.find().toArray().toString)

  }
}
