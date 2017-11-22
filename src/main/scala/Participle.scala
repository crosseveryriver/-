import com.mongodb.{DBCollection, DBObject}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/8.
  */
object Participle {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GraphXTest")
    val sc = new SparkContext(conf)
    val collection :DBCollection = ConnectMongo.createDatabase("192.168.1.102",27017,"test").getCollection("graph")
    val data = collection.find().toArray.toArray(Array[DBObject]()).toStream.map(x =>Edge(
        x.get("from").asInstanceOf[String].charAt(0).toLong,
        x.get("to").asInstanceOf[String].charAt(0).toLong,x.get("count").asInstanceOf[Int]))
      .filter(e => e.attr > 10)
    val edges : RDD[Edge[Int]] = sc.parallelize(data)
    val graph1 = Graph.fromEdges(edges,0)

  }
}
