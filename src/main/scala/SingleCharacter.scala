import com.mongodb.{DBCollection, DBObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by Administrator on 2017/11/8.
  */
object SingleCharacter {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GraphXTest")
    val sc = new SparkContext(conf)
    val dbName = "test"
    val collectionName = "edgeInfo"
    val ip = "192.168.1.102"
    val port = 27017

    val collection :DBCollection = ConnectMongo.createDatabase(ip,port,dbName).getCollection(collectionName)
    val stream = collection.find().toArray.toArray(Array[DBObject]()).toStream

    val mapCharToCount : mutable.HashMap[Char,Int] = new mutable.HashMap[Char,Int]()
    stream.foreach(f => {
      val from = f.get("from").toString.charAt(0)
      val to = f.get("to").toString.charAt(0)
      val fromCount = f.get("fromCount").toString.toInt
      val toCount = f.get("toCount").toString.toInt
      mapCharToCount.put(from,fromCount)
      mapCharToCount.put(to,toCount)
    })

    val data = stream.map(f => {
      val from = f.get("from").toString.charAt(0)
      val to = f.get("to").toString.charAt(0)
      val count = f.get("count").toString.toInt
      //将to的Id补足5位
//      var toId = ""
//      for(  i <- to.toLong.toString.length  to 5){
//        toId += "0"
//      }
//      toId += to.toLong.toString
      Edge(from.toLong,to.toLong,count)
    })

    val edges : RDD[Edge[Int]]= sc.parallelize(data)

    val graph1 = Graph.fromEdges(edges,0)
    val newVertexs = graph1.vertices.mapValues((id,count) => (id.toChar,mapCharToCount.getOrElse(id.toChar, 0)))

    val f = (a : (VertexId,(Char,Int)), b :  (VertexId,(Char,Int))) => (0L,('0',a._2._2 + b._2._2))
    val singleCharacterCount = newVertexs.reduce(f)

//    println("showing edges:")
//    graph1.edges.foreach(println)
//
//    println("showing vertex:")
//    newVertexs.foreach(println)
//
//    println("total single Character Count:")
//    println(singleCharacterCount._2._2)
//
//    println("total words:" + newVertexs.count())
//    println("total edges:" + graph1.edges.count())

    val rankedGraph = graph1.pageRank(0.0001)

//    rankedGraph.edges.foreach(println)
    rankedGraph.vertices.foreach(x =>println(x._1.toChar + " " + x._2))



//    val edges : RDD[Edge[Int]] = sc.parallelize(Array(
//      Edge(1L,2L,10)
//    ))
//
//
//    val graph1 = Graph.fromEdges(edges,0)
//    val result : VertexRDD[Int] = graph1.vertices.mapValues((id, count) => 10)
//
//    graph1.vertices.foreach(println)
//    result.foreach(println)
  }
}
