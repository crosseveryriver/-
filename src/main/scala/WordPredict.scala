import java.util

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{DBCollection, DBObject}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
Edge  * Created by Administrator on 2017/11/9.
  */
object WordPredict {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GraphXTest")
    val sc = new SparkContext(conf)
    val dbName = "test"
    val collectionName = "comments"
    val ip = "192.168.1.102"
    val port = 27017

    val collection :DBCollection = ConnectMongo.createDatabase(ip,port,dbName).getCollection(collectionName)
    val saveTo = ConnectMongo.createDatabase(ip,port,dbName).getCollection("edges")


    var edgesData = new util.ArrayList[Edge[Int]]()
    var graph : Graph[String,Int] = null
    var currentIndex = 0
//    var stream : Stream[DBObject] = null
//    while (currentIndex < length && currentIndex < 5000){
       collection.find().limit(args(0).toInt).toArray.toArray(Array[DBObject]()).toStream.foreach(f => {
        addData(f.get("content").toString,edgesData)
      })

//      currentIndex += 3000
//    }
//    val stream = collection.find().limit(3000).toArray.toArray(Array[DBObject]()).toStream

    val edges = sc.parallelize(edgesData.toArray(Array[Edge[Int]]()))
    graph = Graph.fromEdges(edges,"")
    graph = graph.groupEdges((a,b) => a+b)


//    val mergedEdges = edges.countByValue()
//    val edgesWithCount = sc.parallelize(mergedEdges.map(f => Edge[Int](f._1.srcId,f._1.dstId,f._2.toInt)).toArray[Edge[Int]])
//    val graph = Graph.fromEdges(edgesWithCount,"")


    val result : RDD[(Char,Char,Int)] = graph.triplets.map(f => (f.srcId.toChar,f.dstId.toChar,f.attr))
    result.saveAsTextFile("file:/home/hadoop/edges")
    println("total edges:" + result.count())
  }

  def addData(input : String,edgesData : => util.ArrayList[Edge[Int]]): Unit ={
    var str = input.replace("\n" , "").replace("\r","").replace(":" , "").replace("\\","")
    for(x <- 0 until str.length - 1 ){
      edgesData.add(Edge(str.charAt(x).toLong, str.charAt(x + 1).toLong,1))
    }
  }

//  def func(x : EdgeTriplet[String,Int]) : Unit = {
//    val dbName = "test"
//    val collectionName = "comments"
//    val ip = "192.168.1.102"
//    val port = 27017
//    val saveTo = ConnectMongo.createDatabase(ip,port,dbName).getCollection("edges")
//    saveTo.insert(MongoDBObject("from" -> x.srcId.toChar,"to" -> x.dstId.toChar ,"count" -> x.attr))}

}
