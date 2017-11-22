import java.util

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap


/**
  * Created by Administrator on 2017/11/8.
  */
object DoubleCharacter {
  def main(args: Array[String]): Unit = {

//    val input : String = "党的十九大，是在全面建成小康社会决胜阶段、中国特色社会主义发展关键时期召开的一次十分重要的大会。承担着谋划决胜全面建成小康社会、深入推进社会主义现代化建设的重大任务，事关党和国家事业继往开来，事关中国特色社会主义前途命运，事关最广大人民根本利益"
//
    val conf = new SparkConf()
      .setAppName("GraphXTest")
    val sc = new SparkContext(conf)
//
//    var vertexs = new util.ArrayList[String]()
//    var edges = new util.HashMap[(String,String),Int]()
//    var vertex3 : String = null
//    for(i <-2 to input.length){
//      val vertex1 = input.substring(i - 2,i)
//      vertexs.add(vertex1)
//      if(vertex3 != null){
//        edges.add((vertex3,vertex1))
//      }
//      if(i > 2){
//        vertex3 = input.substring(i - 3, i)
//        vertexs.add(vertex3)
//        edges.add((vertex1,vertex3))
//      }
//    }
//
//    sc.parallelize(vertexs.toArray()).map((_, 1)).reduceByKey(_+_)
//
//  sc.parallelize(vertexs.toArray(Array[String]())).map[(String,Int)](s => (s,1))

    val edges : RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(("你好".charAt(0).toLong.toString + "你好".charAt(1).toLong.toString).toLong,
        ("好啊".charAt(0).toLong.toString + "好啊".charAt(1).toLong.toString).toLong,3)
    ))

    val graph2 = Graph.fromEdges(edges,0)
    val map :  HashMap[String,Int] = new HashMap[String,Int]
    map.put("你好",3)
    map.put("好啊",5)

    val newVertex : VertexRDD[(String,Int)] = graph2.vertices.mapValues((id, count) => {
      val first =id.toString.substring(0,5).toInt.toChar
      val second =id.toString.substring(5,10).toInt.toChar
      val word = first + "" + second
      val count : Option[Int]= map.get(word)
      (word,count.getOrElse(0))
    })

    newVertex.foreach(println)
  }

}
