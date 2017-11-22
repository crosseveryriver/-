import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/3.
  */
object GraphXTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GraphXTest")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val users: RDD[(VertexId,(String,String))] = sc.parallelize(Array(
      (3L,("rxin", "student")),
      (7L,("jgonzal", "postdoc")),
      (5L,("franklin", "professor")),
      (2L,("istoica", "professor"))
    ))

    val relationships : RDD[Edge[String]]  = sc.parallelize(Array(
      Edge(3L,7L,"Collaborator"),
      Edge(5L,3L,"Advisor"),
      Edge(2L,5L,"Colleague"),
      Edge(5L,7L,"PI")
    ))

    val graph : Graph[(String,String),String]= Graph(users,relationships,{("John Doe","Missing")})

    println("=================================================")
    println("totla vertex:" + graph.vertices.count())
    println("final reslult:" + graph.vertices.filter{case (id, (name, pos)) => pos == "postdoc" }.count())

    println("total edges:" + graph.edges.count())

    (graph.triplets.map(triplet => triplet.srcAttr._1 + " " + triplet.attr + " " + triplet.dstAttr._1))
      .collect().foreach(
      str => println(str)
    )


  }
}
