import java.util

import com.mongodb.{DBCollection, DBObject}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/11/13.
  */
object Kmeans {

  def main(args: Array[String]): Unit = {
    //   实例命令 ：spark-submit --class Kmeans --master local spark-assembly-1.0.jar 7 200 1000 file:///home/hadoop/result
    //    agrs参数分别为聚类数，迭代次数，预测次数，文件路径
    val conf = new SparkConf()
      .setAppName("Kmeans")
    val sc = new SparkContext(conf)
    val dbName = "test"
    val collectionName = "result"
    val ip = "192.168.1.102"
    val port = 27017

    val collection: DBCollection = ConnectMongo.createDatabase(ip, port, dbName).getCollection(collectionName)
    val input = collection.find().toArray().toArray(Array[DBObject]()).map(obj => Vectors.dense(obj.get("coordinate").toString.toCharArray.map(x => x.toDouble - 48)))
    val rdd1 = sc.parallelize(input)

    val numOfClusters = args(0).toInt
    val numOfInterations = args(1).toInt

    val model = KMeans.train(rdd1, numOfClusters, numOfInterations)

    println("============")
    for (c <- model.clusterCenters) {
      println(" " + c.toString())
    }

    val limit = args(2).toInt
    val path = args(3).toString
    val testData = collection.find().limit(limit).toArray().toArray(Array[DBObject]()).map(obj => (obj.get("_id").toString, Vectors.dense(
      obj.get("coordinate").toString.toCharArray.map(x => x.toDouble - 48)),obj.get("coordinate").toString))

    testData.foreach(println)
    var map = new mutable.HashMap[Int, util.ArrayList[(String, Double)]]()
    var map2 = new mutable.HashMap[Int,util.ArrayList[(String,String)]]()
    for (i <- 0 to 6){
      map.put(i, new util.ArrayList[(String, Double)]())
      map2.put(i, new util.ArrayList[(String, String)]())
    }

    val result = testData.foreach(x => {
      val predict: Int = model.predict(x._2)
      val center = model.clusterCenters(predict).asInstanceOf[DenseVector]
      var distance: Double = 0
      val cor = x._2.asInstanceOf[DenseVector]
      for (i <- 0 until center.size) {
        distance = distance + Math.pow(center(i) - cor(i), 2)
      }
      distance = Math.pow(distance, 0.5)
//      map.getOrElse(predict, new util.ArrayList[(String, Double)]()).add((x._1, distance))
      map2.getOrElse(predict, new util.ArrayList[(String, String)]()).add((x._1, x._3))
    })
    sc.parallelize(map2.toArray.toSeq).saveAsTextFile(path)


  }

}
