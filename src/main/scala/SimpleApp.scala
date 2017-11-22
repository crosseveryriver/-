import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/2.
  */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    val path = "hdfs://master:9000/README.md"
    val conf = new SparkConf().setAppName("SimpleApp")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(path,2).cache()
    var countA = rdd.filter((line) => line.contains('a')).count()
    println("包含a的行有：" + countA)

  }
}
