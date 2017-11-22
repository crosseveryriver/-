import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/17.
  */
object MyClassification {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Classification")
    val sc = new SparkContext(conf)

    val input = sc.textFile("file:///home/hadoop/result/part-00000")
    val class1 = input.filter(line => {
      line.substring(1,2).equals("1")
    })
    val class2 = input.filter(line => {
      !line.substring(1,2).equals("1")
    })
    val c1 : RDD[(Int,String,String)]= class1.flatMap(line => {
      if(line.charAt(line.length - 1)==',')
        line.substring(4,line.length - 3).split(" ")
      else
        line.substring(4,line.length - 2).split(" ")
    }).map(tuple => {
      var trimed : String = null
      if(tuple.charAt(tuple.length - 1) == ','){
        trimed = tuple.substring(1,tuple.length - 2)
      }else{
        trimed = tuple.substring(1,tuple.length - 1)
      }
      var splited = trimed.split(",")
      (0,splited(1),splited(0))
    })
    val c2 : RDD[(Int,String,String)] = class2.flatMap(line => {
      if(line.charAt(line.length - 1)==',')
        line.substring(4,line.length - 3).split(" ")
      else
        line.substring(4,line.length - 2).split(" ")
    }).map(tuple => {
      var trimed : String = null
      if(tuple.charAt(tuple.length - 1) == ','){
        trimed = tuple.substring(1,tuple.length - 2)
      }else{
        trimed = tuple.substring(1,tuple.length - 1)
      }
      var splited = trimed.split(",")
      (1,splited(1),splited(0))
    })
    val c :RDD[(Int,String,String)] = c1++c2
    val data = c.map(x => (LabeledPoint(x._1.toDouble,Vectors.dense(x._2.toCharArray.map(char => char - 48.0))) , x._3) )
    val splitData = data.randomSplit(Array(args(0).toDouble,args(1).toDouble))
    val model = NaiveBayes.train(splitData(0).map(x => x._1))

    val testData = splitData(1)

    /*贝叶斯分类结果的正确率*/
    val totalCorrect=testData.filter{
      point => model.predict(point._1.features)==point._1.label
    }

    val positiveCorrect=testData.filter{
      point => model.predict(point._1.features)==point._1.label && point._1.label == 0
    }

    val positiveError=testData.filter{
      point => model.predict(point._1.features)!=point._1.label && point._1.label == 0

    }

    val negativeCorrect=testData.filter(
      point => (model.predict(point._1.features)==point._1.label && point._1.label == 1)
    )

    val negativeError=testData.filter(
      point => (model.predict(point._1.features)!=point._1.label && point._1.label == 1)
    )
    val total = testData.count()

    val positive = testData.map{
      point => if( point._1.label == 0)
        1 else 0
    }.sum()

    val negative = testData.map{
      point => if( point._1.label == 1)
        1 else 0
    }.sum()


    println("======================================")
    println("测试数据大小：" + total + "有意义的评论个数："+ negative + "无意义的评论个数：" + positive )
    println("正确预测次数为：" + totalCorrect.count() + "正确预测有意义的评论个数：" + negativeCorrect.count()+ "正确预测无意义的评论个数：" + positiveCorrect.count())
    positiveCorrect.map(x => x._2).saveAsTextFile("file:///home/hadoop/pc")
    negativeCorrect.map(x => x._2)saveAsTextFile("file:///home/hadoop/nc")
    positiveError.map(x => x._2)saveAsTextFile("file:///home/hadoop/pe")
    negativeError.map(x => x._2)saveAsTextFile("file:///home/hadoop/ne")

  }

}
