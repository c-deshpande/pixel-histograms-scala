import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main(args: Array[String]) {

    /* ... */
    val conf = new SparkConf().setAppName("Histogram")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val line = sc.textFile(args(0)).map(line => {

      val a = line.split(",")
      (((1, a(0)), 1), ((2, a(1)), 1), ((3, a(2)), 1))
    })

    val red = line.map(x => {

      x._1
    })
    val green = line.map(x => {

      x._2
    })
    val blue = line.map(x => {

      x._3
    })

    val combined = red.union(green).union(blue)
    val reduceResult = combined.reduceByKey(_ + _)

    val result = reduceResult.map(x => {

      x._1._1 + "\t" + x._1._2 + "\t" + x._2
    }).collect.foreach(println)

    //result.saveAsTextFile("output1")
    sc.stop()
  }
}