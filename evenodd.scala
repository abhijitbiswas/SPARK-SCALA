package Sparkscala.core
//Even -odd usnig spark
import org.apache.spark._
object evenodd {
  def main(args: Array[String]): Unit = {
    val spkConf = new SparkConf().setMaster("local[2]").setAppName("even_odd")
    val sc = new SparkContext(spkConf)
    val x = sc.parallelize(1 to 10, 2)
    //even
    val y = x.filter { a => a%2 !=0 }
    //odd
    val y1 = x.filter(a => a%2 ==0)
    //y.foreach ( println )
    y.collect().foreach(println)
    y1.collect().foreach(println)


  }

}
