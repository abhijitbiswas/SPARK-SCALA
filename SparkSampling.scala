package scala_maven.core
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class SparkSampling {
  def main(args:Array[String]){

    val conf = new SparkConf().setMaster("local[*]")
    val spark: SparkSession = SparkSession
      .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = sc.textFile("data/NumFileForSort.txt")
    //sc.makeRDD(a.takeSample(false, 1000, 1234)) - fixed num record say 1000 . Cannot use fraction
    //sample is fast because it just uses a random boolean generator that returns true fraction
    //percent of the time and thus doesn't need to call count.
    val sample = data.sample(false, 0.2)
    println("hi");
    sample.collect().foreach(println)
  }

}
