package Sparkscala.core
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by abhijit.biswas on 1/25/2018.
  */
object CustomRdd {
  def main(args : Array[String]): Unit ={
    val sprkConf = new SparkConf().setMaster("local[*]").setAppName("Custom Rdd Prac")
    val sprkCtx = new SparkContext(sprkConf)
    val crawlUrl : String = "http://StackOverFlow/aa"
    val customrddObj = new crawlRdd(crawlUrl,sprkCtx)
    customrddObj.saveAsTextFile("")
    sprkCtx.setCheckpointDir("hdfs Path You can set ")

  }

}

class CrawlRddPartition(val baseUrl : String , rddId : Int,idx : Int) extends Partition{
  //override def index: Int = ???
  override def index: Int = ???
}


class crawlRdd(val baseUrl : String , sc : SparkContext) extends RDD[String](sc, Nil){
  // get partition giving you a chance to write the logic
  override protected def getPartitions: Array[org.apache.spark.Partition] = {
    val partitions = new ArrayBuffer[CrawlRddPartition]()
    partitions.toArray
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val p = split.asInstanceOf[CrawlRddPartition]
    val baseUrl = p.baseUrl

    new Iterator[String]{
      override def hasNext: Boolean = {
        // write logic for if new baseUrl is there or not , if not then false
        true
      }

      override def next(): String = {
        //write logic to find out base url and return type of String
        val baseUrl = "http://newArea/"
        baseUrl
      }
    }


}



}

