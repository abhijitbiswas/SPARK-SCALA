package Sparkscala.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
/**
  * Created by abhijit.biswas on 2/21/2018.
  */

//## calculate month and year base maximum spend  on the basis of custid .

object AverageRevenue_spend {
  //caseclass for creating dataframe


  case class sales(month:String,year:Int,custId:Int,billamt:Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    // val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
    val sprkSql = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate()
    val sc = sprkSql.sparkContext
    sc.setLogLevel("ERROR")
    val month = "Jun"
    val year = 2017
    val salesData = sc.textFile("data/datateamintv/*"); // --- /* means all the file inside that location

    val salesDF =  salesData.map{x => x.split("-")}
      .map { x => sales( x(0).toString(),x(1).toInt,x(2).toInt, x(3).toDouble  ) }
      .filter { x => if(x.month == month & x.year==year) true else false }
    import sprkSql.implicits._
    val filteredDDF = salesDF.toDF()
    import org.apache.spark.sql.functions._
    val resultFl = filteredDDF.groupBy("custId")
      .agg(sum("billamt").alias("totalspend"))
      .orderBy(desc("totalspend"))
    resultFl.show();

    // resultFl its order by data...so need to get the first 1, head() is required for getting the first.
    println(resultFl.select("custId").head().get(0))


  }

}
