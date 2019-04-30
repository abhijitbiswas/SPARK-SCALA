package Sparkscala.sql
//Duplicate and uniq column using df
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
object DuplicateDistinctRecordOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    // val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
    val sprkSql = SparkSession.builder().appName("DuplicateDistinct").config(conf).getOrCreate()
    import sprkSql.implicits._
    val sc = sprkSql.sparkContext
    val acctDF = List(("1_ac", "Acc1"), ("1_ac", "Acc1"), ("1_ac", "Acc1"), ("2_ac", "Acc2"),
      ("2_ac", "Acc2"), ("3_ac", "Acc3")).toDF("AcctId", "Details")
    acctDF.show(false)
    val countsDF = acctDF.rdd.map(rec => (rec(0), 1)).reduceByKey(_ + _)
      .map(rec => (rec._1.toString, rec._2)).toDF("AcctId", "AcctCount")
    countsDF.show(false)
    val accJoinedDF = acctDF.join(countsDF, acctDF("AcctId") === countsDF("AcctId"),
      "left_outer").select(acctDF("AcctId"), acctDF("Details"), countsDF("AcctCount"))
    accJoinedDF.show()
    val distAcctDF = accJoinedDF.filter($"AcctCount" === 1)
    distAcctDF.show()
    val duplAcctDF = accJoinedDF.filter($"AcctCount" > 1)
    duplAcctDF.show()


  }

}
