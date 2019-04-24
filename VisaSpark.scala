package scala_maven.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
//problem
// Row level count and with schema create new DF
//in visaschema , there is the schema or field name available

object VisaSpark {
  val conf = new SparkConf().setMaster("local[*]")
  val sc = SparkSession.builder().config(conf).appName("OracleSpark").getOrCreate().sparkContext

  def main(args: Array[String]): Unit = {
    val schemaRdd = sc.textFile("/Users/abisw45/IdeaProjects/scala_maven/src/main/scala/scala_maven/SQL/visaDataSchema")
    val schemaResult = schemaRdd.map { x => x.split(",") }
      .map { x => (x(0),x(1),x(2)) }
    schemaResult.foreach(println)
    println("********************")
    val dataRdd = sc.textFile("/Users/abisw45/IdeaProjects/scala_maven/src/main/scala/scala_maven/SQL/visaData")
    val result = dataRdd.map { x => x.split(",") }
      .map { x => (x(0),x(1),x(2)) }
    //.map(x => ((x._1,1),(x._2,1)))
    result.foreach(println)
    println("********************")
    // x(0) --means row wise like apple.orange,guava,apple,apple
    //You cant provide x(0) instead of x._1(tuple1)
    val result_1= result.map(x=>(x._1,1)).reduceByKey(_+_)

    result_1.foreach(println)
    println("********************")
    val result_2= result.map(x=>(x._2,1)).reduceByKey(_+_)
    result_2.foreach(println)

    println("********************")
    val result_3= result.map(x=>(x._3,1)).reduceByKey(_+_)
    result_3.foreach(println)
    println("********************")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val schemaResult_1 = schemaResult.map(x=>x._1).toDF()
    //schemaResult_1.show(false)

    println("********************")
    //val result_1_col1 = result_1.map(x=>(x._1+","+x._2)).toDF()
    //result_1_col1.show(false)
    val final_1 =  schemaResult_1.crossJoin(result_1.toDF())
    final_1.show(false)

    /* OUTPUT -
      +------+------+---+
      |value |_1    |_2 |
      +------+------+---+
      |field1|guava |1  |
      |field1|orange|1  |
      |field1|apple |3  |
      +------+------+---+

     */

    final_1.rdd.map { x => ((x(0),x(1)),x(2)) }.foreach(println)
    /*OUTPUT -
     ((field1,guava),1)
     ((field1,orange),1)
     ((field1,apple),3)
     */
    final_1.rdd.map { x => (x(0),x(1),x(2)) }.foreach(println)

  }


}
