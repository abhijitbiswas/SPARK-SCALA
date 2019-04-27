package Sparkscala.core
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode


object explodeExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("explodeTest")
    val sprkSql = SparkSession.builder().config(conf).getOrCreate()
    val sc = sprkSql.sparkContext
    val test = sprkSql.read.json(sc.parallelize(Seq("""{"a":1,"b":[2,3]}""")))
    import sprkSql.implicits._
    val flattened = test.withColumn("b", explode($"b"))

    flattened.show()
  }

}
