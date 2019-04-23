package scala_maven.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object WebLogAnalysis {
  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("WebLogAnalysis")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args:Array[String]){
    val data = sc.textFile("data/logfile.txt")
    var logs = data.map { line => line.trim().toLowerCase() }
    logs.persist();
    println("Total Request="+logs.count())

    var errorLogs = logs.filter { x => (x.split(" ")(8) == "404")}

    errorLogs.foreach (println)

    //Longest log line
    var lengths = logs.map { x => (x.length(),x ) }.collect().sortBy(-_._1).take(1)
    println("Longest log line = "+lengths(0)._2+" - Length = "+lengths(0)._1)

    println(logs.map { x => x.length() }.reduce(math.max))

    //Find out who generate most request
    var mostReq= logs.map { x => x.split(" ")(0) }
      .map { x => (x,1) }.reduceByKey(_+_).collect().sortBy(-_._2).take(1)
    println("Most request generatd from = "+ mostReq(0)._1 )
    ///////////////////////////////////////////////////////////////
    case class Student(name: String, score: Int)
    val alex = Student("Alex", 83)
    val david = Student("David", 80)
    val frank = Student("Frank", 85)
    val julia = Student("Julia", 90)
    val kim = Student("Kim", 95)

    val students = Seq(alex, david, frank, julia, kim)

    def max(s1: Student, s2: Student): Student = if (s1.score > s2.score) s1 else s2

    val topStudent = students.reduceLeft(max)
    println(s"${topStudent.name} had the highest score: ${topStudent.score}")

    val a = Array(20, 12, 6, 15, 2, 9)
    println(a.reduceLeft(_ min _))
  }

}
