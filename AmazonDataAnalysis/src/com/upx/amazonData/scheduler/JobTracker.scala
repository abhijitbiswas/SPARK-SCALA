package com.upx.amazonData.scheduler

import com.upx.amazonData.utils.ReadProps
import com.upx.amazonData.utils.SparkUtility
import com.upx.amazonData.utils.AmazonCustomException
import com.upx.amazonData.Operation.DataAnalysisImpl
import org.apache.spark.sql.SparkSession
/**
 * Entry class for all
 */
object JobTracker {

  /**
   * Entrypoint point of all
   */
  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    var sparkSession: SparkSession = null

    try {
      val readPropertyObj = new ReadProps()
      val propData = readPropertyObj.readProFile
      println("property file read")
      //craeting sparkContext
      val sprkConf = SparkUtility.getSprkConf(propData)
      sparkSession = SparkUtility.createSparkSession(sprkConf)

      println("sparkSession is created")
      val dataAnaOprObj = new DataAnalysisImpl()
      dataAnaOprObj.DataAnaOpe(sparkSession, propData)
      println("Process Done ")

    } catch {
      case ex: Exception => throw new AmazonCustomException("Main class getting exception **  ", ex)
    } finally {
      SparkUtility.closeSparkSession(sparkSession)
      SparkUtility.stopSprkContext(sparkSession.sparkContext)
    }

  }

}