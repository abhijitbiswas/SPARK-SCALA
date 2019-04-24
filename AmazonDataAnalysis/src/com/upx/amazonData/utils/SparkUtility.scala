package com.upx.amazonData.utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Properties
import org.apache.spark.sql.SparkSession
import util.control.Breaks._

object SparkUtility {

  /**
   * createing Spark configuration
   * For Spark optimization set parameters here also
   */

  def getSprkConf(prop: Properties): SparkConf = {

    val appName = prop.getProperty(Param.SPARK_APP_NAME)
    val master = prop.getProperty(Param.SPARK_MASTER)
    val schedulerMode = prop.getProperty(Param.SPARK_SCHEDULER_MODE_TYPE)

    val sprkConf = new SparkConf()

    //set sparkJob name
    if (null != appName || !appName.isEmpty()) {
      sprkConf.setAppName(appName)
    } else {
      throw new AmazonCustomException("Spark AppName not provided , please provide the same in property File ")
    }

    //set master in local mode , not running in cluster
    if (null != master || !master.isEmpty()) {
      sprkConf.setMaster(master)
    } else {
      throw new AmazonCustomException("Spark Master not  provided , please provide the same in property File ")
    }

    //set scheduling type
  /*  if (null != schedulerMode || !schedulerMode.isEmpty()) {
      sprkConf.setMaster(schedulerMode)
    } else {
      throw new AmazonCustomException("Spark schedulerMode not  provided , please provide the same in property File ")
    }*/

    return sprkConf

  }

  /**
   * Creating SparkSession
   */

  def createSparkSession(sprkConfig: SparkConf): SparkSession = {

    if (null != sprkConfig) {
      println("We can now set sprkConfig")
    } else {
      throw new AmazonCustomException("Spark config not set properly .. Please check once ")
    }
    val spark = SparkSession
      .builder().
      config(sprkConfig)
      .getOrCreate()

    return spark
  }

  /**
   * spark context from spark session
   */
  def getSparkContext(sprkSession: SparkSession): SparkContext = {

    val sprkContext = sprkSession.sparkContext
    return sprkContext
  }

  /**
   * close SparkSession And SparkContext
   */

  def closeSparkSession(sprkSession: SparkSession) {
    sprkSession.close()
    sprkSession.stop()
  }

  /**
   * stop Spark context
   */
  def stopSprkContext(sprkCtx: SparkContext) {
    sprkCtx.stop()
  }
}