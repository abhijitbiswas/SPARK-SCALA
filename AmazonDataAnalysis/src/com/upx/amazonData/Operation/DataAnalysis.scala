package com.upx.amazonData.Operation
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import com.upx.amazonData.utils.AmazonCustomException
import java.util.Properties
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * 
 */
trait DataAnlysis {
  
  /**
   * operation
   * @return type DataFrame
   */
  @throws(classOf[AmazonCustomException])
  @throws(classOf[Exception])
  def DataAnaOpe(sprkSession : SparkSession,prop : Properties): DataFrame 
  
  /**
   * cleaning the first row
   * @return Dataset[Row]
   */
   @throws(classOf[Exception])
  def cleanData(df: DataFrame): Dataset[Row]
  
  
  /**
   * Renaming the dataframe
   */
  @throws(classOf[Exception])
  def renameDataFrame(dataRow : Dataset[Row]): DataFrame
  
  /**
   * query Result from spark 
   */
  @throws(classOf[Exception])
  def queryResult(sprkSession: SparkSession, prop: Properties,dfQuery : DataFrame) : DataFrame 
}