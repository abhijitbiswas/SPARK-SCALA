package com.upx.amazonData.Operation
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import com.upx.amazonData.utils.AmazonCustomException
import java.util.Properties
import com.upx.amazonData.utils.Param

/**
 * Analysis class
 */
class DataAnalysisImpl extends DataAnlysis {

  @throws(classOf[AmazonCustomException])
  @throws(classOf[Exception])
  override def DataAnaOpe(sprkSession: SparkSession, prop: Properties): DataFrame = {

    var dataResultFinal: DataFrame = null
    try {
      val csvPath = prop.getProperty(Param.CSV_DATA_PATH)
      var csvData = sprkSession.read.csv(csvPath)
      var filtData = cleanData(csvData)
      var dfRenamed = renameDataFrame(filtData)
      
      var dataResultFinal = queryResult(sprkSession, prop, dfRenamed)
      println("************ Final result *********")
      
      dataResultFinal.show(10)
    } catch {
      case ex: Exception => throw new AmazonCustomException("Exception because DataAnaOpe failed**  ", ex)
    }
    
    return dataResultFinal
  }

  
  /**
   * Filter the first column
   * @return Dataset[Row]
   */
  @throws(classOf[Exception])
  override def cleanData(df: DataFrame): Dataset[Row] = {

    var filtData: Dataset[Row] = null
    try {
      //val columnRow = df.first()
      val columnRow = df.first()
      filtData = df.filter(x => x != columnRow)
    } catch {
      case ex: Exception => throw new AmazonCustomException("Exception from Cleaning the data **", ex)
    }
    return filtData

  }

  /**
   * Reanmed the Dataframe
   * return DataFrame
   */

  @throws(classOf[Exception])
  override def renameDataFrame(dataRow: Dataset[Row]): DataFrame = {
    var renamedDataFrame: DataFrame = null
    try {
      val filtNameColumn = Seq("Id", "ProductId", "UserId", "ProfileName", "HelpfulnessNumerator", "HelpfulnessDenominator", "Score", "Time", "Summary", "Text")
      renamedDataFrame = dataRow.toDF(filtNameColumn: _*)
    } catch {
      case ex: Exception => throw new AmazonCustomException("Exception for renaming  the data **", ex)
    }
    return renamedDataFrame

  }

  /**
   * Query Result
   */

  @throws(classOf[Exception])
  override def queryResult(sprkSession: SparkSession, prop: Properties, dfQuery: DataFrame): DataFrame = {
    var queryResult: DataFrame = null
    try {
      val tempTabName = prop.getProperty(Param.GLOBAL_TEMP_TABLE_NAME)
      dfQuery.createGlobalTempView(tempTabName)
      val queryFinal = prop.getProperty(Param.RESULT_QUERY_FINAL)
      println(queryFinal)
      queryResult = sprkSession.sql(queryFinal)
      
    } catch {
      case ex: Exception => throw new AmazonCustomException("Exception for failing query, in method queryResult **", ex)
    }
    return queryResult
  }

}




