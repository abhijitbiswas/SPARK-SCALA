package com.upx.amazonData.utils
import java.util.Properties
import java.io.FileInputStream


class ReadProps  {
  
  /**
   * need to provide property file location
   */
  private val propFilePath : String = Param.PROP_FILE_PATH
  
  
  /**
   * read the property file and pass
   * @return Properties
   */
  def readProFile:Properties = {
   
    val props: Properties = new Properties
    var inputStream = new FileInputStream(propFilePath)
    props.load(inputStream)
    //println(props.getProperty("dbpassword"))
    return props
    
  }
  
  
}


object test{
  def main(args: Array[String]): Unit = {
    val testObj = new ReadProps()
    val data = testObj.readProFile
    println(data.getProperty("dbpassword"))
  }
  
  
}