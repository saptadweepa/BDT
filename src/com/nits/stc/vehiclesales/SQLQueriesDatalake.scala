package com.nits.stc.vehiclesales

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object SQLQueriesDatalake {
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
//
// hdfs://saptadwipa-Aspire-A515-54G:8020/Output_100520/vw/dw_parts_sales_fact/*.avro
  //part-00000-ade1ea71-b608-40ac-a8c7-f478d970ae9a-c000.avro
  //hdfs://saptadwipa-Aspire-A515-54G:8020/Output_100520_avro/part-00000-ade1ea71-b608-40ac-a8c7-f478d970ae9a-c000.avro
  var Extractor = new DFExtractor()
  var df = Extractor.extract(Resource.AVRO, uri + "/Output_100520/vw/dw_parts_sales_fact/*.avro", null)
  //  var driver= new DriverProgram()

  def main(args:Array[String]){
    df.createOrReplaceTempView("InvoiceAll")
    spark.sql("Select sum(INV_TOTAL_PRICE) from InvoiceAll group by INV_CLOSE_DATE").show()
    spark.sql("Select sum(INV_TOTAL_PRICE) from InvoiceAll").show()
  }
}
