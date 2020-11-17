package com.nits.stc.sprint13.service

import com.nits.etlcore.impl.{DFExtractor, DFLoader}
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.scalatest.FunSuite

class Service_Transformer_Tests extends FunSuite {

  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
  var path = "/config/service/serviceConfig.json" // path of serviceConfig.json file in hdfs
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]] // Reading content of config file above in Map format
  var options = Config.readNestedKey(config, "SOURCE") // reading the nested values in the config file under "SOURCE" key

  var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
  //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT, uri + "/raw/vw/service", options)
  //  var driver= new DriverProgram()


  var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
  var targetRP = uri + targetOptions("RO_PARTS")
  var targetDF_RO_Parts = Extractor.extract(Resource.AVRO, targetRP, null)
  var targetRT = uri + targetOptions("RO_TECH")
  var targetDF_RO_Tech = Extractor.extract(Resource.AVRO, targetRT, null)
  var targetVM = uri + targetOptions("VIN_MASTER")
  var targetDFVin_Master = Extractor.extract(Resource.AVRO, targetVM, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH") + "//STATUS_20201013145659//*.avro"
  //status dataframe
  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)
  sourceDF.printSchema()



    test("status DF should contain CI_DEALER_CODE, CI_RO_NUMBER, CI_RO_CLOSE_DATE") {
    var RO_Columns = Seq("CI_DEALER_CODE", "CI_RO_NUMBER", "CI_RO_CLOSE_DATE")
    var StatusDF_Col = statusDF.columns
    println("columns of Status DF: ")
    StatusDF_Col.foreach((element: String) => print(element + " "))
    println()
    RO_Columns.foreach(element => assert(StatusDF_Col.contains(element)))

  }
  test("All the Dates should be converted to long") {
    var statusDf_Schema = statusDF.schema
    for (i <- 0 to statusDf_Schema.fields.size - 1) {
      if (statusDf_Schema.fields(i).name.contains("_DATE") && !statusDf_Schema.fields(i).name.contains("BIRTH")) {
        assert(statusDf_Schema.fields(i).dataType === LongType)
      }
    }
  }
}
