package com.nits.stc.sprint15.service

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

class Test_GeneralScenarios extends FunSuite {

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
  var statusPath = uri + statusOptions("PATH") + "//STATUS_20201111173756//*.avro"
  //status dataframe
  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Number of records in Output should same as number of records in Input "){
    assert(sourceDF.count() == statusDF.count())

  }

  test("success records only to the target"){
    var success_record = statusDF.filter(col("prc") === lit("SUCCESS")).count()
    var RO_Parts_records = targetDF_RO_Parts.count()
    var RO_Tech_records = targetDF_RO_Tech.count()
    var Vin_Master_records = targetDFVin_Master.count()
    var Total_target_records =targetDF_RO_Parts.count() + targetDF_RO_Tech.count() + targetDFVin_Master.count()
    println(success_record)
    statusDF.filter(col("prc") =!= lit("REJECTED")).show()

    //assert(success_record == Total_target_records)

  }





  test("Column should be renamed as per renameColumnsService.json") {
    var renameColumns = Config.readConfig(uri + config("RENAME_COLUMNS")).params.asInstanceOf[Map[String, String]]


    var statusCol = statusDF.columns;
    statusCol.foreach(value => println(value))


    for (i <- 0 to renameColumns.values.size - 1) {
      var temp = renameColumns.values.toList(i)
      if (!"OPERATIONS_CLOB".equals(temp)) {
        assert(statusCol.contains(temp))
      }
    }
  }

    test("Should apply schema on RO_TECH Dataframe as per schemaROTech.json") {

      var path = uri + Config.readNestedKey(config, "SCHEMA")("RO_TECH")
      var df = new DFDefinition()
      df.setSchema(path)
      println(df.schema)
      assert(df.schema === targetDF_RO_Tech.schema)
    }

  test("Should apply schema on RO_PARTS Dataframe as per schemaROParts.json") {

    var path = uri + Config.readNestedKey(config, "SCHEMA")("RO_PARTS")
    var df = new DFDefinition()
    df.setSchema(path)
    println(df.schema)
    assert(df.schema === targetDF_RO_Parts.schema)
  }

  test("Should apply schema on VIN_MASTER Dataframe as per schemaVinMaster.json") {

    var path = uri + Config.readNestedKey(config, "SCHEMA")("VIN_MASTER")
    var df = new DFDefinition()
    df.setSchema(path)
    println(df.schema)
    assert(df.schema === targetDFVin_Master.schema)
  }

  test("status DataFrame should contain STATUS Columns"){

    var status_col= Seq("prc","inference","CUSTPAY_FLAG","RO_CLASS_TYPE","OPERATION_CATEGORY","VALID_OPERATION_FLAG","INTERNAL_FLAG","WARRANTY_FLAG","FILE_TIMESTAMP")
    var statusServiceDf_col= statusDF.columns;
    println("columns of status_service: ")
    statusServiceDf_col.foreach((element:String)=>print(element+ " "))
    println()
    status_col.foreach(element => assert(statusServiceDf_col.contains(element)))

  }





}
