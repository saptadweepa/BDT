package com.nits.stc.vehiclesales

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, lower}
import org.scalatest.FunSuite

class VehicleSalesTransformer_Tests extends FunSuite{

  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
  var path = "/config/vehiclesales/vehicleSalesConfig.json" // path of vehicleSalesConfig.json file in hdfs
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]] // Reading content of config file above in Map format
  var options = Config.readNestedKey(config, "SOURCE") // reading the nested values in the config file under "SOURCE" key

  var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
  //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT, uri + "//raw/vw/vehiclesales", options)
  //  var driver= new DriverProgram()


  var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
  var targetST = uri + targetOptions("SALESTRANSACTIONS")
  var targetDFSalesTransaction = Extractor.extract(Resource.AVRO, targetST, null)
  var targetSalesEmployees = uri + targetOptions("SALESEMPLOYEES")
  var targetDFSalesEmployees = Extractor.extract(Resource.AVRO, targetSalesEmployees, null)
  var targetVF = uri + targetOptions("VEHICLEFEATURES")
  var targetDFVehicleFeatures = Extractor.extract(Resource.AVRO, targetVF, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH") + "//STATUS_20200917132901//*.avro"

  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Target Dataframes should have all words in Uppercase"){
    //var employeeDFCols = targetDFSalesEmployees.columns
    var transactionDFCols = targetDFSalesTransaction.columns
    //var VFDFCols = targetDFVehicleFeatures.columns

    //employeeDFCols.foreach(value=>assert(targetDFSalesEmployees.filter(col(value).rlike("^[a-z]+$")).count()===0))
    //transactionDFCols.foreach(value=>assert(targetDFSalesTransaction.filter(col(value).rlike("^[a-z]+$")).count()===0))
    //VFDFCols.foreach(value=>assert(targetDFVehicleFeatures.filter(col(value).rlike("^[a-z]+$")).count()===0))

    assert(targetDFSalesTransaction.filter(col("DLR_NAME").rlike("^[a-z ]+$")).count()===0)
    targetDFSalesTransaction.show()

  }

  test("convert to lowercase and count"){
    var employeeDFCols = targetDFSalesEmployees
    employeeDFCols.show()
    var smallCase = employeeDFCols.withColumn("lowerCol",lower(col("SDE_FIRST_NAME")))
    smallCase.select(col("lowerCol")).show()
    //smallCase.filter(col("lowerCol").rlike("^[a-z]+$")).select(col("lowerCol")).show()
    smallCase.filter(col("SDE_FIRST_NAME").rlike("^[a-z]+$")).select(col("SDE_FIRST_NAME")).show()
  }


  test("Verify IsDuplicate column is present and contains value Y/N only"){
    var statusDFCols= statusDF.columns

    assert(statusDFCols.contains("IsDuplicate"))

    var statusDFNullcount=statusDF.filter(col("IsDuplicate").isNull).count()
    assert(statusDFNullcount===0)
    assert(statusDF.filter(col("IsDuplicate")=!=lit("Y") && col("IsDuplicate")=!=lit("N")).count()===0)

  }



}
