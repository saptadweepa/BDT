package com.nits.stc.sprint15.service

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

class Test_ServiceTransformer extends FunSuite {

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

  test("status DF should contain CI_DEALER_CODE, CI_RO_NUMBER, CI_RO_CLOSE_DATE"){

    var RO_Columns = Seq("CI_DEALER_CODE", "CI_RO_NUMBER", "CI_RO_CLOSE_DATE")
    var StatusDF_Col = statusDF.columns
    println("columns of Status DF: ")
    StatusDF_Col.foreach((element: String) => print(element + " "))
    println()
    RO_Columns.foreach(element => assert(StatusDF_Col.contains(element)))

  }

  test("should flatten the dataset to contain RO Rows when the OPERATIONS_CLOB is null") {
    var RO_DF = statusDF.filter(col("OPERATIONS_CLOB_IS_NULL") === lit("Y"))

    var RO_notNull_count = RO_DF.filter(col("OPERATIONS_CLOB_IS_NULL").isNotNull || col("RO_ADVISOR_CONTACT_ID").isNotNull ||
      col("RO_ADVISOR_FIRST_NAME").isNotNull || col("RO_ADVISOR_MIDDLE_NAME").isNotNull || col("RO_ADVISOR_LAST_NAME").isNotNull
      || col("RO_ADVISOR_SALUTATION").isNotNull ||
      col("RO_ADVISOR_SUFFIX").isNotNull || col("RO_ADVISOR_FULL_NAME").isNotNull).count()

    assert(RO_notNull_count != 0)
  }

    test("should return total of 18 for SERVICE OPERATIONS with columns starting with SOPR_"){

      var SOPR_Col= statusDF.columns
      var count_res = 0
      for (i <- 0 to SOPR_Col.size - 1) {
        println(SOPR_Col(i))
        if(SOPR_Col(i).contains("SOPR_")){
          count_res = count_res +1
        }
      }

      assert(count_res == 18)

    }

  test("Dummy"){
    var status_Col= statusDF.columns
    var status_Col_count = status_Col.size
    println("status column count :" +status_Col_count)

    var RO_Tech_Col= targetDF_RO_Tech.columns
    var RO_Tech_Col_count = RO_Tech_Col.size
    println("RO_tech column count :" +RO_Tech_Col_count)

    var RO_Parts_Col= targetDF_RO_Parts.columns
    var RO_Parts_Col_count = RO_Parts_Col.size
    println("RO_parts column count :" +RO_Parts_Col_count)

    var Vin_master_Col= targetDFVin_Master.columns
    var Vin_master_Col_count = Vin_master_Col.size
    println("Vin_master column count :" +Vin_master_Col_count)

    var source_Col= sourceDF.columns
    var source_Col_count = source_Col.size
    println("source column count :" +source_Col_count)


  }

  test("should return total of 8 for Repair Order Technicians with columns starting with TECH_COL_"){

    var SOT_COL = targetDF_RO_Tech.columns
    var count_res = 0
    for (i <- 0 to SOT_COL.size - 1) {
      println(SOT_COL(i))
      if(SOT_COL(i).contains("SOT_")){
        count_res = count_res +1
      }
    }

    assert(count_res == 8)


  }



    


}
