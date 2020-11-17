package com.nits.stc.sprint15.service

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

class TestServiceRules extends FunSuite {

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
  //sourceDF.printSchema()

  test("Dealer not found in dealer master"){
    var statusDFExpected = statusDF.filter(col("DLR_NAME").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("Dealer not found in dealer master.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("RO Number is null"){
    var statusDFExpected = statusDF.filter(col("RO_NUMBER").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("RO NUMBER is Null.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("RO Close Date is null"){
    var statusDFExpected = statusDF.filter(col("RO_CLOSE_DATE").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("RO Close Date is null.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("RO Close Date is less than 01-JAN-2013"){
    var statusDFExpected = statusDF.filter(col("RO_CLOSE_DATE") < 1356998400000L && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("RO Close Date is less than 01-JAN-2013.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("RO close date is greater than current RO close date"){
    var statusDFExpected = statusDF.filter(col("RO_CLOSE_DATE") > col("CI_RO_CLOSE_DATE") || col("CI_RO_CLOSE_DATE").isNull
      && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDFExpected.filter(col("inference").contains("RO close date is greater than current RO close date.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }


  test("RO Open Date is null"){
    var statusDFExpected= statusDF.filter(col("RO_OPEN_DATE").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("RO Open Date is null.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("RO close date is less than RO open date"){

    var statusDFExpected = statusDF.filter(col("RO_CLOSE_DATE") < col("RO_OPEN_DATE")
      && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("RO close date is less than RO open date.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("Total Repair Order Price is greater than 100000"){
    var statusDFExpected = statusDF.filter(col("TOTAL_REPAIR_ORDER_PRICE") >= 100000
      && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("Total Repair Order Price is grater than 100000.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("Total Repair Order Cost is grater than 100000"){
    var statusDFExpected = statusDF.filter(col("TOTAL_REPAIR_ORDER_COST") >= 100000
      && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("Total Repair Order Cost is grater than 100000.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("VEH VIN is null"){
    var statusDFExpected= statusDF.filter(col("VEH_VIN").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("VEH VIN is null.")).count()
    println(expected + " " + actual)
    assert(expected == actual)

  }

  test("Length of SRO VEH VIN is not equal to 17"){
    var statusDFExpected = statusDF.filter(length(col("VEH_VIN")) =!= 17 && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Length of SRO VEH VIN is not equal to 17.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Operations clob is null"){

    var statusDFExpected = statusDF.filter(col("OPERATIONS_CLOB_IS_NULL") === lit("Y") && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDF.filter(col("inference").contains("Operations clob is null.")).count()
    println(expected +" "+actual)
    assert(expected == actual)
    
  }

  test("Custpay flag set to Y rule 1"){

    var statusDFExpected = statusDF.filter(col("SOPR_PAYMENT_TYPE") === lit("C") && col("TOTAL_CUST_PRICE") > 0
      && col("CUSTPAY_FLAG") === lit("Y"))
    val expected = statusDFExpected.count()
    var actual = statusDF.filter(col("inference").contains("Custpay flag set to Y rule 1.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Internal flag set to Y rule 2"){

    var statusDFExpected = statusDF.filter(col("SOPR_PAYMENT_TYPE") === lit("I")
      && col("TOTAL_INTL_PRICE") =!= 0
      && col("SOPR_OP_CODE").isNotNull
      && col("SOPR_OP_DESCRIPTION").isNotNull
      && col("SOPR_PARTS_PRICE") =!= 0
      && col("INTERNAL_FLAG") === lit("Y"))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Internal flag set to Y rule 2.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Warranty flag set to Y rule 3"){
    var statusDFExpected = statusDF.filter(col("SOPR_PAYMENT_TYPE") === lit("W")
      && col("TOTAL_WARR_PRICE") > 0
      && col("WARRANTY_FLAG") === lit("Y"))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Warranty flag set to Y rule 3.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("OPERATION_CATEGORY set to M"){
    var statusDFExpected = statusDF.filter(col("SOPR_OPERATION_CATEGORIES").isNotNull
      && col("SOPR_OPERATION_CATEGORIES").contains("MAINTENANCE")
      && col("OPERATION_CATEGORY") === lit("M"))

    val expected = statusDFExpected.count()
    var actual = statusDF.filter(col("inference").contains("OPERATION_CATEGORY set to M.")).count()
    println(expected +" "+actual)
    assert(expected == actual)


  }

  test("RO_CLASS_TYPE set to EXP"){

    var statusDFExpected = statusDF.filter(col("DLR_EXPRESS_DLR_FLAG")=== lit("Y")
      && col("DLR_EXPRESS_SCHEDULED_DATE") <= col("RO_CLOSE_DATE")
      && col("SOPR_OP_CODE").isNotNull
      && col("SOPR_OP_CODE").contains("SXINSP")
      && col("RO_CLASS_TYPE")=== lit("EXP"))

    val expected = statusDFExpected.count()
    var actual = statusDF.filter(col("inference").contains("RO_CLASS_TYPE set to EXP.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("VALID_OPERATION_FLAG set to 0"){

    var statusDFWithResult = statusDF.withColumn("Result",col("SOPR_LABOR_PRICE") +  col("SOPR_PARTS_PRICE"))
    var expectedDf = statusDFWithResult.filter(col("Result") <= 0 && col("VALID_OPERATION_FLAG") === lit(0))
    var actual = expectedDf.filter(col("inference").contains("VALID_OPERATION_FLAG set to 0.")).count()

    println(expectedDf.count() +" "+actual)
    assert(expectedDf.count()  == actual)

  }

  test("SOPR_BILLED_LABOR_HRS is null or 0"){

    var statusDFExpected = statusDF.filter(col("SOPR_BILLED_LABOR_HRS")=== typedLit[Double](0)
      && col("SOPR_ACTUAL_LABOR_HRS") > typedLit[Double](0)
      && col("SOPR_LABOR_PRICE") > typedLit[Double](0))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter( col("SOPR_BILLED_LABOR_HRS") === col("SOPR_ACTUAL_LABOR_HRS")
      && col("inference").contains("SOPR_BILLED_LABOR_HRS null or 0 hence set to SOPR_ACTUAL_LABOR_HRS.")).count()

    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("VEH_MAKE not VOLKSWAGEN and VEH_VIN not of Volkswagen"){
    var statusDFExpected = statusDF.filter(col("VEH_MAKE") =!= "VOLKSWAGEN"
        && col("VEH_VIN").isNull)

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter( col("prc") === lit("REJECTED") && col("inference").contains("Repair Order is not related to VW Service Vehicle")).count()

    println(expected + " " +actual)
    assert(expected == actual)

  }

}
