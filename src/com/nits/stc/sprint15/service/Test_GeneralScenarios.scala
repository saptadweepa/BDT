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
  var partsDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/parts/*.avro", null)
  var parts_category_codeDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/parts_category_code/*.avro", null)
  //  var driver= new DriverProgram()


  var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
  var targetRP = uri + targetOptions("RO_PARTS")
  var targetDF_RO_Parts = Extractor.extract(Resource.AVRO, targetRP, null)
  var targetRT = uri + targetOptions("RO_TECH")
  var targetDF_RO_Tech = Extractor.extract(Resource.AVRO, targetRT, null)
  var targetVM = uri + targetOptions("VIN_MASTER")
  var targetDFVin_Master = Extractor.extract(Resource.AVRO, targetVM, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH") + "//STATUS_*/*.avro"
  //status dataframe
  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Number of records in Output should same as number of records in Input "){
    assert(sourceDF.count() == statusDF.count())

  }

/*  test("success records only to the target"){
    var success_record = statusDF.filter(col("prc") === lit("SUCCESS")).count()
    var RO_Parts_records = targetDF_RO_Parts.count()
    var RO_Tech_records = targetDF_RO_Tech.count()
    var Vin_Master_records = targetDFVin_Master.count()
    var Total_target_records =targetDF_RO_Parts.count() + targetDF_RO_Tech.count() + targetDFVin_Master.count()
    println(success_record)
    statusDF.filter(col("prc") =!= lit("REJECTED")).show()

    //assert(success_record == Total_target_records)

  }*/





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

  test("success records only to the target"){
/*   targetDF_RO_Parts.show(100)
   var t1= targetDF_RO_Parts.select(countDistinct("RO_NUMBER")).show()
   // var t2= targetDF_RO_Parts.select(countDistinct("RO_NUMBER")).first().get(0)
    println(t2)*/

    targetDF_RO_Parts.select(countDistinct("DEALER_CODE","RO_NUMBER")).show()
    var status_successCount= statusDF.filter(col("prc")=== lit("SUCCESS")).count()
    var RO_PartsRecord = targetDF_RO_Parts.select(countDistinct("RO_NUMBER")).first().get(0)
    assert(status_successCount == RO_PartsRecord)

  }

  test("Target Data Frame of Service should contain dealer denormalized columns"){
    var columns = targetDF_RO_Parts.columns

    //DLR_CODE,DLR_NAME,RGN_CODE,AREA_CODE,OEM_CODE,DLR_ACTUAL_STATUS_CODE,DLR_EXPRESS_SCHEDULED_DATE,DLR_EXPRESS_DLR_FLAG
    assert(
      columns.contains("DEALER_CODE") &&
        columns.contains("DLR_NAME") &&
        columns.contains("RGN_CODE") &&
        columns.contains("AREA_CODE") &&
        columns.contains("OEM_CODE") &&
        columns.contains("DLR_ACTUAL_STATUS_CODE"))

  }

  test("Target Data Frame of Service should contain parts denormalized columns"){
    var columns = targetDF_RO_Parts.columns

    //PART_NUMBER,PART_SPG,PCAT_CODE
    assert(
      columns.contains("SOPT_PART_NUMBER") &&
        columns.contains("PART_SPG_CODE") &&
        columns.contains("PCAT_CODE"))
  }

  test("Target Data Frame of Service should contain parts_category_code denormalized columns"){
    var columns = targetDF_RO_Parts.columns

    //PCAT_CODE,PCAT_DESC,PCAT_MINOR_TACT_SEGMENT,PCAT_MAJOR_TACT_SEGMENT,PCAT_PRODUCT_GROUP
    assert(
      columns.contains("PCAT_CODE") &&
        columns.contains("PCAT_DESC") &&
        columns.contains("PCAT_MINOR_TACT_SEGMENT") &&
        columns.contains("PCAT_MAJOR_TACT_SEGMENT") &&
        columns.contains("PCAT_PRODUCT_GROUP")  )
  }

  test("Target Service data is denormalized with Dealers or not")
  {

    var targetDF_Ser = targetDF_RO_Parts

    dealerDF= dealerDF.withColumnRenamed("DEALER_CODE", "DLR_CODE")
    dealerDF= dealerDF.withColumnRenamed("DLR_NAME", "DEALER_NAME")
    dealerDF= dealerDF.withColumnRenamed("RGN_CODE", "REGION")
    dealerDF= dealerDF.withColumnRenamed("AREA_CODE", "AREA")
    dealerDF= dealerDF.withColumnRenamed("OEM_CODE", "OEM")
    dealerDF= dealerDF.withColumnRenamed("DLR_ACTUAL_STATUS_CODE", "DLR_ACTUAL_STATUS")

    var resultDF= targetDF_Ser.join(dealerDF,targetDF_Ser.col("DEALER_CODE")===dealerDF.col("DLR_CODE"))

    targetDF_Ser = targetDF_Ser.select("DEALER_CODE", "DLR_NAME", "RGN_CODE", "AREA_CODE", "OEM_CODE", "DLR_ACTUAL_STATUS_CODE")
    resultDF = resultDF.select("DLR_CODE", "DEALER_NAME", "REGION", "AREA", "OEM", "DLR_ACTUAL_STATUS")




    assert(resultDF.except(targetDF_Ser).count == 0)
  }

/*  test("Target Invoice data is denormalized with Parts or not")
  {

    var targetDF_Ser = targetDF_RO_Parts


    partsDF= partsDF.withColumnRenamed("SOPT_PART_NUMBER", "PART_NUMBER")
    partsDF= partsDF.withColumnRenamed("PART_SPG_CODE", "PART_CODE")
    partsDF= partsDF.withColumnRenamed("PCAT_CODE", "PCATEGORY_CODE")



    var resultDF= targetDF_Ser.join(partsDF,targetDF_Ser.col("SOPT_PART_NUMBER")===partsDF.col("PART_NUMBER"))

    targetDF_Ser = targetDF_Ser.select("SOPT_PART_NUMBER", "PART_SPG_CODE", "PCAT_CODE")
    resultDF = resultDF.select("SOPT_PART_NUMBER", "PART_CODE", "PCATEGORY_CODE")




    assert(resultDF.except(targetDF_Ser).count == 0)
  }*/

/*  test("Target Invoice data is denormalized with parts_category_code or not")
  {

    var targetDFInv = targetInvoiceDf
    // targetDFST.show()
    parts_category_codeDF= parts_category_codeDF.withColumnRenamed("PCAT_CODE", "PCATEGORY_CODE")
    parts_category_codeDF= parts_category_codeDF.withColumnRenamed("PCAT_DESC", "PCATEGORY_DESC")
    parts_category_codeDF= parts_category_codeDF.withColumnRenamed("PCAT_MINOR_TACT_SEGMENT", "MINOR_SEGMENT")
    parts_category_codeDF= parts_category_codeDF.withColumnRenamed("PCAT_MAJOR_TACT_SEGMENT", "MAJOR_SEGMENT")
    parts_category_codeDF= parts_category_codeDF.withColumnRenamed("PCAT_PRODUCT_GROUP", "PRODUCT_GRP")


    var resultDF= targetDFInv.join(parts_category_codeDF,targetDFInv.col("DLR_CODE")===parts_category_codeDF.col("PCATEGORY_CODE"))

    targetDFInv = targetDFInv.select("PCAT_CODE", "PCAT_DESC", "PCAT_MINOR_TACT_SEGMENT", "PCAT_MAJOR_TACT_SEGMENT", "PCAT_PRODUCT_GROUP")
    resultDF = resultDF.select("PCAT_CODE", "PCATEGORY_DESC", "MINOR_SEGMENT", "MAJOR_SEGMENT", "PRODUCT_GRP")


    assert(resultDF.except(targetDFInv).count == 0)

  }*/

  test("Verify Source file  match with status dataframe for Text file")
  {

    println("Source")

    var statusDFS=statusDF

    println("Status")

    println(sourceDF.count())
    println(statusDFS.count())

    assert(sourceDF.count==statusDFS.count())
  }
  /*test( "Verify target DataFrame contains only success records")
  {
    var statusDFS=statusDF

  statusDFS=  statusDFS.filter(col("prc")===lit("SUCCESS"))
  statusDFS=statusDFS.drop("prc")
  statusDFS=statusDFS.drop("Inference")
  var outputDF=statusDFS.except(targetDF)
  assert(outputDF.count==0)
  }*/
  test("Verify Source file  match with status dataframe")
  {
    var sourceDFSS=  sourceDF.withColumnRenamed("_c0", "DEALER_CODE")
      .withColumnRenamed("_c1","RO_NUMBER")
//      .withColumnRenamed("_c2","RO_CLOSE_DATE")
//      .withColumnRenamed("_c3","RO_OPEN_DATE")
      .withColumnRenamed("_c4","CUST_CONTACT_ID")
      .withColumnRenamed("_c5","CUST_FIRST_NAME")
      .withColumnRenamed("_c6","CUST_MIDDLE_NAME")
      .withColumnRenamed("_c7","CUST_LAST_NAME")
      .withColumnRenamed("_c8","CUST_SALUTATION")
      .withColumnRenamed("_c9","CUST_SUFFIX")
      .withColumnRenamed("_c10","CUST_FULL_NAME")
      .withColumnRenamed("_c11","CUST_TITLE")
/*      .withColumnRenamed("_c12","CUST_BUS_PERSON_FLAG")
      .withColumnRenamed("_c13","CUST_COMPANY_NAME")
      .withColumnRenamed("_c14","CUST_DEPARTMENT")
      .withColumnRenamed("_c15","CUST_STREET_ADDR")
      .withColumnRenamed("_c16","CUST_DISTRICT")
      .withColumnRenamed("_c17","CUST_CITY")
      .withColumnRenamed("_c18","CUST_STATE")
      .withColumnRenamed("_c19","CUST_POSTAL_CODE")
      .withColumnRenamed("_c20","CUST_COUNTRY")
      .withColumnRenamed("_c21","CUST_HOME_PHONE_NUM")
      .withColumnRenamed("_c22","CUST_HOME_PHONE_EXT")
      .withColumnRenamed("_c23","CUST_HOME_PHONE_COUNTRY")
      .withColumnRenamed("_c24","CUST_BUS_PHONE_NUMBER")
      .withColumnRenamed("_c25","CUST_BUS_PHONE_EXT")
      .withColumnRenamed("_c26","CUST_BUS_PHONE_COUNTRY")
      .withColumnRenamed("_c27","CUST_PERS_EMAIL_ADDRESS")
      .withColumnRenamed("_c28","CUST_BUS_EMAIL_ADDRESS")
      .withColumnRenamed("_c29","CUST_BIRTH_DATE")
      .withColumnRenamed("_c30","CUST_ALLOW_SOLICITATION")
      .withColumnRenamed("_c31","CUST_ALLOW_PHONE_SOLICIT")
      .withColumnRenamed("_c32","CUST_ALLOW_EMAIL_SOLICIT")
      .withColumnRenamed("_c33","CUST_ALLOW_MAIL_SOLICIT")
      .withColumnRenamed("_c34","RO_ODOMETER_IN")
      .withColumnRenamed("_c35","RO_ODOMETER_OUT")
      .withColumnRenamed("_c36","RO_VEH_PICK_UP_DATE")
      .withColumnRenamed("_c37","RO_APPOINTMENT_FLAG")
      .withColumnRenamed("_c38","RO_DEPARTMENT")
      .withColumnRenamed("_c39","RO_EXT_SERV_CONTRACT_NAMES")
      .withColumnRenamed("_c40","RO_PAYMENT_METHODS")
      .withColumnRenamed("_c41","TOTAL_CUST_PARTS_PRICE")
      .withColumnRenamed("_c42","TOTAL_CUST_LABOR_PRICE")*/
    var statusDFSS=statusDF.select( "DEALER_CODE")/*,
      "RO_NUMBER",
     // "RO_CLOSE_DATE",
     // "RO_OPEN_DATE",
      "CUST_CONTACT_ID",
      "CUST_FIRST_NAME",
      "CUST_MIDDLE_NAME",
      "CUST_LAST_NAME",
      "CUST_SALUTATION",
      "CUST_SUFFIX",
      "CUST_FULL_NAME",
      "CUST_TITLE"),
      "CUST_BUS_PERSON_FLAG",
      "CUST_COMPANY_NAME",
      "CUST_DEPARTMENT",
      "CUST_STREET_ADDR")*/

    //var statusDFSSS = statusDFSS.sort(col("DEALER_CODE"))
    //statusDFSSS.show()

     var sourceDFSSS=sourceDFSS.select( "DEALER_CODE")
    /*,
        "RO_NUMBER",
      //  "RO_CLOSE_DATE",
      //  "RO_OPEN_DATE",
        "CUST_CONTACT_ID",
        "CUST_FIRST_NAME",
        "CUST_MIDDLE_NAME",
        "CUST_LAST_NAME",
        "CUST_SALUTATION",
        "CUST_SUFFIX",
        "CUST_FULL_NAME",
        "CUST_TITLE",
        "CUST_BUS_PERSON_FLAG",
        "CUST_COMPANY_NAME",
        "CUST_DEPARTMENT",
        "CUST_STREET_ADDR"*/

    //var sourceDFSSSS = sourceDFSSS.sort(col("DEALER_CODE"))
    //sourceDFSSSS.show()

    //print(statusDFSS.count( ))
    //print(sourceDFSSS.count())

    statusDFSS.show(false)
    sourceDFSSS.show(false)




       var outputDF = statusDFSS.except(sourceDFSSS)
    outputDF.show(100)
      assert(outputDF.count()==0)
  }


}
