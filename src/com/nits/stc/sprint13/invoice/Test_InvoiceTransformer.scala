package com.nits.stc.sprint13.invoice

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

class Test_InvoiceTransformer extends FunSuite {
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020/"
  var path = "/config/invoice/invoiceConfig.json"
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
  var options= Config.readNestedKey(config,"SOURCE")

  var sourceType:Resource.ResourceType = Resource.withName(options("TYPE"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()

  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/invoice/*.txt",options)
  var partsDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/parts/*.avro", null)
  var parts_category_codeDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/parts_category_code/*.avro", null)

  /*
  var targetOptions =Config.readNestedKey(config, "TARGET_PATH")
  var targetDw_Parts = uri+targetOptions("INVOICE")
  var targetDF_Dw_Parts = Extractor.extract(Resource.AVRO,targetDw_Parts , null)
  var targetCustomer = uri+targetOptions("CUSTOMER")
  var targetDF_Customer = Extractor.extract(Resource.AVRO,targetCustomer , null)
  */

  //target dfs
  var targetInvoicePath = Config.readNestedKey(config,"TARGET_INVOICE")("PATH")
  var targetCustomerPath = Config.readNestedKey(config,"TARGET_CUSTOMER")("PATH")

  var targetInvoiceDf = Extractor.extract(Resource.AVRO, uri + targetInvoicePath, null)
  var targetCustomerDf = Extractor.extract(Resource.AVRO, uri + targetCustomerPath, null)

  var tempPartsMatching = Extractor.extract(Resource.AVRO, uri + "/temp/partsmatching", null)



  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201103110306/*.avro"

  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  //Tests for invoice Transformer

  test("Target DataFrame should contain ADD_EXISTING_INVOICE Columns"){

    var addExistingInvoice_col= Seq("DLR_CODE","INV_NUMBER","INV_CLOSE_DATE","ITM_PART_NUMBER","TRANS_TYPE","ITM_LINE_NUM")
    var targetInvoiceDf_col= targetInvoiceDf.columns;
    println("columns of Target_Invoice: ")
    targetInvoiceDf_col.foreach((element:String)=>print(element+ " "))
    println()
    addExistingInvoice_col.foreach(element => assert(targetInvoiceDf_col.contains(element)))

  }


  test("Target Invoice_Dataframes should contain Audit Columns"){

    var auditColumns_Invoice= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var targetInvoiceDf_col= targetInvoiceDf.columns;
    println("columns of Target_Invoice: ")
    targetInvoiceDf_col.foreach((element:String)=>print(element+ " "))
    println()
    auditColumns_Invoice.foreach(element => assert(targetInvoiceDf_col.contains(element)))


  }

  test("Column should be renamed as per invoiceRenameColumns.json"){
    var renameOptions= Config.readNestedKey(config,"RENAME_COLUMNS")
    var invoicePath = uri + renameOptions("INVOICE")
    var renameColumns = Config.readConfig(invoicePath).params.asInstanceOf[Map[String, String]]
    println(invoicePath)

    var targetCol_Inv = targetInvoiceDf.columns;
    targetCol_Inv.foreach(value => println(value)) // for printing all columns of statusDf we need to use foreach loop, as statusCol will contain an array
    renameColumns.values.foreach(value => assert(targetCol_Inv.contains(value)))

  }

  test("Column should be renamed as per customerRenameColumns.json"){
    var renameOptions= Config.readNestedKey(config,"RENAME_COLUMNS")
    var customerPath = uri + renameOptions("CUSTOMER")
    var renameColumns = Config.readConfig(customerPath).params.asInstanceOf[Map[String, String]]
    println(customerPath)

    var targetCol_Cus = targetCustomerDf.columns;
    targetCol_Cus.foreach(value => println(value)) // for printing all columns of statusDf we need to use foreach loop, as statusCol will contain an array
    renameColumns.values.foreach(value => assert(targetCol_Cus.contains(value)))

    //renameColumns.values.foreach(value => print(targetCol_Cus.contains(value)+ " "+value))

  }

  test("status DataFrame should contain STATUS Columns"){

    var status_col= Seq("prc","inference","CUST_ELIG_FLAG","CUST_D2D_OVERRIDE_FLAG","CLS_CODE","CUST_RET_WHOLESALE_FLAG","CUST_MATCHING_KEY","CUST_MATCHING_PHONE")
    var statusInvoiceDf_col= statusDF.columns;
    println("columns of Target_Invoice: ")
    statusInvoiceDf_col.foreach((element:String)=>print(element+ " "))
    println()
    status_col.foreach(element => assert(statusInvoiceDf_col.contains(element)))

  }

  test("Target Data Frame of Invoice should contain dealer denormalized columns"){
    var columns = targetInvoiceDf.columns
    assert(
      columns.contains("DLR_CODE") &&
        columns.contains("DLR_NAME") &&
        columns.contains("RGN_CODE") &&
        columns.contains("AREA_CODE") &&
        columns.contains("OEM_CODE") &&
        columns.contains("DLR_ACTUAL_STATUS_CODE"))
  }

  test("Target Data Frame of Invoice should contain parts denormalized columns"){
    var columns = targetInvoiceDf.columns
    assert(
        columns.contains("ITM_PART_NUMBER") &&
        columns.contains("PART_DESCRIPTION") &&
        columns.contains("PART_INTG") &&
        columns.contains("PCAT_CODE") &&
        columns.contains("PART_SPG"))
  }

  test("Target Data Frame of Invoice should contain parts_category_code denormalized columns"){
    var columns = targetInvoiceDf.columns
    assert(
      columns.contains("PCAT_CODE") &&
        columns.contains("PCAT_DESC") &&
        columns.contains("PCAT_MINOR_TACT_SEGMENT") &&
        columns.contains("PCAT_MAJOR_TACT_SEGMENT") &&
        columns.contains("PCAT_PRODUCT_GROUP")  )
  }

   test("Target Invoice data is denormalized with Dealers or not")
   {

  var targetDFInv = targetInvoiceDf
   // targetDFST.show()
    dealerDF= dealerDF.withColumnRenamed("DLR_CODE", "DEALER_CODE")
   dealerDF= dealerDF.withColumnRenamed("DLR_NAME", "DEALER_NAME")
   dealerDF= dealerDF.withColumnRenamed("RGN_CODE", "REGION")
   dealerDF= dealerDF.withColumnRenamed("AREA_CODE", "AREA")
   dealerDF= dealerDF.withColumnRenamed("OEM_CODE", "OEM")
    dealerDF= dealerDF.withColumnRenamed("DLR_ACTUAL_STATUS_CODE", "DLR_ACTUAL_STATUS")

    var resultDF= targetDFInv.join(dealerDF,targetDFInv.col("DLR_CODE")===dealerDF.col("DEALER_CODE"))

     targetDFInv = targetDFInv.select("DLR_CODE", "DLR_NAME", "RGN_CODE", "AREA_CODE", "OEM_CODE", "DLR_ACTUAL_STATUS_CODE")
     resultDF = resultDF.select("DLR_CODE", "DEALER_NAME", "REGION", "AREA", "OEM", "DLR_ACTUAL_STATUS")




     assert(resultDF.except(targetDFInv).count == 0)
   }

  test("Target Invoice data is denormalized with Parts or not")
  {

    var targetDFInv = targetInvoiceDf
    // targetDFST.show()
    partsDF= partsDF.withColumnRenamed("PART_NUMBER", "ITEM_PART_NUMBER")
    partsDF= partsDF.withColumnRenamed("PART_DESCRIPTION", "PART_DESC")
    partsDF= partsDF.withColumnRenamed("PART_INTG", "PART_INTEGRAL")
    partsDF= partsDF.withColumnRenamed("PCAT_CODE", "PCATEGORY_CODE")
    partsDF= partsDF.withColumnRenamed("PART_SPG", "PART_SPECIFICATION")


    var resultDF= targetDFInv.join(partsDF,targetDFInv.col("ITM_PART_NUMBER")===partsDF.col("ITEM_PART_NUMBER"))

    targetDFInv = targetDFInv.select("ITM_PART_NUMBER", "PART_DESCRIPTION", "PART_INTG", "PCAT_CODE", "PART_SPG")
    resultDF = resultDF.select("ITM_PART_NUMBER", "PART_DESC", "PART_INTEGRAL", "PCATEGORY_CODE", "PART_SPECIFICATION")




    assert(resultDF.except(targetDFInv).count == 0)
  }

  test("Target Invoice data is denormalized with parts_category_code or not")
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

  }

  test("All Dates should be in LongType"){

    var statusDf_Schema = statusDF.schema;
    var fields = statusDf_Schema.fields
    for(i <- 0 to statusDf_Schema.fields.size -1 ) {
      if(statusDf_Schema.fields(i).name.contains("_DATE")){
        assert(statusDf_Schema.fields(i).dataType === LongType)
      }
    }

  }

  test("YEAR, YEARMONTH, YEARQTR should be populated properly"){

    statusDF.select(col("INV_CLOSE_DATE")).withColumn("NEW_YEAR",year(from_unixtime(col("INV_CLOSE_DATE")/1000))).show()

    statusDF.select(col("INV_CLOSE_DATE"), col("YEARMONTH")).withColumn("new_year_month",
      concat(year(from_unixtime(col("INV_CLOSE_DATE")/1000)),lit("0"),
        month(from_unixtime(col("INV_CLOSE_DATE")/1000)))
    ).show()

    statusDF.select(col("INV_CLOSE_DATE")).withColumn("NEW_YEARQTR",quarter(from_unixtime(col("INV_CLOSE_DATE")/1000))).show()

    assert(statusDF.filter(
      col("YEAR") =!= year(from_unixtime(col("INV_CLOSE_DATE")/1000))   //to check YEAR
    ).count()==0)

    assert(statusDF.filter(
      col("YEARMONTH") =!= concat(year(from_unixtime(col("INV_CLOSE_DATE")/1000)),lit("0"),  //to check YEARMONTH
      month(from_unixtime(col("INV_CLOSE_DATE")/1000)))
    ).count()==0)

    assert(statusDF.filter(
      col("YEARQTR") =!= quarter(from_unixtime(col("INV_CLOSE_DATE")/1000)) //to check YEARQTR
    ).count()==0)
  }


  test("ADD_EXISTING_INVOICE columns should not be null"){

    for(i <- 0 to statusDF.schema.fields.size -1 ) {
      if(statusDF.schema.fields(i).name.contains("CI_")){
        assert(statusDF.filter(col(statusDF.schema.fields(i).name).isNotNull).count()!=0)
      }
    }

  }

  test("Verify Data in string Column should be in UPPER Case"){
    //Customer DF
    var newDf = targetCustomerDf.withColumn("UPPER_CUST_FIRST_NAME", upper(col("CUST_FIRST_NAME")))
    newDf = newDf.withColumn("UPPER_CUST_CITY", upper(col("CUST_CITY")))
    newDf = newDf.withColumn("UPPER_CUST_PERSONAL_EMAIL", upper(col("CUST_PERSONAL_EMAIL")))


    newDf = newDf.filter(col("UPPER_CUST_FIRST_NAME").isNotNull &&
      col("UPPER_CUST_CITY").isNotNull &&
      col("UPPER_CUST_PERSONAL_EMAIL").isNotNull)

    assert(newDf.filter(col("UPPER_CUST_FIRST_NAME") === col("CUST_FIRST_NAME") ).count()==newDf.count())
    assert(newDf.filter(col("UPPER_CUST_CITY") === col("CUST_CITY") ).count()==newDf.count())
    assert(newDf.filter(col("UPPER_CUST_PERSONAL_EMAIL") === col("CUST_PERSONAL_EMAIL") ).count()==newDf.count())

    //Invoice DF TRANS_TYPE BILL_FIRST_NAME BILL_COMPANY_NAME

    var newInvoiceDf = targetInvoiceDf.withColumn("U_TRANS_TYPE", upper(col("TRANS_TYPE")))
    newInvoiceDf = newInvoiceDf.withColumn("U_BILL_FIRST_NAME", upper(col("BILL_FIRST_NAME")))
    newInvoiceDf = newInvoiceDf.withColumn("U_BILL_COMPANY_NAME", upper(col("BILL_COMPANY_NAME")))

    newInvoiceDf = newInvoiceDf.filter(col("U_TRANS_TYPE").isNotNull &&
      col("U_BILL_FIRST_NAME").isNotNull &&
      col("U_BILL_COMPANY_NAME").isNotNull
    )

    assert(newInvoiceDf.filter(col("U_TRANS_TYPE") === col("TRANS_TYPE")).count() == newInvoiceDf.count())
    assert(newInvoiceDf.filter(col("U_BILL_FIRST_NAME") === col("BILL_FIRST_NAME")).count() == newInvoiceDf.count())
    assert(newInvoiceDf.filter(col("U_BILL_COMPANY_NAME") === col("BILL_COMPANY_NAME")).count() == newInvoiceDf.count())


  }


  test("Cleansing: Part Number should not contain  - or \" or space "){
    assert(statusDF.filter(col("PART_NUMBER").contains("-")||col("PART_NUMBER").contains("\"")||col("PART_NUMBER").contains(" ")
      && col("prc") === lit("SUCCESS")).count() == 0)
  }


/*  test("Temp Parts should contain join data from parts and parts category code"){

    var df = partsDF.join(parts_category_codeDF, partsDF.col("PCAT_CODE")===parts_category_codeDF.col("PCAT_CODE"))
    df = df.join(targetInvoiceDf,df.col("SPG_DESC")===targetInvoiceDf.col("SPG_DESC"))
    df.show()
    //var expectedDF = tempPartsMatching.show()


  } */

  test("statusDf contain APPLY_PARTS_MATCHING columns should not be null"){
    assert(statusDF.select("PART_NUMBER","PCAT_CODE","PART_INTG","PCAT_PRODUCT_GROUP","ITM_PART_NUMBER",
      "ITM_PART_NUMBER_DMS","ITM_MANUFACTURER_DMS","ITM_OEM_SALE_FLAG").count()!=0)

  }

  test("APPLY_PARTS_MATCHING columns should not contain suffix \"DSP\" and prefix \"DT\""){

    var ITM_PART_NUMBER_DF = statusDF.filter(col("ITM_PART_NUMBER").contains("DSP") && col("ITM_PART_NUMBER").contains("DT"))
    assert(ITM_PART_NUMBER_DF.count() == 0)

  }

  test("APPLY_PARTS_MATCHING columns should not contain DMS_PREFIXES \"PK\""){

    var ITM_PART_NUMBER_DF = statusDF.filter(col("ITM_PART_NUMBER").contains("PK")&& col("prc") === lit("SUCCESS"))
    assert(ITM_PART_NUMBER_DF.count() == 0)
  //Complete this test

  }




}
