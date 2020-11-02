package com.nits.stc.invoice

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
  var targetOptions =Config.readNestedKey(config, "TARGET_PATH")
  var targetDw_Parts = uri+targetOptions("INVOICE")
  var targetDF_Dw_Parts = Extractor.extract(Resource.AVRO,targetDw_Parts , null)
  var targetCustomer = uri+targetOptions("CUSTOMER")
  var targetDF_Customer = Extractor.extract(Resource.AVRO,targetCustomer , null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201026161100/*.avro"

  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  //Tests for invoice Transformer

  test("status DataFrame should contain DLR_CODE,INV_NUMBER,INV_CLOSE_DATE,ITM_PART_NUMBER,ITM_LINE_NUM"){
    var addExistingInvoice_col= Seq("DLR_CODE","INV_NUMBER","INV_CLOSE_DATE","ITM_PART_NUMBER","ITM_LINE_NUM")
    var Target_DwParts_col= targetDF_Dw_Parts.columns;
    println("columns of Target_Invoice: ")
    Target_DwParts_col.foreach((element:String)=>print(element+ " "))
    println()
    addExistingInvoice_col.foreach(element => assert(Target_DwParts_col.contains(element)))

  }


  test("Target_Dataframes should contain Audit Columns"){

    var auditColumns_Invoice= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var Target_DwParts_col= targetDF_Dw_Parts.columns;
    println("columns of Target_Invoice: ")
    Target_DwParts_col.foreach((element:String)=>print(element+ " "))
    println()
    auditColumns_Invoice.foreach(element => assert(Target_DwParts_col.contains(element)))

    var auditColumns_Customer= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var Target_Customer_col= targetDF_Customer.columns;
    println("columns of Target_Customer: ")
    Target_Customer_col.foreach((element:String)=>print(element+ " "))
    println()
    auditColumns_Customer.foreach(element => assert(Target_Customer_col.contains(element)))
  }

  test("Column should be renamed as per invoiceRenameColumns.json"){
    var renameOptions= Config.readNestedKey(config,"RENAME_COLUMNS")
    var invoicePath = uri + renameOptions("INVOICE")
    var renameColumns = Config.readConfig(invoicePath).params.asInstanceOf[Map[String, String]]
    println(invoicePath)

    var targetCol_Inv = targetDF_Dw_Parts.columns;
    targetCol_Inv.foreach(value => println(value)) // for printing all columns of statusDf we need to use foreach loop, as statusCol will contain an array
    renameColumns.values.foreach(value => assert(targetCol_Inv.contains(value)))

  }

  test("Column should be renamed as per customerRenameColumns.json"){
    var renameOptions= Config.readNestedKey(config,"RENAME_COLUMNS")
    var customerPath = uri + renameOptions("CUSTOMER")
    var renameColumns = Config.readConfig(customerPath).params.asInstanceOf[Map[String, String]]
    println(customerPath)

    var targetCol_Cus = targetDF_Customer.columns;
    targetCol_Cus.foreach(value => println(value)) // for printing all columns of statusDf we need to use foreach loop, as statusCol will contain an array
    renameColumns.values.foreach(value => assert(targetCol_Cus.contains(value)))
    //renameColumns.values.foreach(value => print(targetCol_Cus.contains(value)+ " "+value))

  }



}
