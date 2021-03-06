package com.nits.stc.sprint13.invoice

import com.nits.etlcore.impl.{DFDefinition, DFExtractor}
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class Test_Schema_InvoiceAndCustomer extends FunSuite{
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




  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201103110306/*.avro"

  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Should apply schema on Invoice Dataframe as per invoiceSchema.json"){
    var path = uri + Config.readNestedKey(config, "SCHEMA")("INVOICE")
    var df = new DFDefinition()
    df.setSchema(path)
    println("Invoice Schema :")
    println(df.schema)
    targetInvoiceDf.show()
    assert(df.schema === targetInvoiceDf.schema)

  }

  test("Should apply schema on Customer Dataframe as per customerSchema.json"){
    var path = uri + Config.readNestedKey(config, "SCHEMA")("CUSTOMER")
    var df = new DFDefinition()
    df.setSchema(path)
    println("Customer Schema :")
    println(df.schema)
    assert(df.schema === targetCustomerDf.schema)

  }




}
