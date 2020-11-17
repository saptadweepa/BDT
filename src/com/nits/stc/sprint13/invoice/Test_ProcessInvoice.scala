package com.nits.stc.sprint13.invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class Test_ProcessInvoice extends FunSuite {
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



  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201103110306/*.avro"

  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("should have same no. records of status records as SOURCE data "){
    assert(sourceDF.count() == statusDF.count())
  }

  test("should contain INV_NUMBER,INV_CLOSE_DATE,ITM_LINE_NUM column in invoice dataset Customer data"){
    assert(targetInvoiceDf.select("INV_NUMBER").count()!=0)
    assert(targetInvoiceDf.select("INV_CLOSE_DATE").count()!=0)
    assert(targetInvoiceDf.select("ITM_LINE_NUM").count()!=0)
  }

  test("should contains CUST_ & CLS_CODE columns in Customer Dataset "){
    var i,count =0;
    for(i<-0 to targetCustomerDf.schema.fields.size-1){
      if(targetCustomerDf.schema.fields(i).name.contains("CUST_")){
        count = count + 1;
      }
    }

    assert(count>0)
    assert(targetCustomerDf.select("CLS_CODE").count()!=0)

  }

}
