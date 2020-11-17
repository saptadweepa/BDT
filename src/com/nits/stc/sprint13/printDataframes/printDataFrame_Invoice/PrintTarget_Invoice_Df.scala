package com.nits.stc.sprint13.printDataframes.printDataFrame_Invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintTarget_Invoice_Df {
  def main(args:Array[String]){
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri ="hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path= "/config/invoice/invoiceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
    var options= Config.readNestedKey(config,"SOURCE")
    var Extractor=new DFExtractor()

    //target dfs
    var targetInvoicePath = Config.readNestedKey(config,"TARGET_INVOICE")("PATH")
    var targetCustomerPath = Config.readNestedKey(config,"TARGET_CUSTOMER")("PATH")

    var targetInvoiceDf = Extractor.extract(Resource.AVRO, uri + targetInvoicePath, null)
    var targetCustomerDf = Extractor.extract(Resource.AVRO, uri + targetCustomerPath, null)
    var temPartsMatching = Extractor.extract(Resource.AVRO, uri + "/temp/partsmatching", null)
    //temPartsMatching.show()
    targetInvoiceDf.show()
    println("count of record of target_InvoiceDf :"+targetInvoiceDf.count())

  }

}
