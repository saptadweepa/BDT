package com.nits.stc.invoice

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

class Test_CustomerTransformer extends FunSuite {
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
 /* var targetOptions =Config.readNestedKey(config, "TARGET_PATH")
  var targetDw_Parts = uri+targetOptions("INVOICE")
  var targetDF_Dw_Parts = Extractor.extract(Resource.AVRO,targetDw_Parts , null)
  var targetCustomer = uri+targetOptions("CUSTOMER")
  var targetDF_Customer = Extractor.extract(Resource.AVRO,targetCustomer , null)*/

  var targetInvoicePath = Config.readNestedKey(config,"TARGET_INVOICE")("PATH")
  var targetCustomerPath = Config.readNestedKey(config,"TARGET_CUSTOMER")("PATH")

  var targetInvoiceDf = Extractor.extract(Resource.AVRO, uri + targetInvoicePath, null)
  var targetDF_Customer = Extractor.extract(Resource.AVRO, uri + targetCustomerPath, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201103110306/*.avro"


  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Target Customer_DF should contain audit columns"){

    var auditColumns_Customer= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var targetCustomerDf_col= targetDF_Customer.columns;
    println("columns of Target_Customer: ")
    targetCustomerDf_col.foreach((element:String)=>print(element+ " "))
    println()
    auditColumns_Customer.foreach(element => assert(targetCustomerDf_col.contains(element)))

/*   var created_ByCount =  targetDF_Customer.filter(col("CREATED_BY").isNotNull).count()
    var created_DateCount =  targetDF_Customer.filter(col("CREATED_DATE").isNotNull).count()
    var updated_ByCount =  targetDF_Customer.filter(col("UPDATED_BY").isNotNull).count()
    var updated_DateCount =  targetDF_Customer.filter(col("UPDATED_DATE").isNotNull).count()
    var deleted_ByCount =  targetDF_Customer.filter(col("DELETED_BY").isNotNull).count()
    var deleted_DateCount =  targetDF_Customer.filter(col("DELETED_DATE").isNotNull).count()
    println("count of created_By: "+created_ByCount)
    println("count of created_Date: "+created_DateCount)
    println("count of updated_By: "+updated_ByCount)
    println("count of updated_Date: "+updated_DateCount)
    println("count of deleted_By: "+deleted_ByCount)

    assert(created_ByCount !=0)
    assert(created_DateCount !=0)
    assert(updated_ByCount !=0)
    assert(updated_DateCount !=0)
    assert(deleted_ByCount !=0)
    */

  }

    test("should add prefix PAIQ00 to CUST_NUMBER if CUST_NUMBER,CUST_FULL_NAME,CUST_ADDRESS,CUST_CITY,CUST_STATE and CUST_BUS_PHONE is null"){

      var actualDf = targetDF_Customer.filter(col("CUST_NUMBER").contains("PAIQ00"))

      var expectedDf= actualDf.filter(
          col("CUST_FULL_NAME").isNull ||
            col("CUST_ADDRESS").isNull ||
            col("CUST_CITY").isNull ||
            col("CUST_STATE").isNull ||
            col("CUST_BUS_PHONE").isNull
      )

      assert(actualDf.count() == expectedDf.count())

    }

  test("should add prefix PAIQ to CUST_NUMBER if CUST_NUMBER is null and any of the CUST_FULL_NAME,CUST_ADDRESS,CUST_CITY,CUST_STATE and CUST_BUS_PHONE is not null "){


    var actualDf = targetDF_Customer.filter(col("CUST_NUMBER").contains("PAIQ") && !col("CUST_NUMBER").contains("PAIQ00"))

    var expectedDf= actualDf.filter(
      col("CUST_FULL_NAME").isNotNull ||
        col("CUST_ADDRESS").isNotNull ||
        col("CUST_CITY").isNotNull ||
        col("CUST_STATE").isNotNull ||
        col("CUST_BUS_PHONE").isNotNull
    )

    assert(actualDf.count() == expectedDf.count())


  }


}
