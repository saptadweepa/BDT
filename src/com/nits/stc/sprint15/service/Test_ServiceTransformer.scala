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

    test(" total of 18 for SERVICE OPERATIONS with columns starting with SOPR_"){

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



  test(" total of 8 for Repair Order Technicians with columns starting with TECH_COL_"){

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

test(" total of 10 Repair Order Parts columns starting with PARTS_COL_"){

  var SOPT_COL = targetDF_RO_Parts.columns
  var count_res = 0
  for (i <- 0 to SOPT_COL.size - 1) {
    println(SOPT_COL(i))
    if(SOPT_COL(i).contains("SOPT_")){
      count_res = count_res +1
    }
  }

  assert(count_res == 10)
  
}



  test(" should generate columns RO_WARRANTY_FLAG, RO_INTERNAL_FLAG, RO_CUSTPAY_FLAG"){

    var RO_Columns1 = Seq("RO_WARRANTY_FLAG", "RO_INTERNAL_FLAG", "RO_CUSTPAY_FLAG")
    var RO_Tech_Col = targetDF_RO_Tech.columns
    println("columns of RO_Tech DF: ")
    RO_Tech_Col.foreach((element: String) => print(element + " "))
    println()
    RO_Columns1.foreach(element => assert(RO_Tech_Col.contains(element)))

    var RO_Columns = Seq("RO_WARRANTY_FLAG", "RO_INTERNAL_FLAG", "RO_CUSTPAY_FLAG")
    var RO_Parts_Col = targetDF_RO_Parts.columns
    println("columns of RO_Parts DF: ")
    RO_Parts_Col.foreach((element: String) => print(element + " "))
    println()
    RO_Columns.foreach(element => assert(RO_Parts_Col.contains(element)))

  }

  test("should set RO_WARRANTY_FLAG to Y when any of rows of operations belonging to an RO have WARRANTY_FLAG = Y"){

    var WARRANTY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("WARRANTY_FLAG")=== lit("Y")).count()
    var RO_WARRANTY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_WARRANTY_FLAG") === lit("Y")).count()
    assert(WARRANTY_FLAG_parts_Count == RO_WARRANTY_FLAG_parts_Count)

    var WARRANTY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("WARRANTY_FLAG")=== lit("Y")).count()
    var RO_WARRANTY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_WARRANTY_FLAG") === lit("Y")).count()
    assert(WARRANTY_FLAG_Tech_Count == RO_WARRANTY_FLAG_Tech_Count)

  }

  test("should set RO_INTERNAL_FLAG to Y when any of rows of operations belonging to an RO have INTERNAL_FLAG = Y"){

    var INTERNAL_FLAG_parts_Count = targetDF_RO_Parts.filter(col("INTERNAL_FLAG")=== lit("Y")).count()
    var RO_INTERNAL_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_INTERNAL_FLAG") === lit("Y")).count()
    assert(INTERNAL_FLAG_parts_Count == RO_INTERNAL_FLAG_parts_Count)

    var INTERNAL_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("INTERNAL_FLAG")=== lit("Y")).count()
    var RO_INTERNAL_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_INTERNAL_FLAG") === lit("Y")).count()
    assert(INTERNAL_FLAG_Tech_Count == RO_INTERNAL_FLAG_Tech_Count)


  }

  test("should set RO_CUSTPAY_FLAG to Y when any of rows of operations belonging to an RO have CUSTPAY_FLAG = Y"){

    var CUSTPAY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("CUSTPAY_FLAG")=== lit("Y")).count()
    var RO_CUSTPAY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_CUSTPAY_FLAG") === lit("Y")).count()
    assert(CUSTPAY_FLAG_parts_Count == RO_CUSTPAY_FLAG_parts_Count)

    var CUSTPAY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("CUSTPAY_FLAG")=== lit("Y")).count()
    var RO_CUSTPAY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_CUSTPAY_FLAG") === lit("Y")).count()
    assert(CUSTPAY_FLAG_Tech_Count == RO_CUSTPAY_FLAG_Tech_Count)

  }

/*  test("Count of  RO_CLASS_TYPE to EXP should not be zero"){

    var RO_CLASS_TYPE_parts_Count = targetDF_RO_Parts.filter(col("RO_CLASS_TYPE")=== lit("EXP")).count()
    println("RO_CLASS_TYPE == EXP count in RO_parts: "+RO_CLASS_TYPE_parts_Count)

    var RO_CLASS_TYPE_Tech_Count = targetDF_RO_Tech.filter(col("RO_CLASS_TYPE")=== lit("EXP")).count()
    println("RO_CLASS_TYPE == EXP count in RO_Tech: "+RO_CLASS_TYPE_Tech_Count)


  }

  test("Count of  RO_CLASS_TYPE to WKS should not be zero"){

    var RO_CLASS_TYPE_parts_Count = targetDF_RO_Parts.filter(col("RO_CLASS_TYPE")=== lit("WKS")).count()
    println("RO_CLASS_TYPE == WKS count in RO_parts: "+RO_CLASS_TYPE_parts_Count)

    var RO_CLASS_TYPE_Tech_Count = targetDF_RO_Tech.filter(col("RO_CLASS_TYPE")=== lit("WKS")).count()
    println("RO_CLASS_TYPE == WKS count in RO_Tech: "+RO_CLASS_TYPE_Tech_Count)


  }*/

  test("should set RO_WARRANTY_FLAG to N when any of rows of operations belonging to an RO have WARRANTY_FLAG = N"){

    var WARRANTY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("WARRANTY_FLAG")=== lit("N")).count()
    var RO_WARRANTY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_WARRANTY_FLAG") === lit("N")).count()
    assert(WARRANTY_FLAG_parts_Count == RO_WARRANTY_FLAG_parts_Count)

    var WARRANTY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("WARRANTY_FLAG")=== lit("N")).count()
    var RO_WARRANTY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_WARRANTY_FLAG") === lit("N")).count()
    assert(WARRANTY_FLAG_Tech_Count == RO_WARRANTY_FLAG_Tech_Count)

  }

  test("should set RO_INTERNAL_FLAG to N when any of rows of operations belonging to an RO have INTERNAL_FLAG = N"){

    var INTERNAL_FLAG_parts_Count = targetDF_RO_Parts.filter(col("INTERNAL_FLAG")=== lit("N")).count()
    var RO_INTERNAL_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_INTERNAL_FLAG") === lit("N")).count()
    assert(INTERNAL_FLAG_parts_Count == RO_INTERNAL_FLAG_parts_Count)

    var INTERNAL_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("INTERNAL_FLAG")=== lit("N")).count()
    var RO_INTERNAL_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_INTERNAL_FLAG") === lit("N")).count()
    assert(INTERNAL_FLAG_Tech_Count == RO_INTERNAL_FLAG_Tech_Count)


  }

  test("should set RO_CUSTPAY_FLAG to N when any of rows of operations belonging to an RO have CUSTPAY_FLAG = N"){

    var CUSTPAY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("CUSTPAY_FLAG")=== lit("N")).count()
    var RO_CUSTPAY_FLAG_parts_Count = targetDF_RO_Parts.filter(col("RO_CUSTPAY_FLAG") === lit("N")).count()
    assert(CUSTPAY_FLAG_parts_Count == RO_CUSTPAY_FLAG_parts_Count)

    var CUSTPAY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("CUSTPAY_FLAG")=== lit("N")).count()
    var RO_CUSTPAY_FLAG_Tech_Count = targetDF_RO_Tech.filter(col("RO_CUSTPAY_FLAG") === lit("N")).count()
    assert(CUSTPAY_FLAG_Tech_Count == RO_CUSTPAY_FLAG_Tech_Count)

  }

    


}
