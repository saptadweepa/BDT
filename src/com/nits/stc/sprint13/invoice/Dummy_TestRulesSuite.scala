package com.nits.stc.sprint13.invoice

  import com.nits.etlcore.impl.{ DFDefinition, DFExtractor }
  import org.apache.spark.sql.types._
  import org.apache.spark.sql._

  import com.nits.global.Resource
  import com.nits.util.Config
  import org.apache.spark.sql.SparkSession
  import org.scalatest.FunSuite

  class Dummy_TestRulesSuite extends FunSuite {

    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020/"
    var path = "/config/invoice/invoiceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]]
    var options = Config.readNestedKey(config, "SOURCE")

    var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
    var sourcePath = uri + options("PATH")
    var Extractor = new DFExtractor()

    var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
    var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/invoice/*.txt",options)
    var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
    var target_Parts = uri + targetOptions("TARGET_PATH")
    var targetDF_Parts = Extractor.extract(Resource.AVRO, target_Parts, null)
    val statusOptions = Config.readNestedKey(config, "STATUS")
   // var statusPath = uri + "status/partspurchases/STATUS_20201006133845/*.avro"
    //println(statusPath)
    var statusCols = statusOptions("COLUMNS").split(",").toSeq
    //var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

    test("Should apply schema on Invoice Dataframe as per invoiceSchema.json"){
      var path = uri + Config.readNestedKey(config,"SCHEMA")
      var df = new DFDefinition()
      df.setSchema(path)
      println(df.schema)
      assert(df.schema == targetDF_Parts.schema)
    }

    /*test("Dealer Name is Unknown") {
      val count = statusDF.filter((col("DLR_NAME").isNull)
        && (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Dealer Name is Unknown Inference") {
      val inf_Df = statusDF.filter((col("DLR_NAME").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference").contains("Dealer Name is Unknown")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Dealer code is Unknown") {
      //Invoice Total Cost is Null
      statusDF.show()
      val count = statusDF.filter((col("DLR_CODE").isNull)
        && (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Dealer code is Unknown Inference") {
      val inf_Df = statusDF.filter((col("DLR_CODE").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference").contains("Dealer code is Unknown")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Purchase Quantity is null") {
      val count = statusDF.filter((col("DLR_CODE").isNull)
        && (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Purchase Quantity is null Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_QTY").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference").contains("Purchase Quantity is null")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Warranty Quantity and Audi Warranty Quantity is null") {
      val count = statusDF.filter((col("DPR_WARRANTY_QTY").isNull) &&
        (col("DPR_WARRANTY_QTY").isNull) &&
        (col("DPR_AUDI_WARRANTY_QTY").isNull) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Warranty Quantity and Audi Warranty Quantity is null Inference") {
      val inf_Df = statusDF.filter((col("DPR_WARRANTY_QTY").isNull) &&
        (col("DPR_AUDI_WARRANTY_QTY").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference").contains("Warranty Quantity and Audi Warranty Quantity is null")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Return Quantity is null") {
      val count = statusDF.filter((col("DPR_RETURN_QTY").isNull) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Return Quantity is null Inference") {
      val inf_Df = statusDF.filter((col("DPR_RETURN_QTY").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference").contains("Return Quantity is null")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Purchase Quantity,Warranty Quantity,Audi Warranty Quantity and Return Quantity is equal to 0") {
      val count = statusDF.filter((col("DPR_PURCHASE_QTY") === 0.0) &&
        (col("DPR_WARRANTY_QTY") === 0.0) &&
        (col("DPR_AUDI_WARRANTY_QTY") === 0.0) &&
        (col("DPR_RETURN_QTY") === 0.0) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Purchase Quantity,Warranty Quantity,Audi Warranty Quantity and Return Quantity is equal to 0 Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_QTY") === 0.0) &&
        (col("DPR_WARRANTY_QTY") === 0.0) &&
        (col("DPR_AUDI_WARRANTY_QTY") === 0.0) &&
        (col("DPR_RETURN_QTY") === 0.0))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("Purchase Quantity,Warranty Quantity,Audi Warranty Quantity and Return Quantity is equal to 0")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Dealer Code is null") {
      val count = statusDF.filter((col("DLR_CODE").isNull) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Dealer Code is null Inference") {
      val inf_Df = statusDF.filter((col("DLR_CODE").isNull))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("Dealer Code is null")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Condition 1:Quantity/Price Mismatch") {
      val count = statusDF.filter((col("DPR_PURCHASE_QTY") === 0.0) &&
        (col("DPR_PURCHASE_AMOUNT") < 0.0) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Condition 1:Quantity/Price Mismatch Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_QTY") === 0.0) &&
        (col("DPR_PURCHASE_AMOUNT") < 0.0))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("DPR_PURCHASE_QTY == 0.0 and DPR_PURCHASE_AMOUNT < 0.0")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Condition 2:Quantity/Price Mismatch") {
      val count = statusDF.filter((col("DPR_PURCHASE_AMOUNT") === 0.0) &&
        (col("DPR_PURCHASE_QTY") < 0.0) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Condition 2:Quantity/Price Mismatch Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_AMOUNT") === 0.0) &&
        (col("DPR_PURCHASE_QTY") < 0.0))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("DPR_PURCHASE_QTY < 0.0 and DPR_PURCHASE_AMOUNT == 0.0")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Condition 1:Quantity/Price Parity") {
      val count = statusDF.filter((col("DPR_PURCHASE_AMOUNT") < 0.0) &&
        (col("DPR_PURCHASE_QTY") > 0.0) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Condition 1:Quantity/Price Parity Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_AMOUNT") < 0.0) &&
        (col("DPR_PURCHASE_QTY") > 0.0))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("DPR_PURCHASE_QTY > 0.0 and DPR_PURCHASE_AMOUNT < 0.0")).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("Condition 2:Quantity/Price Parity") {
      val count = statusDF.filter((col("DPR_PURCHASE_QTY") < 0.0) &&
        (col("DPR_PURCHASE_QTY") > 0.0) &&
        (col("PRC") !== lit("REJECTED"))).count()
      assert(count == 0)
    }
    test("Condition 2:Quantity/Price Parity Inference") {
      val inf_Df = statusDF.filter((col("DPR_PURCHASE_AMOUNT") > 0.0) &&
        (col("DPR_PURCHASE_QTY") < 0.0))
      val inf_Count1 = inf_Df.count()
      val inf_Count2 = inf_Df.filter(col("inference")
        .contains("DPR_PURCHASE_QTY < 0.0 and DPR_PURCHASE_AMOUNT > 0.0")).count()
      assert(inf_Count1 == inf_Count2)
    }
    //statusDF.printSchema()
    test("set DPR_WARRANTY_QTY, DPR_WARRANTY_AMOUNT - condition 1") {
      val df = statusDF.filter((col("DUAL_DLR_CODE").isNotNull) &&
        (col("DIVISION_BRAND") === lit("A")))
      val inf_Count1 = df.count()
      val inf_Count2 = df.filter((col("DPR_WARRANTY_QTY").cast(LongType) === col("DPR_AUDI_WARRANTY_QTY").cast(LongType)) && (col("DPR_WARRANTY_AMOUNT").cast(LongType) === col("DPR_AUDI_WARRANTY_AMOUNT").cast(LongType))).count()
      assert(inf_Count1 == inf_Count2)
    }
    test("set DPR_WARRANTY_QTY, DPR_WARRANTY_AMOUNT - condition 2") {
      val df = statusDF.filter((col("DUAL_DLR_CODE").isNull) &&
        (col("OEM_CODE") === lit("AU")) &&
        (col("DPR_MATERIAL_GRP5") === lit("AU")) &&
        (col("DPR_MATERIAL_GRP5") !== lit("60")) &&
        (col("DPR_MATERIAL_GRP5") !== lit("60A")))
      val inf_Count1 = df.count()
      print(inf_Count1)

      val inf_Count2 = df.filter((col("DPR_WARRANTY_QTY").cast(LongType) === col("DPR_AUDI_WARRANTY_QTY").cast(LongType)) &&
        (col("DPR_WARRANTY_AMOUNT").cast(LongType) === col("DPR_AUDI_WARRANTY_AMOUNT").cast(LongType))).count()
      assert(inf_Count1 == inf_Count2)
    }

    test("Verify No Date & timeStamp Column") {
      var value = false
      val columnDataTypes: Array[String] = statusDF.schema.fields.map(x => x.dataType).map(x => x.toString)
      val loop = new Breaks;

      loop.breakable {
        for (myTypes <- columnDataTypes) {
          if ((myTypes == lit("Date")) || (myTypes == lit("timestamp"))) {
            value = true
            System.out.println(myTypes + " " + value)
          } else {
            value = false
            System.out.println(myTypes + " " + value)
          }
          if (value == true) {
            loop.break()
          }
        }
      }
    }*/


}
