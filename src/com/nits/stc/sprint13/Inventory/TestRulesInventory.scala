package com.nits.stc.sprint13.Inventory

import com.nits.util._
import com.nits.global._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.inventory._
import org.scalatest._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.HashMap
import org.apache.spark.sql.functions.array

class TestRulesInventory extends FunSuite {
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var uri ="hdfs://sanjeev-HP-240-G4-Notebook-PC:8020/"
  var path="/config/inventory/inventoryConfig.json"

  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
  var options: Map[String, String] = Map("delimiter" -> "|", "inferSchema" -> "true")

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH")
  val statusCols = statusOptions("COLUMNS").split(",").toSeq
  val statusColumn = statusOptions("STATUS_COLUMN")

  var Extractor=new DFExtractor()
  var statusDF=  Extractor.extract(Resource.AVRO,uri+"/status/inventory/STATUS_20201012142945",options)
  //var statusDF=  Extractor.extract(Resource.AVRO, statusPath,options)
  var targetdf=Extractor.extract(Resource.AVRO,uri +config("TARGET_PATH"), null)
  // statusDF= statusDF.filter(!col("DLR_CODE").contains("402A01"))e
  statusDF.show()


  test("Rule 1: Verify Part Number is null with inference"){

    val firstRes=statusDF.filter(col("PART_NUMBER").isNull)
    firstRes.select ("PART_NUMBER").show()
    val expected=firstRes.count()
    val actual=firstRes.filter(col("inference").contains("PART_NUMBER is null")).count()
    assert(expected==actual)
  }


  test("Rule 3: DLI_PART_NUMBER and PART_NUMBER is null"){

    val firstRes=statusDF.filter(col("DLI_PART_NUMBER").isNull && col("PART_NUMBER").isNull)
    firstRes.select ("PART_NUMBER","DLI_PART_NUMBER").show()
    val expected=firstRes.count()

    val actual=firstRes.filter(col("inference").contains("DLI_PART_NUMBER and PART_NUMBER is null")).count()
    assert(expected==actual)
    //assert(inf_count1==inf_count)
  }

  test("Rule 4: Verify DLI_PART_NUMBER is not null and PART_NUMBER is null "){


    val inf_df=statusDF.filter((col("DLI_PART_NUMBER").isNotNull && col("PART_NUMBER").isNull) && (col("prc")=!= lit("REJECTED") ))
    val expected=inf_df.count()

    val actual=inf_df.filter(col("inference").contains("DLI_PART_NUMBER is not null and PART_NUMBER is null")).count()
    assert(expected==actual)
    //assert(inf_count==0)
  }

  test("Rule 5 :PART_NUMBER is not null but Quantity on hand is null or zero"){

    val test_df=statusDF.filter((col("PART_NUMBER").isNotNull && col("DLI_QTY_ONHAND").isNull && col("SR")=!=1))

    val expected=test_df.count()
    val actual=test_df.filter(col("inference").contains("PART_NUMBER is not null but Quantity on hand is null or zero")).count()

    assert(expected==actual)

  }
  test("Rule 6: PART_NUMBER is not null but Quantity on hand is null or zero"){

    val test_df=statusDF.filter((col("PART_NUMBER").isNotNull && col("DLI_QTY_ONHAND")==0 && col("SR")=!=1))

    val expected=test_df.count()
    val actual=test_df.filter(col("inference").contains("PART_NUMBER is not null but Quantity on hand is null or zero")).count()

    assert(expected==actual)
  }

  test("Rule 7:Verify Part Number is null"){

    val firstRes=statusDF.filter(col("SR")>1 && col("PART_NUMBER").isNull)
    firstRes.select ("PART_NUMBER").show()
    val expected=firstRes.count()

    val actual=firstRes.filter(col("inference").contains("PART_NUMBER is null.")).count()

    assert(expected==actual)
    //assert(expected==0)
  }


  test("Rule 8:PART_NUMBER is null and DLI_PART_NUMBER is null"){

    val firstRes=statusDF.filter(col("SR")>1 && col("PART_NUMBER").isNull && col("DLI_PART_NUMBER").isNull)
    val expected=firstRes.count()


    val actual=firstRes.filter(col("inference").contains("PART_NUMBER is null and DLI_PART_NUMBER is null")).count()
    assert(expected==actual)

  }

  test("Rule 9:PART_NUMBER is null and DLI_PART_NUMBER is not null"){

    val firstRes=statusDF.filter(col("SR")>1 && col("PART_NUMBER").isNull && col("DLI_PART_NUMBER").isNotNull)
    val expected=firstRes.count()

    val actual=firstRes.filter(col("inference").contains("PART_NUMBER is null and DLI_PART_NUMBER is not null")).count()
    assert(expected==actual)

  }

}
