package com.nits.stc.sprint15.printDataframe.printDataFrame_Service

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import org.scalatest.FunSuite

object PrintDf_Service_Status {

  def main(args:Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path = "/config/service/serviceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]]
    var options = Config.readNestedKey(config, "SOURCE")
    var Extractor = new DFExtractor()
    val statusOptions = Config.readNestedKey(config, "STATUS")
    var statusPath = uri + statusOptions("PATH") + "//STATUS_20201111173756//*.avro"
    //status dataframe
    println(statusPath)
    var statusCols = statusOptions("COLUMNS").split(",").toSeq
    var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

    //statusDF.select(col("RO_CLOSE_DATE"),col("CI_RO_CLOSE_DATE"),col("prc"),col("inference")).show(false)
    statusDF.show(false)
    println("count of records for statusDF:  " + statusDF.count())

    //statusDF.select(col("SOPR_OPERATION_CATEGORIES")).filter(col("SOPR_OPERATION_CATEGORIES").contains("MAINTENANCE")).show(false)
    //statusDF.filter(col("inference").contains("OPERATION_CATEGORY set to M.")).show(false)
  /*  statusDF.filter(col("SOPR_BILLED_LABOR_HRS") === typedLit[Double](0) && col("SOPR_ACTUAL_LABOR_HRS") > typedLit[Double](0) && col("SOPR_LABOR_PRICE") > typedLit[Double](0))
      .select(col("SOPR_BILLED_LABOR_HRS"), col("SOPR_ACTUAL_LABOR_HRS"), col("SOPR_LABOR_PRICE"),  col("inference")).show(false) */

    //statusDF.filter(col("VEH_MAKE") =!= lit("VOLKSWAGEN")).select(col("VEH_MAKE"), col("VEH_VIN"), col("prc"),  col("inference")).show(false)
  }

}
