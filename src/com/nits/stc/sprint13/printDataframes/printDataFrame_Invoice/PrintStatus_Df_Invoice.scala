package com.nits.stc.sprint13.printDataframes.printDataFrame_Invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import com.sun.xml.internal.fastinfoset.stax.events.Util
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, length, lit}

object PrintStatus_Df_Invoice {

  def main(args:Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path = "/config/invoice/invoiceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]]
    var options = Config.readNestedKey(config, "SOURCE")
    var Extractor = new DFExtractor()
    val statusOptions = Config.readNestedKey(config, "STATUS")
   // var statusPath = uri + statusOptions("PATH") + "//STATUS_20201020150215//*.avro"

    var statusPath = uri + statusOptions("PATH") + "//STATUS_20201103110306//*.avro"
    //status dataframe
    println(statusPath)
    var statusCols = statusOptions("COLUMNS").split(",").toSeq
    var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)
      statusDF.show()
    println("count of records for statusDF:  " + statusDF.count())
    println("count of ITM_PART_NUMBER is null :" + statusDF.filter(col("ITM_PART_NUMBER").isNull).count())


    //ITM_PART_NUMBER containing DMS_PREFIX "PK"
    var ITM_PART_NUMBER_PK_count = statusDF.filter(col("ITM_PART_NUMBER").contains("PK")&& col("prc") === lit("SUCCESS")).count()
    println("ITM_PART_NUMBER_PK_count : "+ITM_PART_NUMBER_PK_count )
    statusDF.filter(col("ITM_PART_NUMBER").contains("PK")&& col("prc") === lit("SUCCESS")).show(false)

    //ITM_PART_NUMBER containing space
    var ITM_PART_NUMBER_Space = statusDF.filter(col("ITM_PART_NUMBER").contains(" ")&& col("prc") === lit("SUCCESS")).count()
    println("ITM_PART_NUMBER_Space_count : "+ITM_PART_NUMBER_Space )
    statusDF.filter(col("ITM_PART_NUMBER").contains(" ") && col("prc") === lit("SUCCESS")).show(false)

    var NullCount = statusDF.filter(col("YEARQTR").isNull).count()
    println("YEARQTR  null count"+NullCount)

    statusDF.write.csv("/usr/local/nits/test.csv")


    //statusDF.filter(Util.isEmptyString(col("CI_DLR_CODE"))).show()

    /*
      println("count of INV_CLOSE_DATE is null :" + statusDF.filter(col("INV_CLOSE_DATE").isNull).count())



    println("count of INV_PAYMENT_CODE > 256 char :"+ statusDF.filter(length(col("INV_PAYMENT_CODE")) > 256).count())

    var inferenceDf= statusDF.select("inference").show()
    var DuplicateCount= statusDF.filter(col("inference").contains("Duplicate Invoice.")).count()
    println("Is Duplicate count in inference: "+DuplicateCount)
    */

   //printColDF("CI_DLR_CODE",statusDF.filter(col("CLS_CODE") === lit("I")))

    //statusDF.select(col("CI_INV_CLOSE_DATE")).show()

   // printColDF("INV_TOTAL_MISC",statusDF)

   // printItemPartContainingU(statusDF)

    //ruleFailTestFunctionCUST_RET_WHOLESALE_FLAGCondition2(statusDF)
  }

  def printColDF(colname: String, dF:DataFrame){
    dF.select(col(colname)).show()
    print("Is Null?: ")
    println(dF.select(col(colname)).first().get(0) == null)
    println(dF.select(col(colname)).first().get(0).toString.length)
  }

  def printItemPartContainingU(df:DataFrame): Unit ={
    // Item core flag set to Y rule 1
    println("Item core flag set to Y rule 1")
    df.filter(col("ITM_PART_NUMBER").substr(11,12)===lit("U")).show(false)
  }

  def ruleFailTestFunctionCUST_RET_WHOLESALE_FLAGCondition2(statusDF:DataFrame): Unit ={
    //CUST_RET_WHOLESALE_FLAG set to W - Condition 2
    println("CUST_RET_WHOLESALE_FLAG set to W - Condition 2")
    var expectedDf = statusDF.filter(col("INV_RET_WHOLE_FLAG") =!= lit("W")
      && col("INV_TOTAL_TAX") === lit(0.0) && col("CUST_RET_WHOLESALE_FLAG") =!= lit("W")
    )
    expectedDf.show()

    var countOf_W = statusDF.filter(col("INV_RET_WHOLE_FLAG") =!= lit("W")
      && col("INV_TOTAL_TAX") === lit(0.0) && col("CUST_RET_WHOLESALE_FLAG") =!= lit("W")
    ).count()
    var countOf_R = statusDF.filter(col("INV_RET_WHOLE_FLAG") =!= lit("W")
      && col("INV_TOTAL_TAX") === lit(0.0) && col("CUST_RET_WHOLESALE_FLAG") === lit("W")
    ).count()
    println("CUST_RET_WHOLESALE_FLAG contains W count: " +countOf_W)
    println("CUST_RET_WHOLESALE_FLAG contains R count: " +countOf_R)
  }




}
