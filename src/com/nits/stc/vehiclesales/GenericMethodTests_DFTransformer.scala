package com.nits.stc.vehiclesales

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

/**
 * This Class test generic methods of DFTransformer Class, by checking
 * that the Data in Target and Status is correct as per the process
 */
class GenericMethodTests_DFTransformer extends FunSuite{

  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
  var path = "/config/vehiclesales/vehicleSalesConfig.json" // path of vehicleSalesConfig.json file in hdfs
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]] // Reading content of config file above in Map format
  var options = Config.readNestedKey(config, "SOURCE") // reading the nested values in the config file under "SOURCE" key

  var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
  //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT, uri + "//raw/vw/vehiclesales", options)
  //  var driver= new DriverProgram()


  var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
  var targetST = uri + targetOptions("SALESTRANSACTIONS")
  var targetDFSalesTransaction = Extractor.extract(Resource.AVRO, targetST, null)
  var targetSalesEmployees = uri + targetOptions("SALESEMPLOYEES")
  var targetDFSalesEmployees = Extractor.extract(Resource.AVRO, targetSalesEmployees, null)
  var targetVF = uri + targetOptions("VEHICLEFEATURES")
  var targetDFVehicleFeatures = Extractor.extract(Resource.AVRO, targetVF, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var finalDataSetPreviousPath = uri + statusOptions("PATH") + "//STATUS_20200917123515//*.avro"
  var finalDataSetUpdatedPath = uri + statusOptions("PATH") + "//STATUS_20200917132901//*.avro"

  var previous_finalDataSet = Extractor.extract(Resource.AVRO, finalDataSetPreviousPath, null)
  var updated_finalDataSet= Extractor.extract(Resource.AVRO, finalDataSetUpdatedPath, null)

  var statusCols = statusOptions("COLUMNS").split(",").toSeq

  test("updateExistingData: Updated Final Dataset should have latest Updated Date"){
    /**
     * Scenario
     * 1. Confirm Updated date is same across all the records in both final Datasets
     * 2. Confirm the updated date in updated final Dataset is greater than previous Final Dataset
     */
    var updated_date_previous = previous_finalDataSet.select("UPDATED_DATE").first().getLong(0) //select value at first record in "Updated date column"
    var updated_date_updated = updated_finalDataSet.select("UPDATED_DATE").first().getLong(0) //select value at first record in "Updated date column"

    //assert that all the colums have same updated_date value
    assert(previous_finalDataSet.filter(col("UPDATED_DATE")===updated_date_previous).count()===previous_finalDataSet.count())
    assert(updated_finalDataSet.filter(col("UPDATED_DATE")===updated_date_updated).count()===updated_finalDataSet.count())

    //assert that updated "updated_date" is greater than previous "updated_date"
    assert(updated_date_previous<updated_date_updated)
  }

  test("Denormalization: Final Data set should only contain columns from master which are present in COLUMN_ key in config"){
    /**
     * Scenario
     * 1. Final Dataset should contain all the columns from master which are present in COLUMN_ key in config under DENORMALIZE
     * 2. Final Dataset should not contain rest of the columns from the master dataset
     * NOTE: Dealer is Master Dataset
     */
    var masterColumnsPresentInConfig = Config.readNestedKey(config, "DENORMALIZE")("COLUMNS_1").split(",").toSeq
    var totalColumnsInMaster = dealerDF.columns
    var count = totalColumnsInMaster.size - masterColumnsPresentInConfig.size
    var masterColumnsNotPresentInConfig = new Array[String](count)

    var x = ""
    var i = 0
    for(x <- totalColumnsInMaster){
      if(!masterColumnsPresentInConfig.contains(x)){
        masterColumnsNotPresentInConfig(i) = x
      }
    }

    var columnsPresentInFinalDataSet = updated_finalDataSet.columns

    //assert that all master columns in config are present in final Dataset
    masterColumnsPresentInConfig.foreach(value => assert(columnsPresentInFinalDataSet.contains(value)))

    //assert that maset columns apart frm those present in config should not be present in final Dataset
    masterColumnsNotPresentInConfig.foreach(value => assert(!columnsPresentInFinalDataSet.contains(value)))
  }

  test("DENORMALIZATION: final dataset should contain DEALER_NAME from Master dataset"){
    /**
     * Scenario
     * Since the DEALER_NAME is coming from the master dataset, the DLR_CODE must not be present in Master
     * Dataset where DLR_NAME is Null in final Dataset
     */


    var dfWithNulDLRNAME =previous_finalDataSet.filter(col("DLR_NAME").isNull)
    val nulDLRCODE = dfWithNulDLRNAME.select(col("DLR_CODE")).first().getInt(0)
    var dfJoinWithMaster = dealerDF.join(dfWithNulDLRNAME,dfWithNulDLRNAME("DLR_CODE")===dealerDF("DLR_CODE"))
    print(dfJoinWithMaster.count() + " ")
    print(dealerDF.filter(col("DLR_NAME").isNotNull).count()+" "+dealerDF.count())
   assert(updated_finalDataSet.filter(col("DLR_NAME").isNull).count()===0)

    dealerDF.filter(col("DLR_CODE")===nulDLRCODE).show()
    dfWithNulDLRNAME.show(200)
  }


     test("Target Data Frame of SalesTransactions should contain dealer denormalized columns")
    {
    var columns = targetDFSalesTransaction.columns
    assert(
      columns.contains("DLR_NAME") &&
      columns.contains("RGN_CODE") &&
      columns.contains("AREA_CODE") &&
      columns.contains("OEM_CODE") &&
      columns.contains("DLR_ACTUAL_STATUS_CODE"))

  }





   test("Target SalesTransactions data is denormalized with Dealers or not") //Run Successfull 3
   {

  var targetDFST= targetDFSalesTransaction
   // targetDFST.show()
    dealerDF= dealerDF.withColumnRenamed("DLR_CODE", "DEALER_CODE")
   dealerDF= dealerDF.withColumnRenamed("DLR_NAME", "DEALER_NAME")
   dealerDF= dealerDF.withColumnRenamed("RGN_CODE", "REGION")
   dealerDF= dealerDF.withColumnRenamed("AREA_CODE", "AREA")
   dealerDF= dealerDF.withColumnRenamed("OEM_CODE", "OEM")
    dealerDF= dealerDF.withColumnRenamed("DLR_ACTUAL_STATUS_CODE", "DLR_ACTUAL_STATUS")

    var resultDF= targetDFST.join(dealerDF,targetDFST.col("DLR_CODE")===dealerDF.col("DEALER_CODE"))

     targetDFST = targetDFST.select("DLR_CODE", "DLR_NAME", "RGN_CODE", "AREA_CODE", "OEM_CODE", "DLR_ACTUAL_STATUS_CODE")
     resultDF = resultDF.select("DLR_CODE", "DEALER_NAME", "REGION", "AREA", "OEM", "DLR_ACTUAL_STATUS")




     assert(resultDF.except(targetDFST).count == 0)
   }

}
