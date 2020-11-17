package com.nits.stc.sprint13.vehiclesales

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.vehiclesales._
import org.scalatest._
import org.scalactic.source.Position.apply

class VehicleSalesConfigTest  extends FunSuite{
  val spark = SparkSession
          					   .builder()					   
          					   .appName("nits-etlcore")
    					         .master("local")
    					         .config("spark.speculation", false)
    					         .getOrCreate()
    	spark.sparkContext.setLogLevel("ERROR")
   var uri ="hdfs://saptadwipa-Aspire-A515-54G:8020"
   var path="/config/vehiclesales/vehicleSalesConfig.json"
   var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
   var options= Config.readNestedKey(config,"SOURCE")
    
   var sourceType:Resource.ResourceType = Resource.withName(options("TYPE"))
   //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
   var sourcePath = uri + options("PATH") 
   var Extractor=new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
   var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/vehiclesales",options)
 //  var driver= new DriverProgram()
 

   var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
   var targetST=uri+targetOptions("SALESTRANSACTIONS")
   var targetDFST=Extractor.extract(Resource.AVRO,targetST , null)
   var targetSalesEmployees=uri+targetOptions("SALESEMPLOYEES")
   var targetDFSales=Extractor.extract(Resource.AVRO,targetSalesEmployees , null)
   var targetVF=uri+targetOptions("VEHICLEFEATURES")
   var targetDFVF=Extractor.extract(Resource.AVRO,targetVF , null)

   val statusOptions = Config.readNestedKey(config, "STATUS")
   var statusPath = uri + statusOptions("PATH")+"//STATUS_20200917132901//*.avro"
  //status dataframe
   println(statusPath)
   var statusCols = statusOptions("COLUMNS").split(",").toSeq
   var statusDF = Extractor.extract(Resource.AVRO, statusPath,null)
   //sourceDF.printSchema()
   
   var cfeature= new commonFeatures()
           
   test("Verify Source file  match with status dataframe for Text file")
     {
    
    println("Source")
  
    var statusDFS=statusDF
 
    println("Status")
 
    println(sourceDF.count())
    println(statusDFS.count())

    assert(sourceDF.count==statusDFS.count())
  }
  
  /*
  test("Verify target DataFrame of Sales Transactions has converted columns-PURCHASE_ORDER_DATE,CONTRACT_DATE,DELIVERY_DATE,REVERSAL_DATE,INVENTORY_DATE in long format")
     {
    
     var sourceDFS=sourceDF.select("_c3","_c4","_c5","_c6","_c7","_c10")
sourceDFS=sourceDFS.filter(col("_c3").isNotNull  && col("_c4").isNotNull && col("_c5").isNotNull && 
    col("_c6").isNotNull && col("_c7").isNotNull && col("_c10").isNotNull)
   var  finalDF  = sourceDFS.withColumn("PURCHASE_ORDER_DATE", lit(1000)*unix_timestamp(col("_c3"), "MM/dd/yyyy"))  
   finalDF  = finalDF.withColumn("CONTRACT_DATE", lit(1000)*unix_timestamp(col("_c4"), "MM/dd/yyyy"))
   finalDF  = finalDF.withColumn("DELIVERY_DATE", lit(1000)*unix_timestamp(col("_c5"), "MM/dd/yyyy"))  
   finalDF  = finalDF.withColumn("REVERSAL_DATE", lit(1000)*unix_timestamp(col("_c6"), "MM/dd/yyyy"))  
   finalDF  = finalDF.withColumn("INVENTORY_DATE", lit(1000)*unix_timestamp(col("_c7"), "MM/dd/yyyy"))  
   finalDF  = finalDF.withColumn("DEAL_STATUS_DATE", lit(1000)*unix_timestamp(col("_c10"), "MM/dd/yyyy"))  
   finalDF.show()
   finalDF=  finalDF.drop("_c3")
   .drop("_c4")
   .drop("_c5")
   .drop("_c6")
   .drop("_c7")
   .drop("_c10")
 
   
   targetDFST= targetDFST.select("PURCHASE_ORDER_DATE","CONTRACT_DATE","DELIVERY_DATE","REVERSAL_DATE","INVENTORY_DATE","DEAL_STATUS_DATE")
   targetDFST.show()
   targetDFST=targetDFST.filter(col("PURCHASE_ORDER_DATE").isNotNull && col("CONTRACT_DATE").isNotNull && col("DELIVERY_DATE").isNotNull && 
    col("REVERSAL_DATE").isNotNull && col("INVENTORY_DATE").isNotNull && col("DEAL_STATUS_DATE").isNotNull)
   var outputDF=finalDF.except(targetDFST)  
   print(outputDF.count)
   outputDF.show()
   assert(outputDF.count==0)
     }*/
     
        /*  test("Verify target DataFrame of VehicleFeatures match with sourceDataFrame")
          {
   
            var sourceDFS=  cfeature.getsourceDataFrame(sourceDF)
           // sourceDFS=statusDF.filter(col("prc")===lit("SUCCESS"))
            var sourceDFSV=cfeature.getVehicleFeatures(sourceDFS)
             sourceDFSV  = sourceDFSV.withColumn("CREATED_DATE", lit(1000)*unix_timestamp(col("CREATED_DATE"), "MM/dd/yyyy"))
   sourceDFSV  = sourceDFSV.withColumn("UPDATED_DATE", lit(1000)*unix_timestamp(col("UPDATED_DATE"), "MM/dd/yyyy"))  
   sourceDFSV  = sourceDFSV.withColumn("DELETED_DATE", lit(1000)*unix_timestamp(col("DELETED_DATE"), "MM/dd/yyyy"))  
        sourceDFSV=  sourceDFSV.select("VIN","SVF_FEATURE_DESC")
          targetDFVF=targetDFVF.select("VIN","SVF_FEATURE_DESC")
          // sourceDFSV.show()
          // targetDFVF.show()
            println(sourceDFSV.count)
            println(targetDFVF.count)
           
         var outputDF=sourceDFSV.except(targetDFVF)
           //var outputDF= sourceDFSV.join(targetDFVF,sourceDFSV.col("VIN")===targetDFVF.col("VIN"),"Inner")
          // outputDF.show()
           //sourceDFSV.where(col("VIN")===lit("1V2MR2CA8KC551512") ).show()
            targetDFVF.where(col("VIN")===lit("1V2MR2CA8KC551512")).show()
           print(outputDF.count())
           assert(outputDF.count()==0)
          }*/
/*test("Verify target DataFrame of SalesTransactions match with sourceDataFrame")
          {
            var sourceDFS=  cfeature.getsourceDataFrame(sourceDF)
            var sourceDFST=cfeature.getSalesTransactions(sourceDFS)
           sourceDFST= sourceDFST.select("SVL_CARLINE_CODE","SVL_CARLINE_NAME","SVL_INT_COLOR_CODE","BUYER_VCST_ID")
          targetDFST=   targetDFST.select("SVL_CARLINE_CODE","SVL_CARLINE_NAME","SVL_INT_COLOR_CODE","BUYER_VCST_ID")
            //sourceDFST.show()

            //targetDFST.show()
           
            var outputDF=sourceDFST.except(targetDFST)
            assert(outputDF.count()==0)
          }
    */
   /*
  test("Verify target DataFrame of SalesEmployees match with sourceDataFrame") //Run Successful 1
   {
            var sourceDFS=  cfeature.getsourceDataFrame(sourceDF)
            var sourceDFSales=cfeature.getSalesEmployees(sourceDFS)
            sourceDFSales= sourceDFSales.select("DLR_CODE","SDE_TYPE","SDE_EMP_ID").orderBy("DLR_CODE","SDE_TYPE","SDE_EMP_ID")
            targetDFSales=targetDFSales.select("DLR_CODE","SDE_TYPE","SDE_EMP_ID").orderBy("DLR_CODE","SDE_EMP_ID")
            //targetDFSales=   targetDFSales.filter(col("DLR_CODE")===lit("403207"))
            var outputDF=sourceDFSales.except(targetDFSales)
     
            sourceDFSales.show()
            targetDFSales.show()
            outputDF.show()
            assert(outputDF.count()==0)
    }
  */
  
  /*
    test("Target Data Frame of SalesTransactions should contain dealer denormalized columns") //Run Successful 2
    {
    var columns = targetDFST.columns
    assert(
      columns.contains("DLR_NAME") &&
      columns.contains("RGN_CODE") &&
      columns.contains("AREA_CODE") &&
      columns.contains("OEM_CODE") &&
      columns.contains("DLR_ACTUAL_STATUS_CODE"))

  }
   */
  
  
  
  
 /* test("Target SalesTransactions data is denormalized with Dealers or not") //Run Successfull 3
  {
  

   targetDFST.show()
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
  }*/
}