package com.nits.stc.vehiclesales

import com.nits.global._
import scala.util.control._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.vehiclesales._
import org.scalatest._
import org.scalactic.source.Position.apply
class GeneralScenarios extends FunSuite{
   val spark = SparkSession
          					   .builder()					   
          					   .appName("nits-etlcore")
    					         .master("local")
    					         .config("spark.speculation", false)
    					         .getOrCreate()
    	spark.sparkContext.setLogLevel("ERROR")
   var uri ="hdfs://sonia:8020/"
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
  var schema = Config.readNestedKey(config, "SCHEMA")
  var SALES_EMPLOYEE_SCHEMA = uri + schema("SALESEMPLOYEES")
  var SALES_TRANSACTION_SCHEMA = uri + schema("SALESTRANSACTIONS")
  var VEHICLEFEATURES_SCHEMA = uri + schema("VEHICLEFEATURES")

   var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
   var targetST=uri+targetOptions("SALESTRANSACTIONS")
   var targetDFST=Extractor.extract(Resource.AVRO,targetST , null)
   var targetSalesEmployees=uri+targetOptions("SALESEMPLOYEES")
   var targetDFSales=Extractor.extract(Resource.AVRO,targetSalesEmployees , null)
   var targetVF=uri+targetOptions("VEHICLEFEATURES")
   var targetDFVF=Extractor.extract(Resource.AVRO,targetVF , null)

   
   test("Verify No Date  & timeStamp Column in salesEmployee should exist") 
   {
     var value=false
     var allcolumns=""
      val columnNames : Array[String] = targetDFSales.schema.fields.map(x=>x.name).map(x=>x.toString)
   val columnDataTypes : Array[String] = targetDFSales.schema.fields.map(x=>x.dataType).map(x=>x.toString)
  // val col1:Array[String]
   val loop = new Breaks;
      
      loop.breakable {
  
    for(myTypes <- columnDataTypes) {
      if( (myTypes==lit("Date")) ||(myTypes==lit("timestamp")))
      {
        value=true
         System.out.println( myTypes + " " + value)
      }
      else
      {
        value=false
        System.out.println(myTypes + " " + value)
      }
        if(value==true)
        {
        loop.break() 
        }
     }
    
   
      }
   }
  test("Verify No Date  & timeStamp Column in salesTransactions should exist") 
   {
     var value=false
     var allcolumns=""
   val columnDataTypes : Array[String] = targetDFST.schema.fields.map(x=>x.dataType).map(x=>x.toString)
  // val col1:Array[String]
    val loop = new Breaks;
      
      loop.breakable {
    for(myString <- columnDataTypes) {
      if( (myString==lit("Date")) ||(myString==lit("timestamp")))
        value=true
      else
        value=false
        System.out.println(myString + value)
        loop.break()
     }

   }
   }
   test("Verify No Date  & timeStamp Column in VehicleFeatures should exist") 
   {
     var value=false
     var allcolumns=""
   val columnDataTypes : Array[String] = targetDFVF.schema.fields.map(x=>x.dataType).map(x=>x.toString)
  // val col1:Array[String]
    val loop = new Breaks;
      
      loop.breakable {
    for(myString <- columnDataTypes) {
      if( (myString==lit("Date")) ||(myString==lit("timestamp")))
        value=true
      else
        value=false
        System.out.println(myString + value)
        loop.break()
     }

   }
    
      
   
}
    

 
    test("Verify Data in String columns should not lowercase") 
   {
   targetDFST=   targetDFST.withColumn("UDLR_NAME",upper(col("DLR_NAME")))
    targetDFST.show()
    targetDFST= targetDFST.filter(col("UDLR_NAME")===col("DLR_NAME")) 
         assert(targetDFST.count!=0)
 
   }


}