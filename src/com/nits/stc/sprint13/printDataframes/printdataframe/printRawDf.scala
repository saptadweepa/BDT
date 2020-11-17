package com.nits.stc.sprint13.printDataframes.printdataframe

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.vehiclesales._
import org.scalatest._
import org.scalactic.source.Position.apply

object printRawDf {
  def main(args:Array[String]){
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
   var sourceDF = Extractor.extract(Resource.TEXT,uri +"/raw/vw/vehiclesales",options) 
   sourceDF.show()
   
   println("no. of records in sourceDF: "+sourceDF.count())
  
  println("count for c37 is null: "+sourceDF.filter(col("_c37").isNull).count())

    //statusDF.createOrReplaceGlobalTempView("table")
    var endingWithMinus = sourceDF.filter(col("_c49").rlike("-$")) // match based on regex
    endingWithMinus.show()
   
  }
}