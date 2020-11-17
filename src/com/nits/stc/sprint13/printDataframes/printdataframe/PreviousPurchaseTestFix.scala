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

object PreviousPurchaseTestFix {
  
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
    var Extractor=new DFExtractor()
        val statusOptions = Config.readNestedKey(config, "STATUS")
   var statusPath = uri + statusOptions("PATH")+"//STATUS_20200917132901//*.avro"
  //status dataframe
   println(statusPath)
   var statusCols = statusOptions("COLUMNS").split(",").toSeq
   var statusDF = Extractor.extract(Resource.AVRO, statusPath,null)
   var defintion = new DFDefinition()
   defintion.appendRules(uri + config("RULE_PATH"))
   var transformer = new VehicleSalesTransformer(defintion)
   var newStatus = statusDF.filter(col("PREV_PURCHASE_ORDER_DATE").isNull)
   var updateDf = newStatus.withColumn("PREV_PURCHASE_ORDER_DATE", col("PURCHASE_ORDER_DATE")+1000000)
   
    
   println("new dataframe count"+updateDf.count())
   updateDf = transformer.applyRules(updateDf)
   
   println("count of Incoming Purchase" + updateDf.filter(col("inference").contains("Incoming Purchase Order Date less than Previous Purchase Order Date")).count())
   println("count of purchase < prev purchase" + updateDf.filter(col("PURCHASE_ORDER_DATE")<col("PREV_PURCHASE_ORDER_DATE")).count())
  
   }
}