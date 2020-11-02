package printdataframe

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.vehiclesales._
import org.scalatest._
import org.scalactic.source.Position.apply

object PrintDealerDf {
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

   dealerDF.show()
   //dealerDF.printSchema()
   println("records count of DealerFileDF: "+dealerDF.count())
    var Dealer1count = dealerDF.filter((col("DLR_NAME"))===lit("Hendrick Motors")).count()
   
    println("count of Ist dealer: "+Dealer1count)
    
     var Dealer2count = dealerDF.filter((col("DLR_NAME"))===lit("Sterling Volkswagen")).count()
   
    println("count of 2nd dealer: "+Dealer2count)
    
     var Dealer3count = dealerDF.filter((col("DLR_NAME"))===lit("Greg Imports")).count()
   
    println("count of 3rd dealer: "+Dealer3count)
    
     var Dealer4count = dealerDF.filter((col("DLR_NAME"))===lit("Westwood Volkswagen")).count()
   
    println("count of 4th dealer: "+Dealer4count)
    
     var Dealer5count = dealerDF.filter((col("DLR_NAME"))===lit("Jim Jard Volkswagen")).count()
   
    println("count of 5th dealer: "+Dealer5count)
    
    println(dealerDF.filter(col("DLR_CODE").contains("408148")).count())
    
  
    //var loader = new DFLoader()
    //var newDealerFilesDir = uri + "/new_dealer";
    //loader.save(dealerDF.filter(!col("DLR_CODE").contains("408148")), Resource.AVRO, newDealerFilesDir, Map("header" -> "true"))
    
   }
  
}