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

object SalesTransactionDf {
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
   
    var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
   var targetST=uri+targetOptions("SALESTRANSACTIONS")
   var targetDFST=Extractor.extract(Resource.AVRO,targetST , null)
   targetDFST.show()
   println("records count of salesTransactionDf : "+targetDFST.count())
     }
  
}