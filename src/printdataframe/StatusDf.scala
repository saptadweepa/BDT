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

object StatusDf {
  
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
   
   statusDF.show()
   println("count of records for statusDF:  "+statusDF.count())
   println("count for PREV_PURCHASE_ORDER_DATE is null :  "+statusDF.filter(col("PREV_PURCHASE_ORDER_DATE").isNull).count())
   
   println("count for DLR_NAME is null: "+statusDF.filter(col("DLR_NAME").isNull).count())
  
   println("count of PRC_Success : " +statusDF.filter(col("prc")===lit("SUCCESS")).count())



   }
}