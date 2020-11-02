package printDataFrame_Invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintDw_Parts_Invoice {
  def main(args:Array[String]){
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri ="hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path= "/config/invoice/invoiceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
    var options= Config.readNestedKey(config,"SOURCE")
    var Extractor=new DFExtractor()
    var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
    var targetDw_parts=uri+targetOptions("INVOICE")
    var targetDF_Dw_parts=Extractor.extract(Resource.AVRO,targetDw_parts , null)
    targetDF_Dw_parts.show()
    println("records count of targer_DW_Parts_DF : "+targetDF_Dw_parts.count())

  }

}
