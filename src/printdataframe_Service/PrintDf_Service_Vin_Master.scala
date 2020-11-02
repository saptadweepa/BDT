package printdataframe_Service

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintDf_Service_Vin_Master {

    def main(args:Array[String]){
      val spark = SparkSession
        .builder()
        .appName("nits-etlcore")
        .master("local")
        .config("spark.speculation", false)
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020"
      var path= "/config/service/serviceConfig.json"
      var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
      var options= Config.readNestedKey(config,"SOURCE")
      var Extractor=new DFExtractor()

      var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
      var targetVM=uri+targetOptions("VIN_MASTER")
      var targetDFVM=Extractor.extract(Resource.AVRO,targetVM , null)
      targetDFVM.show()
      println("records count of VinMaster_Df : "+targetDFVM.count())
    }

}
