package printdataframe_Service

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintDealerDf {
  def main(args:Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path = "/config/service/serviceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]]
    var options = Config.readNestedKey(config, "SOURCE")

    var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
    //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
    var sourcePath = uri + options("PATH")
    var Extractor = new DFExtractor()
    var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)


    dealerDF.show()
    //dealerDF.printSchema()
    println("records count of DealerFileDF: " + dealerDF.count())
  }
}
