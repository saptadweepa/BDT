package printDataFrame_Invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintRAW_Df_Invoice {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("nits-etlcore")
      .master("local")
      .config("spark.speculation", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020"
    var path = "/config/invoice/invoiceConfig.json"
    var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]]
    var options = Config.readNestedKey(config, "SOURCE")

    var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
    //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
    var sourcePath = uri + options("PATH")
    var Extractor = new DFExtractor()
    var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
    var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/invoice/*.txt",options)
    sourceDF.show()

    println("no. of records in sourceDF: " + sourceDF.count())

  }


}
