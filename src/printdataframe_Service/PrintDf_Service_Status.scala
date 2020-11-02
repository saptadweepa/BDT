package printdataframe_Service

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession

object PrintDf_Service_Status {

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
    var Extractor = new DFExtractor()
    val statusOptions = Config.readNestedKey(config, "STATUS")
    var statusPath = uri + statusOptions("PATH") + "//STATUS_20201013145659//*.avro"
    //status dataframe
    println(statusPath)
    var statusCols = statusOptions("COLUMNS").split(",").toSeq
    var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

    statusDF.show()
    println("count of records for statusDF:  " + statusDF.count())
  }

}
