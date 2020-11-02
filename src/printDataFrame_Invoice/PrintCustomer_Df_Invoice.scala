package printDataFrame_Invoice

import com.nits.etlcore.impl.DFExtractor
import com.nits.global.Resource
import com.nits.util.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, length, lit}

object PrintCustomer_Df_Invoice {
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
    var target_Customer=uri+targetOptions("CUSTOMER")
    var targetDF_Customer=Extractor.extract(Resource.AVRO,target_Customer , null)
    //targetDF_Customer.show()
    println("records count of Target_Customer_df : "+targetDF_Customer.count())
    targetDF_Customer.filter(col("DLR_CODE") === col("CUST_NUMBER")).show()

  }

}
