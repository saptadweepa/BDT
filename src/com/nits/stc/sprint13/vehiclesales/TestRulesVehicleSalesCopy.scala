package  com.nits.stc.sprint13.vehiclesales

import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.vehiclesales._
import org.scalatest._
import org.scalactic.source.Position.apply

class TestRulesVehicleSalesCopy extends FunSuite{
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var uri ="hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
  var path="/config/vehiclesales/vehicleSalesConfig.json" // path of vehicleSalesConfig.json file in hdfs
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]] // Reading content of config file above in Map format
  var options= Config.readNestedKey(config,"SOURCE")// reading the nested values in the config file under "SOURCE" key

  var sourceType:Resource.ResourceType = Resource.withName(options("TYPE"))
  //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
  var sourcePath = uri + options("PATH")
  var Extractor=new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/vehiclesales",options)
  //  var driver= new DriverProgram()


  var targetOptions=Config.readNestedKey(config, "TARGET_PATH")
  var targetST=uri+targetOptions("SALESTRANSACTIONS")
  var targetDFST=Extractor.extract(Resource.AVRO,targetST , null)
  var targetSalesEmployees=uri+targetOptions("SALESEMPLOYEES")
  var targetDFSales=Extractor.extract(Resource.AVRO,targetSalesEmployees , null)
  var targetVF=uri+targetOptions("VEHICLEFEATURES")
  var targetDFVF=Extractor.extract(Resource.AVRO,targetVF , null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH")+"//STATUS_20200917132901//*.avro"
  //status dataframe
  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath,null)
  //sourceDF.printSchema()

  // var cfeature= new commonFeatures()
  test("Verify Purchase Order Date less then Previous Purchase Order Date")
  {

    statusDF.printSchema()
    var statusDFExpected=statusDF.filter(col("PURCHASE_ORDER_DATE")<col("PREV_PURCHASE_ORDER_DATE"))
    statusDFExpected.show()
    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("Incoming Purchase Order Date less than Previous Purchase Order Date")).count()
    println(expected+ " " +actual)
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  assert(expected==0)
    assert(expected==actual)
    assert(statusDFExpected.count!=0)


  }

  test("Verify Length of VIN is not equal to 17")
  {
    var statusDFExpected=statusDF.filter(length(col("VIN"))=!=17 &&
      col("prc")===lit("REJECTED"))
    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("Length of VIN is not equal to 17.")).count()

    assert(expected!=0)

    assert(expected==actual)

  }

  test("Verify  VIN is null")
  {
    var statusDFExpected=statusDF.filter(col("VIN").isNull && col("prc")===lit("REJECTED"))

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("VEH VIN is Null.")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    assert(statusDFExpected.count==0)
    assert(expected==actual)
  }
  test("Verify  MAKE is not equal to AUDI or VOLKSWAGEN")
  {
    var statusDFExpected=statusDF.filter(col("MAKE").isNotNull && col("MAKE")=!=lit("AUDI") && col("MAKE")=!=lit("VOLKSWAGEN") && col("prc")===lit("REJECTED"))

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("MAKE is not equal to AUDI or VOLKSWAGEN.")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    assert(expected!=0)
    assert(expected==actual)
  }
  test("Verify  DLR_NAME is Null")
  {
    var statusDFExpected=statusDF.filter(col("DLR_NAME").isNull && col("prc")===lit("REJECTED") )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("Dealer Name is null.")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    assert(statusDFExpected.count!=0)
    assert(expected==actual)
  }
  test("Verify Duplicate row found")
  {
    var statusDFExpected=statusDF.filter(col("IsDuplicate")=== lit("Y") && col("prc")===lit("REJECTED") )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("It is duplicate row.")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))
    println(expected+ ""+actual)
    assert(statusDFExpected.count!=0)
    assert(expected==actual)
  }
  test("Verify Make is null")
  {
    var statusDFExpected=statusDF.filter(col("MAKE").isNull  )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("MAKE is null")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    // assert(statusDFExpected.count==0)
    assert(expected==actual)
  }
  test("Verify Model is null")
  {
    var statusDFExpected=statusDF.filter(col("MODEL").isNull  )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("MODEL is null")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    // assert(statusDFExpected.count==0)
    assert(expected==actual)
  }
  test("Verify Model Year is null")
  {
    var statusDFExpected=statusDF.filter(col("MODEL_YEAR").isNull  )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("MODEL_YEAR is null")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    // assert(statusDFExpected.count==0)
    assert(expected==actual)
  }
  test("Verify BUYER_ID is null")
  {
    var statusDFExpected=statusDF.filter(col("BUYER_ID").isNull  )

    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("BUYER_ID is null")).count()
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  statusDFExpected.filter(!(col("inference").contains("Dealer not found in dealer master.")))

    // assert(statusDFExpected.count==0)
    assert(expected==actual)
  }
}