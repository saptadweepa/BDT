package com.nits.stc.sprint13.vehiclesales

import com.nits.etlcore.impl._
import com.nits.global.Resource
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField}
import org.scalatest.FunSuite



/**
 * This class Tests Generic Methods of MetadataModifier (Schema) Object
 * The Target/Status Dataframes are tested and it is verified that the values are correct
 * and as per the process.
 */
class GenericMethodsTests_MetadataModifier extends FunSuite {
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020" //to tell where HDFS is running
  var path = "/config/vehiclesales/vehicleSalesConfig.json" // path of vehicleSalesConfig.json file in hdfs
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String, String]] // Reading content of config file above in Map format
  var options = Config.readNestedKey(config, "SOURCE") // reading the nested values in the config file under "SOURCE" key

  var sourceType: Resource.ResourceType = Resource.withName(options("TYPE"))
  //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()
  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT, uri + "//raw/vw/vehiclesales", options)
  //  var driver= new DriverProgram()


  var targetOptions = Config.readNestedKey(config, "TARGET_PATH")
  var targetST = uri + targetOptions("SALESTRANSACTIONS")
  var targetDFSalesTransaction = Extractor.extract(Resource.AVRO, targetST, null)
  var targetSalesEmployees = uri + targetOptions("SALESEMPLOYEES")
  var targetDFSalesEmployees = Extractor.extract(Resource.AVRO, targetSalesEmployees, null)
  var targetVF = uri + targetOptions("VEHICLEFEATURES")
  var targetDFVehicleFeatures = Extractor.extract(Resource.AVRO, targetVF, null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + statusOptions("PATH") + "//STATUS_20200917132901//*.avro"
  //status dataframe
  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)
  //sourceDF.printSchema()
  var cfeature = new commonFeatures()
  //var NewSourceDF = sourceDF.select("_c0", "_c1", "_c2", "_c3", "_c4")

  test("Column should be renamed as per VehiclesalesRenameColumn.json") {
    /**
     * Scenario:
     * 1. Read all columns mapping from vehicleSalesRenameColumn.json
     * 2. Check that all the columns from vehicleSalesRenameColumn.json are present in StatusDf
     * 3. We are not checking Target Dataframes as the target Dataframe contains data divided into
     *    3 tables Sales Transaction, Sales Emplopyees and Vehicle Features. All the Data/Column from
     *    Source Data is NOT present in Target Dataframes even when combined.
     */
    var renameColumns = Config.readConfig(uri + config("RENAME_COLUMNS")).params.asInstanceOf[Map[String, String]]
    //var targetColumns = Array.concat(targetDFSalesTransaction.columns,targetDFVehicleFeatures.columns,targetDFSalesEmployees.columns)
    var statusCol = statusDF.columns;
    println(statusCol)
    renameColumns.values.foreach(value => assert(statusCol.contains(value)))
  }
  test("Target_Dataframes should contain Audit Columns"){
    /**
     * Scenario:
     * Check the columns added through auditColumn method is present in all the 3 target tables
     */

    var auditColums_ST= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var TargetDFCol_ST= targetDFSalesTransaction.columns;
    println("columns of SalesTransaction: ")
    TargetDFCol_ST.foreach((element:String)=>print(element+ " ")) //  TargetDFCol_ST is an Array so need to be printed using for loop
    println()
    auditColums_ST.foreach(element => assert(TargetDFCol_ST.contains(element)))

    var auditColums_SE= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var TargetDFCol_SE= targetDFSalesEmployees.columns;
    println("columns of SalesEmployee: ")
    TargetDFCol_SE.foreach((element:String)=>print(element+ " "))
    println()
    auditColums_SE.foreach(element => assert(TargetDFCol_SE.contains(element)))

    var auditColums_VF= Seq("CREATED_BY", "CREATED_DATE", "UPDATED_BY" ,"UPDATED_DATE" ,"DELETED_BY" ,"DELETED_DATE")
    var TargetDFCol_VF= targetDFVehicleFeatures.columns;
    println("columns of VehicleFeatures: ")
    TargetDFCol_VF.foreach((element:String)=>print(element+ " "))

    auditColums_VF.foreach(element => assert(TargetDFCol_VF.contains(element)))

  }



  test("Should apply schema on vehicleFeatures Dataframe as per vehicleFeaturesSchema.json") {
    /**
     * Scenario
     * 1. Create Schema from the vehicleFeatures schema json
     * 2. get actual schema from the target dataframe
     * 3. check if the schema obtained from above two steps are same
     */
    var path = uri + Config.readNestedKey(config, "SCHEMA")("VEHICLEFEATURES")
    var df = new DFDefinition()
    df.setSchema(path)
    println(df.schema)
    assert(df.schema === targetDFVehicleFeatures.schema)
  }


  test("Should apply schema on salesTransaction Dataframe as per salesTransactionSchema.json") {
    /**
     * Scenario
     * 1. Create Schema from the salesTransaction schema json
     * 2. get actual schema from the target dataframe
     * 3. check if the schema obtained from above two steps are same
     */
    var path = uri + Config.readNestedKey(config, "SCHEMA")("SALESTRANSACTIONS")
    var df = new DFDefinition()
    df.setSchema(path)
    println(df.schema)
    println(targetDFSalesTransaction.schema)
    assert(df.schema === targetDFSalesTransaction.schema)
  }

  test("Should apply schema on salesEmployee Dataframe as per salesEmployeeSchema.json") {
    /**
     * Scenario
     * 1. Create Schema from the salesEmployee schema json
     * 2. get actual schema from the target dataframe
     * 3. check if the schema obtained from above two steps are same
     */
    var path = uri + Config.readNestedKey(config, "SCHEMA")("SALESEMPLOYEES")
    var df = new DFDefinition()
    df.setSchema(path)
    var newSchema = df.schema.add(StructField("IS_NEW", StringType, true)) //Added to SalesEmployee DF during ETL process
    assert(newSchema === targetDFSalesEmployees.schema)
  }

  /*  test("YEAR, YEARQTR, YEARMONTH should be present in the target dataframe"){
      println(targetDFSalesEmployees.columns.contains("YEAR"))
      println(targetDFSalesEmployees.columns.contains("YEARQTR"))
      println(targetDFSalesEmployees.columns.contains("YEARMONTH"))

      println(targetDFSalesTransaction.columns.contains("YEAR"))
      println(targetDFSalesTransaction.columns.contains("YEARQTR"))
      println(targetDFSalesTransaction.columns.contains("YEARMONTH"))

      println(targetDFVehicleFeatures.columns.contains("YEAR"))
      println(targetDFVehicleFeatures.columns.contains("YEARQTR"))
      println(targetDFVehicleFeatures.columns.contains("YEARMONTH"))

      println(statusDF.columns.contains("YEAR"))
      println(statusDF.columns.contains("YEARQTR"))
      println(statusDF.columns.contains("YEARMONTH"))

    }*/

  test("YEAR, YEARQTR, YEARMONTH should be present in resulting Dataset when MetadataModifier.addCalendarColumns is used") {
    /**
     * Year, YearQtr and YearMonth seems to be temporary column as
     * they are not present in the final dataframes (status + target)
     * Scenario
     * 1. apply MetadataModifier.addCalendarColumns to sourceDf
     * 2. Check if the resulting Df has proper columns added as expected
     */
    var testDf = sourceDF
    var attributes = Map("CALENDAR_SOURCE" -> "_c4", "CALENDAR_FORMAT" -> "MM/dd/yyyy")

    var finalDF = MetadataModifier.addCalendarColumns(testDf, attributes)

    var expectedCount = testDf.filter(col("_c4").isNotNull).count()
    var YEAR_COUNT = finalDF.filter(col("YEAR").isNotNull).count()
    var YEARQTR_COUNT = finalDF.filter(col("YEARQTR").isNotNull).count()
    var YEARMONTH_COUNT = finalDF.filter(col("YEARMONTH").isNotNull).count()

    assert(YEAR_COUNT != 0)
    assert(expectedCount == YEAR_COUNT)
    assert(expectedCount == YEARQTR_COUNT)
    assert(expectedCount == YEARMONTH_COUNT)



  }

  test("The date should be converted to epoch") {
    /**
     * Scenario
     * 1.Convert sourceDf date to epoch using MetadataModifier.convertToEpoch
     * 2.Check that all the values in sourceDf and the result df are equal in count
     * 3. take first value from sourcedf convert to epoch using scala code
     * 4. take first value from resultdf
     * 5. the values from step 3 and 4 should be equal.
     */
    var testDf =  sourceDF
    var finaldf =  MetadataModifier.convertToEpoch(testDf, Map("TO_EPOCH_1" -> "_c4", "FORMAT_1" -> "MM/dd/yyyy"))

    var expectedDates = finaldf.select(col("_c4"))

    //step 2
    assert(testDf.select(col("_c4")).filter(col("_c4").isNotNull).count()
      === expectedDates.filter(col("_c4").isNotNull).count())

    var expectedDateEpoch = expectedDates.first().getLong(0) //from the resultDf after convertToEpch is used
    var actualDate = testDf.select(col("_c4")).first().getString(0)

    println(actualDate)
    println("expected: "+expectedDateEpoch)

    var actualDateEpoch = new java.text.SimpleDateFormat("MM/dd/yyyy").parse(actualDate).getTime;
    println("actual : "+actualDate)

    //step 5
    assert(expectedDateEpoch===actualDateEpoch)

  }
  test("All the Dates should be converted to long"){
    /**
     * Scenario:
     *
     */

    var statusDf_Schema= statusDF.schema
    for(i <- 0 to statusDf_Schema.fields.size -1 ) {
      if(statusDf_Schema.fields(i).name.contains("_DATE") && !statusDf_Schema.fields(i).name.contains("BIRTH")){
        assert(statusDf_Schema.fields(i).dataType === LongType)
      }
    }

  }
}
