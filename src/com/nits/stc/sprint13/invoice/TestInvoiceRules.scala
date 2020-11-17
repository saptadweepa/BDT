package com.nits.stc.sprint13.invoice


import com.nits.global._
import com.nits.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.nits.etlcore.impl._
import com.nits.invoice._
import org.scalatest._
import org.scalactic._
import java.text.SimpleDateFormat._
import org.apache.spark.sql.Column
import java.util.Calendar
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.parser.ParseException

class TestInvoiceRules extends FunSuite {
  val spark = SparkSession
    .builder()
    .appName("nits-etlcore")
    .master("local")
    .config("spark.speculation", false)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var uri = "hdfs://saptadwipa-Aspire-A515-54G:8020/"
  var path = "/config/invoice/invoiceConfig.json"
  var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
  var options= Config.readNestedKey(config,"SOURCE")

  var sourceType:Resource.ResourceType = Resource.withName(options("TYPE"))
  var sourcePath = uri + options("PATH")
  var Extractor = new DFExtractor()

  var dealerDF = Extractor.extract(Resource.AVRO, uri + "/raw/vw/dealer/*.avro", null)
  var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/invoice/*.txt",options)
//  var targetOptions =Config.readNestedKey(config, "TARGET_PATH")
//  var targetDw_Parts = uri+targetOptions("INVOICE")
//  var targetDF_Dw_Parts = Extractor.extract(Resource.AVRO,targetDw_Parts , null)
//  var targetCustomer = uri+targetOptions("CUSTOMER")
//  var targetDF_Customer = Extractor.extract(Resource.AVRO,targetCustomer , null)

  val statusOptions = Config.readNestedKey(config, "STATUS")
  var statusPath = uri + "/status/invoice/STATUS_20201103110306/*.avro"

  println(statusPath)
  var statusCols = statusOptions("COLUMNS").split(",").toSeq
  var statusDF = Extractor.extract(Resource.AVRO, statusPath, null)

  test("Verify Invoice Close Date is null") {
    var statusDFExpected = statusDF.filter(col("INV_CLOSE_DATE").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("Invoice Close Date is null.")).count()

    println(expected + " " + actual)
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  assert(expected==0)
    //assert(statusDFExpected.count != 0)
    assert(expected == actual)

  }

  test("Check invoice level values"){

    var newDf = statusDF.filter(
        col("INV_TOTAL_COST").isNull &&
        col("INV_TOTAL_PRICE").isNull &&
        col("INV_OPEN_DATE").isNull &&
        col("INV_CLOSE_DATE").isNull &&
        col("TRANS_TYPE").isNull &&
        col("INV_RET_WHOLE_FLAG").isNull
    )

    var count = newDf.filter(
        col("inference") === lit("INV_TOTAL_COST, INV_TOTAL_PRICE, INV_OPEN_DATE, INV_CLOSE_DATE, TRANS_TYPE and INV_RET_WHOLE_FLAG is blank.") &&
        col("prc") === lit("REJECTED")
    ).count()

    assert(newDf.count() == count)

  }

  test("Length of invoice payment code is greater than 256 characters") {

    var statusDFExpected = statusDF.filter(length(col("INV_PAYMENT_CODE")) > 256 && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("Length of Invoice Payment Code is greater than 256 characters.")).count()
    println("inference Actual : " + actual)
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  assert(expected==0)
    println(expected + " " + actual)
    //assert(statusDFExpected.count != 0)
    assert(expected == actual)

  }

  test("Invoice CLOSE_DATE is greater than Current Date")
  {
    var current_Date= System.currentTimeMillis()
    println("current time : "+current_Date)
    //var systemDt = statusDF.withColumn("SystemDate", lit(1000)*unix_timestamp(java.time.LocalDate.now(), "MM/dd/yyyy"))
    //statusDF.printSchema()
    var statusDFExpected=statusDF.filter(col("INV_CLOSE_DATE") > current_Date && col("prc")===lit("REJECTED"))
    println("count of statusDFExpected" +statusDFExpected.count())
    //statusDFExpected.show()
    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("Invoice close date is greater than the current date.")).count()
    println(expected+ " " +actual)
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  assert(expected==0)
    //assert(statusDFExpected.count!=0)
    assert(expected==actual)

  }

  test("Invoice close date is less than matching invoice in comparable invoices")
  {
    var statusDFExpected = statusDF.filter(col("INV_CLOSE_DATE") < col("CI_INV_CLOSE_DATE") && col("prc") === lit("REJECTED"))

    //var statusDFExpected=statusDF.filter(col("INV_CLOSE_DATE")> col("CI_INV_CLOSE_DATE")&& col("prc")===lit("REJECTED"))
    //statusDFExpected.show()
    val expected=statusDFExpected.count()

    val actual=statusDF.filter(col("inference").contains("Invoice Close Date is less than matching invoice in Comparable invoices.")).count()
    println(expected+ " " +actual)
    //var output= statusDFExpected.except(statusDF)
    //statusDFExpected=  assert(expected==0)
    assert(expected==actual)

  }

  test("Duplicate Invoice") {

    var statusDFExpected = statusDF.filter(col("DLR_CODE")===col("CI_DLR_CODE")
      && col("INV_NUMBER")===col("CI_INV_NUMBER")
      && col("INV_CLOSE_DATE")===col("CI_INV_CLOSE_DATE")
      && col("ITM_PART_NUMBER")===col("CI_ITM_PART_NUMBER")
      && col("TRANS_TYPE")===col("CI_TRANS_TYPE")
      && col("ITM_LINE_NUM")===col("CI_ITM_LINE_NUM")
      && col("prc") === lit("REJECTED"))


    val expected = statusDFExpected.count()
    val actual = statusDFExpected.filter(col("inference").contains("Duplicate Invoice.")).count()
    println(expected+ " " +actual)
    //println(statusDFExpected.select(col("inference")).first().get(0))
    assert(expected == actual)
  }

  test("DLR_CODE  is not present in dealer master file") {
    var statusDFExpected = statusDF.filter(col("DLR_NAME").isNull && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    val actual = statusDF.filter(col("inference").contains("DLR_CODE  is not present in dealer master file")).count()
    println(expected + " " + actual)
    assert(expected == actual)
  }

  test("Set INV_RET_WHOLE_FLAG to R when not W"){
    var statusDfExpected = statusDF.filter(
      col("inference").contains("INV_RET_WHOLE_FLAG set to R.")
    )
    var actual = statusDfExpected.filter(
      col("INV_RET_WHOLE_FLAG") === lit("R")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("Length of bill number greater than 32 characters") {
    var statusDFExpected = statusDF.filter((length(col("BILL_NUMBER")) > 32) && col("prc") === lit("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Length of Bill Number greater than 32 characters.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Length of bill postal code greater than 60 characters") {
    // var statusDFExpected = statusDF.filter((length(col("BILL_POSTAL_CODE")) <= 60) )
    // val expected = statusDFExpected.count()
    var newStatusDf = statusDF.filter(col("inference").contains("Length of Bill Postal Code trimmed to 60 characters."));
    var actual = newStatusDf.count()
    var expected = newStatusDf.filter((length(col("BILL_POSTAL_CODE")) <= 60) ).count()
    println(expected +" "+actual)

    assert(expected == actual)

  }

  test("Length of ship full name greater than 256 characters") {

    var StatusDfactual = statusDF.filter(col("inference").contains("Length of Ship Full Name trimmed to 256 characters."))
    var actual= StatusDfactual.count()
    var expected= StatusDfactual.filter(length(col("SHIP_FULL_NAME")) <= 256).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Length of ship city greater than 60 characters") {
    var StatusDfactual = statusDF.filter(col("inference").contains("Length of Ship City trimmed to 60 characters."))
    var actual= StatusDfactual.count()
    var expected= StatusDfactual.filter(length(col("SHIP_CITY")) <= 60).count()
    println(expected +" "+actual)
    assert(expected == actual)
  }

  test("Length of ship postal code greater than 60 characters") {
    var StatusDfactual = statusDF.filter(col("inference").contains("Length of Ship Postal Code trimmed to 60 characters."))
    var actual= StatusDfactual.count()
    var expected= StatusDfactual.filter(length(col("SHIP_POSTAL_CODE")) <= 60).count()
    println(expected +" "+actual)
    assert(expected == actual)
  }

  test("Length of item part number is greater than 40 characters") {
    var statusDFExpected = statusDF.filter((length(col("ITM_PART_NUMBER")) > 40) && ("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Length of Item Part Number is greater than 40 characters.")).count()
    assert(expected == actual)

  }

  test("Length of item part number DMS is greater than 60 characters") {
    var statusDFExpected = statusDF.filter((length(col("ITM_PART_NUMBER_DMS")) > 60) && ("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Length of Item Part Number DMS is greater than 60 characters.")).count()
    assert(expected == actual)

  }

  test("Item quantity is less than -10000") {
    var statusDFExpected = statusDF.filter((col("ITM_QTY") < -10000) && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Item Quantity is less than -10000.")).count()
    println(expected +" "+actual)
    assert(expected == actual)
  }

  test("Item quantity is greater than 10000") {
    var statusDFExpected = statusDF.filter(col("ITM_QTY") > 10000 && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Item Quantity is greater than 10000.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Verify Item quantity * item unit cost is greater than 100000") {
    var statusDFWithResult = statusDF.withColumn("Result",col("ITM_QTY")*col("ITM_UNIT_COST"))
    var expected = statusDFWithResult.filter(col("Result")> 100000 && col("prc") === lit(("REJECTED"))).count()
    var actual = statusDF.filter(col("inference").contains("Item Quantity * Item Unit Cost is greater than 100000.")).count()

    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Item unit cost is greater than 20000") {
    var statusDFExpected = statusDF.filter(col("ITM_UNIT_COST") > 20000 && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Item Unit Cost is greater than 20000.")).count()
    println(expected +" "+actual)
    assert(expected == actual)
  }

  test("Item unit price is greater than 20000") {
    var statusDFExpected = statusDF.filter(col("ITM_UNIT_PRICE") > 20000 && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Item Unit Price is greater than 20000.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Length of item EMP ID is greater than 30 characters") {
    var statusDFExpected = statusDF.filter(length(col("ITM_EMP_ID")) > 30 && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Length of Item EMP ID is greater than 30 characters.")).count()
    println(expected +" "+actual)
    assert(expected == actual)

  }

  test("Item core flag set to Y rule 1") {
    var statusDFExpected = statusDF.filter(col("ITM_PART_NUMBER").substr(11,12) === lit("U") && col("ITM_CORE_FLAG") === lit("Y"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_CORE_FLAG set to Y Rule-1.")).count()
    print(expected+" "+actual)
    assert(expected == actual)
    assert(statusDFExpected != 0)

  }

  test("ITM_CORE_FLAG set to Y Rule-2.") {
    var statusDFExpected = statusDF.filter(col("inference").contains("ITM_CORE_FLAG set to Y Rule-2.") && col("ITM_CORE_FLAG") === lit("Y"))


    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter((col("ITM_PART_DESC").contains("BATTERY")) &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_CORE_QTY") < 0)).count()

    println(expected+" "+actual)
    assert(expected == actual)
    //assert(statusDFExpected != 0)

  }

  test("Item core flag set to Y rule 3") {
    var statusDFExpected = statusDF.filter((col("ITM_PART_DESC").contains("BATTERY")) &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_UNIT_CORE_COST") =!= 0) &&
      (col("ITM_CORE_FLAG") === lit("Y")))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("TM_CORE_FLAG set to Y Rule-3.")).count()
    println(expected+" "+actual)
    assert(expected == actual)
    //assert(statusDFExpected != 0)
  }

  test("Item core flag set to Y rule 4") {
    var statusDFExpected = statusDF.filter(col("ITM_PART_DESC").contains("BATTERY")  &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_UNIT_CORE_PRICE") =!= 0) &&
      (col("ITM_CORE_FLAG") === lit("Y")))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_CORE_FLAG set to Y Rule-4.")).count()
    println(expected+" "+actual)
    assert(expected == actual)
  }

  test("ITM_CORE_FLAG set to Y Rule-5."){
    var statusDFExpected = statusDF.filter(col("ITM_PART_DESC").contains("CORE RETURN")  &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_CORE_QTY") < 0))

    var actual = statusDFExpected.filter(
      col("ITM_CORE_FLAG") === lit("Y") &&
        col("inference").contains("ITM_CORE_FLAG set to Y Rule-5.")
    ).count()

    assert(statusDFExpected.count() == actual)
  }

  test("ITM_CORE_FLAG set to Y Rule-6."){
    var statusDFExpected = statusDF.filter(col("ITM_PART_DESC").contains("CORE RETURN")  &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_UNIT_CORE_COST") =!= 0))

    var actual = statusDFExpected.filter(
      col("ITM_CORE_FLAG") === lit("Y") &&
        col("inference").contains("ITM_CORE_FLAG set to Y Rule-6.")
    ).count()

    assert(statusDFExpected.count() == actual)
  }

  test("ITM_CORE_FLAG set to Y Rule-7."){
    var statusDFExpected = statusDF.filter(col("ITM_PART_DESC").contains("CORE RETURN")  &&
      (col("ITM_QTY") < 0) &&
      (col("ITM_UNIT_CORE_PRICE") =!= 0))

    var actual = statusDFExpected.filter(
      col("ITM_CORE_FLAG") === lit("Y") &&
        col("inference").contains("ITM_CORE_FLAG set to Y Rule-7.")
    ).count()

    assert(statusDFExpected.count() == actual)
  }

  test("Item core flag set to Y Rule-8"){
    var statusDFExpected = statusDF.filter(col("ITM_PART_NUMBER").substr(11,12) === lit("X")
    && col("ITM_QTY")<0
    && col("ITM_UNIT_COST") === col("ITM_UNIT_PRICE")
    && col("ITM_UNIT_COST") =!= col("ITM_NET_COST"))

    var actual = statusDFExpected.filter(
      col("ITM_CORE_FLAG") === lit("Y") &&
        col("inference").contains("ITM_CORE_FLAG set to Y Rule-8.")
    ).count()

    assert(statusDFExpected.count() == actual)

  }

  test("Item Trans class set to RO") {
    var statusDFExpected = statusDF.filter(col("INV_REPAIR_ORDER_NUM").isNotNull
      && col("ITM_TRANS_CLASS") === lit("RO"))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to RO.")).count()
    println(expected+" "+actual)
    assert(expected == actual)
  }

  test("Item Trans class set to CR rule 1") {

    var statusDFExpected = statusDF.filter(col("INV_REPAIR_ORDER_NUM").isNull
      && (col("BILL_BUS_PERSON_FLAG") =!= 0)
      && (col("ITM_TRANS_CLASS") === lit("CR")))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to CR Rule-1.")).count()

    println(expected+" "+actual)
    assert(expected == actual)
  }

  test("Item Trans class set to CR rule 2") {

    var statusDFExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull &&
        col("BILL_BUS_PERSON_FLAG") === lit("0") &&
        col("INV_TOTAL_TAX") =!= 0 &&
        col("INV_RET_WHOLE_FLAG").isNull &&
        col("ITM_TRANS_CLASS") === lit("CR")
    )

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to CR Rule-2.")).count()
    println(expected+" "+actual)
    assert(expected == actual)

  }

  test("Item Trans class set to CR rule 3") {
    var statusDFExpected = statusDF.filter((col("INV_REPAIR_ORDER_NUM").isNull) &&
      (col("BILL_BUS_PERSON_FLAG") =!= lit("0")) &&
      (col("INV_RET_WHOLE_FLAG") === lit("W")) &&
      (col("TRANS_TYPE") === lit("REPAIRORDER")) &&
      (col("ITM_TRANS_CLASS") === lit("CR")))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to CR Rule-3.")).count()
    println(expected+" "+actual)
    assert(expected == actual)

  }

  test("Item Trans class set to CR Rule-4"){
    var statusDFExpected = statusDF.filter(
      col("inference").contains("ITM_TRANS_CLASS set to CR Rule-4.") &&
        col("ITM_TRANS_CLASS") === lit("CR")
    )


    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter((col("INV_REPAIR_ORDER_NUM").isNull) &&
      (col("BILL_BUS_PERSON_FLAG") === lit("0")) &&
      (col("INV_RET_WHOLE_FLAG") === lit("R"))
    ).count()
    println(expected+" "+actual)
    assert(expected == actual)
  }

  test("ITM_TRANS_CLASS set to WH Rule-1.") {
    var statusDFExpected = statusDF.filter((col("INV_REPAIR_ORDER_NUM").isNull) &&
      (col("BILL_BUS_PERSON_FLAG") === lit("0")) &&
      (col("TRANS_TYPE") =!= lit("REPAIRORDER")) &&
      (col("INV_RET_WHOLE_FLAG") === lit("W")) &&
      (col("ITM_TRANS_CLASS") === lit("WH")))

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to WH Rule-1.")).count()
    println(expected+" "+actual)
    assert(expected == actual)
    assert(statusDFExpected.count != 0)
  }

  test("Item Trans class set to WH rule 2") {

    var statusDFExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
        && col("BILL_BUS_PERSON_FLAG") === lit("0")
        && col("INV_TOTAL_TAX") === lit(0)
        && col("INV_RET_WHOLE_FLAG").isNull
        && col("ITM_TRANS_CLASS") === lit("WH")
    )

    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("ITM_TRANS_CLASS set to WH Rule-2.")).count()
    println(expected+" "+actual)
    assert(expected == actual)

  }

  test("Length of SIR_BILL_NUMBER is greater than 32 characters") {

    var statusDFExpeacted = statusDF.filter(length(col("BILL_NUMBER")) > 32 && col("prc") === ("REJECTED"))
    val expected = statusDFExpeacted.count()
    var actual = statusDFExpeacted.filter(col("inference").contains("BILL_NUMBER is greater than 32 characters.")).count()
    println(expected+" "+actual)
    assert(expected == actual)

  }

  test("Verify Length of SIR_BILL_POSTAL_CODE is greater than 60 characters") {

    var actualDf = statusDF.filter(col("inference").contains("BILL_POSTAL_CODE is greater than 60 characters."))
    var actual = actualDf.count()
    var expected = actualDf.filter(length(col("BILL_POSTAL_CODE"))<=60).count()
    print(actual+" "+ expected)
    assert(expected == actual)
  }

  test("CLS_CODE set to I Rule-1"){
    var statusDfExpected = statusDF.filter(
      col("ITM_QTY") > 0
      && col("ITM_UNIT_PRICE") > 0
      && col("ITM_OEM_SALE_FLAG") == lit("Y")
      && col("ITM_NET_PRICE") > 0
      && !col("INV_CLOSE_DATE").contains("")
    )

    var actual = statusDfExpected.filter(
      col("CLS_CODE") === lit("I")
      && col("inference").contains("CLS_CODE is set to I Rule-1.")
    )

    assert(statusDfExpected.count() == actual.count())

  }

  test("CLS_CODE is set to A Rule-2."){
    var time = 90*24*60*60*1000
    var statusDfExpected = statusDF.filter(
      col("ITM_QTY") > 0
        && col("ITM_UNIT_PRICE") > 0
        && col("ITM_OEM_SALE_FLAG") == lit("Y")
        && col("ITM_NET_PRICE") > 0
        && !col("INV_CLOSE_DATE").contains("")
        && col("SYSDATE") - col("INV_CLOSE_DATE") < time
        && col("SYSDATE") >= col("INV_CLOSE_DATE")
    )

    var actual = statusDfExpected.filter(
      col("CLS_CODE") === lit("A")
        && col("inference").contains("CLS_CODE is set to A Rule-2.")
    )

    assert(statusDfExpected.count() == actual.count())

  }

  test("CUST_RET_WHOLESALE_FLAG set to W Rule-1"){

    var statusDfExpected = statusDF.filter(
      col("INV_RET_WHOLE_FLAG") === lit("W")
      && col("BILL_BUS_PERSON_FLAG") === lit("0")
      && col("TRANS_TYPE") =!= lit("REPAIRORDER")
    )

    var actual = statusDfExpected.filter(
      col("CUST_RET_WHOLESALE_FLAG") === lit("W")
        && col("inference").contains("CUST_RET_WHOLESALE_FLAG is set to W Rule-1.")
    )

    assert(statusDfExpected.count() == actual.count())

  }

  test("CUST_RET_WHOLESALE_FLAG set to W Rule-2"){
    var statusDfExpected = statusDF.filter(
      col("CUST_RET_WHOLESALE_FLAG") === lit("W")
        && col("inference") === lit("CUST_RET_WHOLESALE_FLAG is set to W Rule-2.")
    )

    var actual = statusDfExpected.filter(
      col("INV_RET_WHOLE_FLAG") =!= lit("W")
        && col("INV_TOTAL_TAX") === 0.0
    )

    assert(statusDfExpected.count() == actual.count())

  }

  test("Duplicate Invoice - 2") {
    var statusDFExpected = statusDF.filter(col("IsDuplicate") === lit("Y") && col("prc") === ("REJECTED"))
    val expected = statusDFExpected.count()
    var actual = statusDFExpected.filter(col("inference").contains("Exactly same invoice.")).count()
    print(expected+" "+actual)
    assert(expected == actual)

  }

  test("Item Wholesale Flag Rule-1" ){
    var statusDFExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
      && col("BILL_BUS_PERSON_FLAG") === lit("0")
      && col("INV_RET_WHOLE_FLAG") === lit("W")
      && col("INV_TOTAL_TAX") === lit("0")
    )

    statusDFExpected.show(false)

    var actual = statusDFExpected.filter(
      col("ITM_WHOLESALE_FLAG") === lit("Y")
      && col("inference").contains("ITM_WHOLESALE_FLAG set to Y Rule-1.")
    )

    assert(statusDFExpected.count() == actual.count())

  }

  test("Item Wholesale Flag Rule-2" ){
    var statusDfExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
      && col("BILL_BUS_PERSON_FLAG").isNull
      && col("INV_RET_WHOLE_FLAG") === lit("W")
      && col("INV_TOTAL_TAX") === lit("0")
    )

    var actual = statusDfExpected.filter(
      col("ITM_WHOLESALE_FLAG") === lit("Y")
        && col("inference").contains("ITM_WHOLESALE_FLAG set to Y Rule-2.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("ITM_WHOLESALE_FLAG set to Y Rule-3." ){
    var statusDfExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
        && col("BILL_BUS_PERSON_FLAG") === lit("0")
        && col("INV_RET_WHOLE_FLAG") === lit("W")
        && col("TRANS_TYPE") =!= lit("REPAIRORDER")
    )

    var actual = statusDfExpected.filter(
      col("ITM_WHOLESALE_FLAG") === lit("Y")
        && col("inference").contains("ITM_WHOLESALE_FLAG set to Y Rule-3.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("ITM_WHOLESALE_FLAG set to Y Rule-4." ){
    var statusDfExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
        && col("BILL_BUS_PERSON_FLAG").isNull
        && col("INV_RET_WHOLE_FLAG") === lit("W")
        && col("TRANS_TYPE") =!= lit("REPAIRORDER")
    )

    var actual = statusDfExpected.filter(
      col("ITM_WHOLESALE_FLAG") === lit("Y")
        && col("inference").contains("ITM_WHOLESALE_FLAG set to Y Rule-4.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("ITM_WHOLESALE_FLAG set to Y Rule-5."){
    var statusDfExpected = statusDF.filter(
      col("INV_REPAIR_ORDER_NUM").isNull
        && col("BILL_BUS_PERSON_FLAG") === lit("0")
        && col("INV_RET_WHOLE_FLAG").isNull
        && col("INV_TOTAL_TAX") === 0
    )

    var actual = statusDfExpected.filter(
      col("ITM_WHOLESALE_FLAG") === lit("Y")
        && col("inference").contains("ITM_WHOLESALE_FLAG set to Y Rule-5.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-1"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("REPAIRORDER")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("RO")
      && col("inference").contains( "TRANS_CODE set to RO Rule-1.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-2"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("POINTOFSALE")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("PS")
        && col("inference").contains( "TRANS_CODE set to PS Rule-2.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-3"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("BACKORDER")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("BO")
        && col("inference").contains( "TRANS_CODE set to BO Rule-3.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-4"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("CREDIT")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("CR")
        && col("inference").contains( "TRANS_CODE set to CR Rule-4.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-5"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("RETURN")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("RT")
        && col("inference").contains( "TRANS_CODE set to RT Rule-5.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-6"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("REPAIRINTERNAL")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("RI")
        && col("inference").contains( "TRANS_CODE set to RI Rule-6.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

  test("TRANS_CODE Rule-7"){
    var statusDfExpected = statusDF.filter(
      upper(col("TRANS_TYPE")) === lit("INTERNALPARTS")
    )

    var actual = statusDfExpected.filter(
      col("TRANS_CODE") === lit("IP")
        && col("inference").contains( "TRANS_CODE set to IP Rule-7.")
    )

    assert(statusDfExpected.count() == actual.count())
  }

}


