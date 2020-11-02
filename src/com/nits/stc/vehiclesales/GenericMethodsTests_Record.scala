package com.nits.stc.vehiclesales



import com.nits.brms._
import org.scalatest.FunSuite

class GenericMethodsTests_Record extends FunSuite{

  val colName = "doubleValue"

  test("should return 0 as double value when converting empty string into double"){

    var A:Map[String,Any] = Map(colName ->"")
    var record = new Record(A)
    assert(record.getDoubleBrms(colName)===0.0)
  }

  test("should return 0 as double value when converting null string into double"){
    var A:Map[String,Any] = Map(colName -> null)
    var record = new Record(A)
    assert(record.getDoubleBrms(colName)===0.0)
  }

  test("should return double value when retrieving double value stored in a string"){
    var A:Map[String,Any] = Map(colName -> "22.43")
    var record = new Record(A)
    assert(record.getDoubleBrms(colName)===22.43)
  }

  test("should return double value 0 when retrieving integer value stored in a integer is empty"){
    var A:Map[String,Any] = Map(colName -> 0)
    var record = new Record(A)
    assert(record.getDoubleBrms(colName)=== 0.0)
  }

  test("should return double value when retrieving integer value stored in a integer"){
    var A:Map[String,Any] = Map(colName -> 10)
    var record = new Record(A)
    assert(record.getDoubleBrms(colName) === 10.0)
  }

  test("should return 0 as integer value for when converting empty string into integer"){
    var A:Map[String,Any] = Map(colName -> "")
    var record = new Record(A)
    assert(record.getIntBrms(colName)===0)
  }


  test("should return 0 as integer value when converting null string into integer"){
    var A:Map[String,Any] = Map(colName -> null)
    var record = new Record(A)
    assert(record.getIntBrms(colName)===0)
  }

  test("should return integer value when retrieving integer value stored in a string"){
    var A:Map[String,Any] = Map(colName -> "45")
    var record = new Record(A)
    assert(record.getIntBrms(colName)===45)
  }

  test("should return integer value 0 when retrieving integer value stored in a integer is empty"){
    var A:Map[String,Any] = Map(colName -> 0)
    var record = new Record(A)
    assert(record.getIntBrms(colName)===0)
  }

  test("should return integer value when retrieving integer value stored in a integer"){
    var A:Map[String,Any] = Map(colName -> 10)
    var record = new Record(A)
    assert(record.getIntBrms(colName)===10)
  }

  test("should return long when long value stored in column"){
    var A:Map[String,Any] = Map(colName -> 12L)
    var record = new Record(A)
    assert(record.getLongBrms(colName)===12L)
  }

  test("should return long when long value stored as numeric string in column"){
    var A:Map[String,Any] = Map(colName -> "10")
    var record = new Record(A)
    assert(record.getLongBrms(colName)===10L)
  }

  test("should throw java.lang.NumberFormatException error getting a long value from a non-numeric string column"){
    var A:Map[String,Any] = Map(colName -> "asdfa")
    var record = new Record(A)

    try{
      record.getLongBrms(colName)
    }catch {
      case e: NumberFormatException =>
        assert(true)
    }

  }

  test("should return 0 when empty string stored in column"){
    var A:Map[String,Any] = Map(colName -> "")
    var record = new Record(A)
    assert(record.getLongBrms(colName)===0L)
  }

  test("should return 0 when null string stored in column"){
    var A:Map[String,Any] = Map(colName -> null)
    var record = new Record(A)
    assert(record.getLongBrms(colName)===0L)
  }


  test("should return long when integer value stored in column"){
    var A:Map[String,Any] = Map(colName -> 10)
    var record = new Record(A)
    assert(record.getLongBrms(colName)===10L)
  }

  test("should return string when string value stored"){
    var A:Map[String,Any] = Map(colName -> "name")
    var record = new Record(A)
    assert(record.getStringBrms(colName)==="name")
  }
  test("should return string equivalent of long when string value retrieved"){
    var A:Map[String,Any] = Map(colName -> 123456789L)
    var record = new Record(A)
    assert(record.getStringBrms(colName)==="123456789")
  }
  test("should return string equivalent of integer when string value retreived"){
    var A:Map[String,Any] = Map(colName -> 123)
    var record = new Record(A)
    assert(record.getStringBrms(colName)==="123")
  }

  test("should set the value of the map column as per value passed"){

    var A:Map[String,Any] = Map(colName -> 123)
    var record = new Record(A)
    record.appendField(colName,"somevalue")
    assert(record.fields(colName)==="somevalue")

  }

    test("should append the value provided to the existing value of map column"){

    var A:Map[String,Any] = Map(colName -> 123)
    var record = new Record(A)
    val newCol = "newCol"
    assert(record.getIntBrms(colName)===123)
    record.appendField(newCol, "somevalue")
    assert(record.fields(newCol)==="somevalue")

  }


}
