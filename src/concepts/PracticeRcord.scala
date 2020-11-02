package concepts
import com.nits.brms._

object PracticeRcord {
  def main(args:Array[String]): Unit ={
    var mp:Map[String,Any]=Map("name"->"gunja","age"->27,"address"->"agt","weight"->55.5)
    var rec= new Record(mp)
/*    println(rec.getStringBrms("name"))
    println(mp.get("name"))
    println(rec.getIntBrms("age"))
    println(rec.getStringBrms("address"))
    println(rec.getDoubleBrms("weight"))*/
    println(rec.fields)
    rec.appendField("newcol","someval")
    println(rec.getIntBrms("age"))
    rec.appendField("newcol","sa")
    println(rec.getStringBrms("newcol"))
    println(rec.fields.get("newcol").toString)

  }


}
