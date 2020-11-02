package concepts

object MapPractice {
  def main(args:Array[String]): Unit ={
    var mp: Map[String,Any]= Map("name"->"Gunja", "age"->27, "address"->"agartala","weight"->55) //Immutable map
    println(mp.get("name"))
    println(mp.get("age"))
    println(mp.get("address"))
    println(mp.get("weight"))
    println(mp.keys)
    mp= Map("name"->"Gunja")
    var map2= Map("age"->27, "address"->"agartala")
    println(mp)
    println(map2)
  }

}
