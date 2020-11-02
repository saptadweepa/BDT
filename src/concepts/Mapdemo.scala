package concepts

object Mapdemo {
  def main(args:Array[String]){
    //Define a map with 3 keys and corresponding values
    var ex:Map[String,Any] = Map("Name"->"Kaushal", "Age"->27, "Address"->"ignou")
    println(ex.get("Name"))
  }
}
