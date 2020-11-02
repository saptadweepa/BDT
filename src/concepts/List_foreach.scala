package concepts

object List_foreach {
  def main(args : Array[String]): Unit ={
    var list= List(1,2,3,4,5)
    list.foreach(print)
    println
    list.foreach((element:Int)=>print(element+" "))      // Explicitly mentioning type of elements
  }

}
