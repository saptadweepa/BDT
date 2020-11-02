package concepts
import scala.util.matching.Regex
object Regex {
  def main(arg:Array[String]): Unit ={

/*    val numberPattern: Regex = "[^a-z]\\G".r

    numberPattern.findFirstMatchIn("awesomepassword") match {
      case Some(_) => println("Password OK")
      case None => println("Password must contain a number")
    }*/

    val regexStr = ".{9}[U]";
    print("UUUUUUUUUUUUUUKL".matches(regexStr))
  }
}
