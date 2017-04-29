import java.text.DecimalFormat

object Exchange extends Config {
  def main(args: Array[String]): Unit = {
    val formatter = new DecimalFormat("#.###")
    val usdEur = 1.087
    val file = sc.textFile("./resources/exchange_rate.txt")
//    val date =
//    val pair = file.map(m => (m.split(",")(0), m.split(",")(3).toDouble * usdEur)).foreach(line => println(line))
  }

}
