

object Exchange extends Config {
  def main(args: Array[String]): Unit = {
    val usdEur = 1.087
    val file = sc.textFile("./resources/exchange_rate.txt")
    val pair = file.map(m => (m.split(",")(0), m.split(",")(3).toDouble * usdEur)).foreach(line => println(line))
  }

}
