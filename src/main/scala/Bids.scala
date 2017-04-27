
object Bids extends Config{
  def main(args: Array[String]): Unit = {
    val file = sc.textFile("./resources/bids.txt")
    val filter = file.filter(f => !f.contains("ERROR"))
    val pairs = filter.map(m => (m.split(",")(0), m.split(",")(1), m.split(",")(5), m.split(",")(6), m.split(",")(8))).foreach(line => println(line))
  }

}
