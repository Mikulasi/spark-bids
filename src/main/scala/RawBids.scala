
object RawBids extends Config{
  def main(args: Array[String]): Unit = {
    val file = sc.textFile("./resources/bids.txt")
    val lines = file.map(line => line.split(",").toList)
  }
}
