

object Records extends Config {
  def main(args: Array[String]): Unit = {
    val distFile = sc.textFile("./resources/bids.txt")
    val filtered = distFile.filter(f => f.contains("ERROR")).cache()
    val errors = filtered.map(s => (s.substring(8), 1)).countByKey()
    errors.foreach(line => println(line))
  }

}
