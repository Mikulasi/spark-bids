import org.apache.spark.{SparkConf, SparkContext}

object Records {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Err numbers").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val distFile = sc.textFile("./resources/bids.txt")
    val filtered = distFile.filter(f => f.contains("ERROR")).cache()
    val errors = filtered.map(s => (s.substring(8), 1)).countByKey()
    errors.foreach(line => println(line))
  }

}
