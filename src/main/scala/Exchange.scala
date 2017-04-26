import org.apache.spark.{SparkConf, SparkContext}

object Exchange {
  def main(args: Array[String]): Unit = {
    var usdEur = 1.087
    val conf = new SparkConf().setAppName("Exchange rates").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("./resources/exchange_rate.txt")
    val pair = file.map(m => (m.split(",")(0), m.split(",")(3).toDouble * usdEur)).foreach(line => println(line))
  }

}
