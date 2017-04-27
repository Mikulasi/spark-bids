import org.apache.spark.{SparkConf, SparkContext}

trait Config {
  val conf = new SparkConf().setAppName("Bids").setMaster("local[4]")
  val sc = new SparkContext(conf)
}
