import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Motels {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[2]"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: collection.Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val file = sc.textFile(bidsPath)
    val list = file.map(line => line.split(",").toList)
    list.filter(_.size > 2)
  }


  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    val filter = rawBids.filter(records => records(2).contains("ERROR"))
    val errRecords = filter.map(err => ("%s,%s".format(err(1), err(2)), 1)).reduceByKey((a, b) => a + b)
    errRecords.map(pairs => "%s,%d".format(pairs._1, pairs._2))
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): collection.Map[String, Double] = {
    val usdEur = 1.087
    val file = sc.textFile(exchangeRatesPath).map(lines => lines.split(","))
    val pairs = file.map(m => (m(0), m(3).toDouble * usdEur)).collectAsMap()
    pairs
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: collection.Map[String, Double]): RDD[BidItem] = {
    val filter = rawBids.filter(records => !records(2).contains("ERROR"))
    val bindBids = filter.flatMap(bid => List((bid.head, bid(1), "US", bid(5)), (bid.head, bid(1), "CA", bid(8)), (bid.head, bid(1), "MX", bid(6))))
    val reduceBids = bindBids.map(b => (b._1, b._2, b._3, b._4.toDouble * exchangeRates.getOrElse(b._2, Double.NaN)))
    val f = bindBids.map(line => BidItem(line._1, line._2, line._3, convertString(line._4)))
    f
  }

  def convertString(value: String): Double = {
    try {
      value.toDouble
    } catch {
      case _ : NumberFormatException => Double.NaN
    }
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    val path = sc.textFile(motelsPath)
    val lines = path.map(r => r.split(","))
    val field = lines.map(f => (f(0), f(1)))
    field
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val records = bids.map(b => (b.motelId, b)).join(motels.map(m => (m._1, m))).map(r => EnrichedItem(r._1, r._2._2._2, r._2._1.bidDate, r._2._1.loSa, r._2._1.price))
    val filtered = records.map(m => (m.motelId, m.bidDate) -> m)
    val reduce = filtered.reduceByKey((k, v) => if (k.price > v.price) k else v)
    val getLast = reduce.map(number => number._2)
    getLast

  }
}
