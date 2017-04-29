import java.io
import java.io.File

import com.holdenkarau.spark.testing.JavaRDDComparisons
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}
import org.junit._
import org.junit.rules.TemporaryFolder

class MotelsTest extends FunSuite with BeforeAndAfter {

  val _outputFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _outputFolder

  val INPUT_BIDS_SAMPLE = "./resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "./resources/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "./resources/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "./resources/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "./resources/output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "./resources/output/error_records"

  private var outputFolder: File = null
  private var sc: SparkContext = null

  override def before() {
    outputFolder = temporaryFolder.newFolder("output")
    sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("motels-home-recommendation test"))

  }

  test("testGetRawBids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )
    val actual = Motels.getRawBids(sc, INPUT_BIDS_SAMPLE)
    JavaRDDComparisons.assertRDDEquals(expected, actual)
  }


  test("testGetErroneousRecords") {
    val bids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val actual = Motels.getErroneousRecords(bids)
    JavaRDDComparisons.assertRDDEquals(expected, actual)
  }

}
