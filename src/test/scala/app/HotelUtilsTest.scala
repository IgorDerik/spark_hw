package app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Assert, Before, Test}

class HotelUtilsTest {
/*
  private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework Testing")
  private val context: SparkContext = new SparkContext(conf)
  private val csvPath: String = "src/test/resources/test.csv"
  private val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context, csvPath)
*/

  private var conf: SparkConf = _
  private var context: SparkContext = _
  private var csvPath: String = _
  private var hotelRDD: RDD[Hotel] = _

  @Before
  def initialize() {
    conf = new SparkConf().setMaster("local").setAppName("Spark Homework Testing")
    context = new SparkContext(conf)
    csvPath = "src/test/resources/test.csv"
    hotelRDD = HotelUtils.createHotelRDD(context, csvPath)
  }

  @After
  def destroy() {
    context.stop()
  }

  @Test
  def createHotelRDD() {
    Assert.assertTrue(hotelRDD.count() == 882)
    val firstHotel = new Hotel(66,2,0,8250,0,2,50,628,1)
    Assert.assertEquals(hotelRDD.first(), firstHotel)
  }

  @Test
  def get3TopHotelsPeopleWithChildrenInterestedButNotBooked() {
    val expectedArray = Array(((2,50,368),119), ((2,50,365),66), ((2,50,366),46))
    val actualArray = HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)
  }

  @Test
  def get3MostPopularCoupleHotels() {
    val expectedArray = Array(((2,50,368),110), ((6,105,29),99), ((6,105,35),82))
    val actualArray = HotelUtils.get3MostPopularCoupleHotels(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)
  }

  @Test
  def get1MostPopularCountryHotelsBookedAndSearchedFromSameCountry() {
    val expectedArray = Array((31,2))
    val actualArray = HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)
  }

}