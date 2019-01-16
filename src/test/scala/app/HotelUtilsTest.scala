package app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Assert, Before, Test}

class HotelUtilsTest {

  private var conf: SparkConf = _
  private var context: SparkContext = _
  private var csvPath: String = _
  private var hotelRDD: RDD[Hotel] = _
  private var testHotelsList: List[String] = _
  private var fromListHotelRDD: RDD[Hotel] = _

  @Before
  def initializeData() {
    conf = new SparkConf().setMaster("local").setAppName("Spark Homework Test")
    context = new SparkContext(conf)
    csvPath = "src/test/resources/test.csv"
    hotelRDD = HotelUtils.createHotelRDD(context, csvPath)
    testHotelsList = List(
      "11/22/2014 20:53,30,4,195,991,47725,,1048,1,0,9,6/26/2015,6/28/2015,2,0,1,8803,1,0,1,3,151,1236,82,,,,,,,,,,,,,,",
      "1/8/2014 13:57,2,3,66,462,41898,2454.8588,1482,0,1,1,2/21/2014,2/23/2014,3,0,2,12009,125,,1048,1,0,9,6/26/2015,6/28/2015,2,0,1,8803,1,0,1,3,151,69,59,,",
      "1/3/2014 16:44,2,3,66,462,41898,2454.8588,1482,0,1,1,2/22/2014,2/27/2014,2,0,1,12009,1,1,1,2,50,368,95,,,,,,,,,,,,,,",
      "12/2/2013 3:06,24,2,3,64,12576,,2451,0,1,4,2/26/2014,2/28/2014,2,3,1,66,1,0,1,6,105,35,25,,,,,,,,,,,,,,",
      "8/17/2014 15:08,24,2,3,64,4777,,2451,0,0,2,8/27/2014,8/28/2014,2,3,1,23,1,0,1,6,105,35,82,,,,,,,,,,,,,,",
      "12/26/2014 19:05,24,2,3,64,9448,,2451,0,0,4,2/5/2015,2/6/2015,2,3,1,3,6,0,5,6,105,35,8,,,,,,,,,,,,,,",
      "12/30/2014 12:51,24,2,3,64,9448,,2451,0,0,4,2/9/2015,2/10/2015,2,0,1,3,6,1,1,6,105,35,15,,,,,,,,,,,,,,",
      "6/25/2013 0:09,25,2,23,79,26143,,3972,0,0,1,8/11/2013,8/12/2013,2,1,1,23,1,0,2,3,151,1236,30,,,,,,,,,,,,,,",
      "7/11/2014 0:52,25,2,23,48,4924,,3972,1,0,9,8/14/2014,8/15/2014,2,1,1,8278,1,0,1,2,50,368,91,,,,,,,,,,,,,,",
      "7/12/2014 19:28,25,2,23,48,4924,,3972,1,0,9,8/14/2014,8/15/2014,2,1,1,8278,1,0,1,2,50,368,0,9,8/13/2014,8/14/2014,3,1,1,12265,6,0,1,2,50,368,5")
    fromListHotelRDD = context.parallelize(testHotelsList).map(Hotel.createHotel)
  }

  @After
  def destroyContext() {
    context.stop()
  }

  @Test
  def createHotelRDD() {
    Assert.assertTrue(hotelRDD.count() == 882)
    val firstHotel = new Hotel(66,2,0,8250,0,2,50,628,1)
    Assert.assertEquals(hotelRDD.first(), firstHotel)

    Assert.assertTrue(fromListHotelRDD.count() == 10)
    val firstHotelFromList = new Hotel(195,2,0,8803,0,3,151,1236,82)
    Assert.assertEquals(fromListHotelRDD.first(), firstHotelFromList)
  }

  @Test
  def get3TopHotelsPeopleWithChildrenInterestedButNotBooked() {
    val expectedArray = Array(((2,50,368),119), ((2,50,365),66), ((2,50,366),46))
    val actualArray = HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)

    val secondExpectedArray = Array(((6,105,35),3), ((2,50,368),2), ((3,151,1236),1))
    val secondActualArray = HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(fromListHotelRDD)
    Assert.assertTrue(secondExpectedArray sameElements secondActualArray)
  }

  @Test
  def get3MostPopularCoupleHotels() {
    val expectedArray = Array(((2,50,368),110), ((6,105,29),99), ((6,105,35),82))
    val actualArray = HotelUtils.get3MostPopularCoupleHotels(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)

    val secondExpectedArray = Array(((6,105,35),4), ((2,50,368),3), ((3,151,1236),2))
    val secondActualArray = HotelUtils.get3MostPopularCoupleHotels(fromListHotelRDD)
    Assert.assertTrue(secondExpectedArray sameElements secondActualArray)
  }

  @Test
  def get1MostPopularCountryHotelsBookedAndSearchedFromSameCountry() {
    val expectedArray = Array((31,2))
    val actualArray = HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD)
    Assert.assertTrue(expectedArray sameElements actualArray)

    val secondExpectedArray = Array((105,2))
    val secondActualArray = HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(fromListHotelRDD)
    Assert.assertTrue(secondExpectedArray sameElements secondActualArray)
  }

}