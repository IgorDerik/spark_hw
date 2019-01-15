package app

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HotelUtils {

  /**
    *
    * @param context
    * @param csvPath
    * @throws
    * @return
    */
  @throws[InvalidInputException]
  def createHotelRDD(context: SparkContext, csvPath: String): RDD[Hotel] = {
    context
      .textFile(csvPath)
      .filter(row => !row.startsWith("date_time,site_name,posa_continent,user_location_country"))
      .map(Hotel.createHotel)
      .cache()
  }

  /**
    *
    * @param hotelRDD
    * @return
    */
  def get3MostPopularCoupleHotels(hotelRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    hotelRDD
      .filter(hotel => hotel.srch_adults_cnt == 2)
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(3)
  }

  /**
    *
    * @param hotelRDD
    * @return
    */
  def getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD: RDD[Hotel]): Array[(Int, Int)] = {
    hotelRDD
      .filter(hotel => hotel.srch_destination_id == hotel.user_location_country)
      .map(hotel => (hotel.hotel_country, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(1)
  }

  /**
    *
    * @param hotelRDD
    * @return
    */
  def get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    hotelRDD
      .filter(hotel => hotel.is_booking == 0 && hotel.srch_children_cnt > 0)
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(3)
  }

}