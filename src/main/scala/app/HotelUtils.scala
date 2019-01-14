package app

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HotelUtils {

  def createHotelRDD(context: SparkContext, csvPath: String): RDD[Hotel] = {
    val stringRDD: RDD[String] = context
      .textFile(csvPath)
      .filter(row => !row.startsWith("date_time,site_name,posa_continent,user_location_country"))

    val hotelRDD: RDD[Hotel] = stringRDD.map(Hotel.createHotel).cache()

    hotelRDD
  }

  def get3MostPopularCoupleHotels(hotelRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    hotelRDD
      .filter(hotel => hotel.srch_adults_cnt == 2)
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(3)
  }

  def getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD: RDD[Hotel]): Int = {
    hotelRDD
      .filter(hotel => hotel.srch_destination_id == hotel.user_location_country)
      .map(hotel => (hotel.hotel_country, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .first()
      ._1
  }

  def get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    hotelRDD
      .filter(hotel => hotel.is_booking == 0 && hotel.srch_children_cnt > 0)
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .take(3)
  }

}