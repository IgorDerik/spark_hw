package app

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HotelUtils {

  /**
    * Method to create dataset of Hotels from the csv file
    * @param context Spark Context
    * @param csvPath Path to the csv file
    * @throws InvalidInputException if path to the csv file is not correct
    * @return Resilient Distributed Dataset (RDD) of Hotels
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
    * Method to find top 3 most popular hotels between couples
    * @param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
    * @return Array[((continent, country, market), number)]
    *         Array of hotels and their number in the dataset.
    *         Hotels are treated as composite keys of continent, country and market.
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
    * Method to find the most popular country where hotels are booked and searched from the same country
    * @param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
    * @return Array[(country, number)]
    *         Array of countries and their number in the dataset.
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
    * Method to find top 3 hotels where people with children are interested but not booked in the end
    * @param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
    * @return Array[((continent, country, market), number)]
    *         Array of hotels and their number in the dataset.
    *         Hotels are treated as composite keys of continent, country and market.
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