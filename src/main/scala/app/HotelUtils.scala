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

}