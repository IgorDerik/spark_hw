package app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework")
  conf.set("spark.testing.memory", "2147480000")
  val context: SparkContext = new SparkContext(conf)

  val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context,"src/test/resources/test.csv")
//  val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context, args(0))

  HotelUtils.get3MostPopularCoupleHotels(hotelRDD).foreach(arr => println(arr._1 ))
  HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD).foreach(arr => println(arr._1 ))
  HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD).foreach(arr => println(arr._1 ))

  /*
  while (true) {

  }
  */
}
