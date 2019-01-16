package app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework")
  conf.set("spark.testing.memory", "2147480000")
  val context: SparkContext = new SparkContext(conf)

  val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context, args(0))

  println("Top 3 most popular hotels between couples:")
  HotelUtils.get3MostPopularCoupleHotels(hotelRDD).foreach(arr => println(arr._1 ))

  println("The most popular country where hotels are booked and searched from the same country:")
  HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD).foreach(arr => println(arr._1 ))

  println("Top 3 hotels where people with children are interested but not booked in the end:")
  HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD).foreach(arr => println(arr._1 ))

  context.stop()
}