package app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework")
  val context: SparkContext = new SparkContext(conf)

  val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context,"src/main/resources/train.csv")

  HotelUtils.get3MostPopularCoupleHotels(hotelRDD).foreach(println)
  println(HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD))
  HotelUtils.get3TopHotelsPeopleWithChildrenInterestedButNotBooked(hotelRDD).foreach(println)
/*
  while (true) {

  }
*/
}
