package app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test extends App {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework Testing")
  val context: SparkContext = new SparkContext(conf)
  val csvPath = "src/test/resources/test.csv"

  val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context,csvPath)

  HotelUtils.getMostPopularCountryHotelsBookedAndSearchedFromSameCountry(hotelRDD).foreach(println)

//  var arr: Array[((Int, Int, Int), Int)] = Array(((1,2,3),5))
//  println(arr(0)._1)
//  var arr2: Array[(Int, Int)] = Array((0,0))
//  println(arr2(0))
}
