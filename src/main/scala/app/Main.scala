package app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework")
    val context: SparkContext = new SparkContext(conf)

    val hotelRDD: RDD[Hotel] = HotelUtils.createHotelRDD(context,"src/main/resources/t.csv")

    hotelRDD.collect().foreach(println)
    println(hotelRDD.count())

  }

}
