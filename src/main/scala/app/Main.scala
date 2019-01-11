package app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Homework Task One")
    val context: SparkContext = new SparkContext(conf)
    val headerStringRDD: RDD[String] = context.textFile("src/main/resources/t.csv")
    val header: String = headerStringRDD.first()
    val stringRDD: RDD[String] = headerStringRDD.filter(line => line != header)
    val hotelRDD: RDD[Hotel] = stringRDD.map(Hotel.createHotel)

    println( hotelRDD.count() )

    //val testRDD: RDD[Hotel] = hotelRDD.filter( hotel => hotel.getIs_mobile<1 )
    //println( testRDD.count() )
  }

}
