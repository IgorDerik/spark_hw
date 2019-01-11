package app

object Split {

  def splitLine(line: String): Array[String] = {
    line.split(",")
  }

  def main(args: Array[String]): Unit = {

    val s: String = "2014-01-17 06:24:56,2,3,66,318,22418,420.6642,756,0,1,9,2014-04-17,2014-04-20,2,0,1,8291,1,0,5,2,50,191,18"

//    val arr: Array[String] = splitLine(s)
//    arr.foreach(println)
//    println( arr(0) )

    var h: Hotel = Hotel.createHotel(s)

//    print( h.date_time )
//    println(0.0)

  }

}
