package app

object TestArr extends App {

  var arr: Array[((Int, Int, Int), Int)] = Array(((1,2,3),5))

  println(arr(0)._1)

  var arr2: Array[(Int, Int)] = Array((0,0))

  println(arr2(0))
}
